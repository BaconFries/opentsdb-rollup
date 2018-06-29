package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
	resty "gopkg.in/resty.v1"
)

// Vars
var wait = sync.WaitGroup{}
var from = flag.Int64("from", 0, "start of window.")
var to = flag.Int64("to", 0, "end of window.")
var window = flag.Int("window", 0, "window size.")
var getrollup = make(chan []Rollup, 128)
var gettslist = make(chan []string, 128)
var getmetrics = make(chan string, 128)
var config tomlConfig

// NewRoundRobin server list
func NewRoundRobin(pool []string) *RoundRobin {
	return &RoundRobin{
		current: 0,
		pool:    pool,
	}
}

// Get next server in list
func (r *RoundRobin) Get() string {
	r.Lock()
	defer r.Unlock()
	if r.current >= len(r.pool) {
		r.current = r.current % len(r.pool)
	}
	result := r.pool[r.current]
	r.current++
	return result
}

// GetMetrics search metric names from suggest api call
func GetMetrics(rr *RoundRobin, wait *sync.WaitGroup) {
	defer wait.Done()
	for _, metric := range config.Metric.List {

		uri := rr.Get() + "/api/suggest"
		resp, err := resty.R().
			SetQueryParams(map[string]string{
				"type": "metrics",
				"max":  config.API.SuggestMax,
				"q":    metric,
			}).
			Get(uri)
		if err != nil {
			panic(err)
		}
		var ml []string
		json.Unmarshal(resp.Body(), &ml)

		for _, m := range ml {
			fmt.Printf("GetMetrics: %s\n", m)
			getmetrics <- m
		}
	}
	close(getmetrics)
}

// GetTSList gets timeseries list
func GetTSList(rr *RoundRobin, wait *sync.WaitGroup) {
	defer wait.Done()
	for metric := range getmetrics {

		uri := rr.Get() + "/api/search/lookup"
		resp, err := resty.R().
			SetResult(&Lookup{}).
			SetQueryParams(map[string]string{
				"useMeta": config.API.LookupUseMeta,
				"limit":   config.API.LookupLimit,
				"m":       metric,
			}).
			Get(uri)
		if err != nil {
			panic(err)
		}
		tslist := resp.Result().(*Lookup)
		if tslist == nil {
			log.Fatal("fail")
		}
		for i := 0; i < len(tslist.Results); i += config.API.Batch {
			var k []string
			j := i + config.API.Batch
			if j > len(tslist.Results) {
				j = len(tslist.Results)
			}
			for _, t := range tslist.Results[i:j] {
				k = append(k, string(t.Tsuid))
			}
			gettslist <- k
		}
	}
}

// GetRollup get rollup datapoints for time series
func GetRollup(endTime int64, startTime int64, rr *RoundRobin, wait *sync.WaitGroup) {
	defer wait.Done()
	for tsuid := range gettslist {
		//fmt.Println("capacity is", cap(gettslist))
		//fmt.Println("length is", len(gettslist))
		//fmt.Printf("getrollup tsuid: %v\n", tsuid)

		var jsondata Query
		var queries SubQuery
		jsondata.Start = startTime
		jsondata.End = endTime
		queries.Aggregator = "none"
		queries.Downsample = "1h-count"
		for _, chunk := range tsuid {
			queries.Tsuids = append(queries.Tsuids, chunk)
		}
		jsondata.Queries = append(jsondata.Queries, queries)
		queries.Downsample = "1h-sum"
		jsondata.Queries = append(jsondata.Queries, queries)

		uri := rr.Get() + "/api/query"
		resp, err := resty.R().
			SetResult(&QueryRespItem{}).
			SetBody(jsondata).
			Post(uri)
		if err != nil {
			panic(err)

		}
		result := resp.Result().(*QueryRespItem)
		if *result != nil {
			converted := convertRollup(result)
			getrollup <- converted
		} else {
			log.Println("GetRollup no results")
		}
	}
}

// convertRollup takes aggregated rollup data and formated it for posting to rollup api
func convertRollup(in *QueryRespItem) []Rollup {
	var temp Rollup
	var result []Rollup
	a := len(*in) / 2
	for i, item := range *in {
		agg := "SUM"
		if i < a {
			agg = "COUNT"
		}
		for dps, value := range item.Dps {
			temp.Aggregator = agg
			temp.Interval = "1h"
			temp.Metric = item.Metric
			temp.Tags = item.Tags
			temp.Timestamp = dps
			temp.Value = value
			result = append(result, temp)
		}
	}
	return result
}

// PostRollup send rollup datapoints for time series
func PostRollup(wr *RoundRobin, wait *sync.WaitGroup) {
	defer wait.Done()
	for data := range getrollup {
		uri := wr.Get() + "/api/rollup?summary"
		resp, err := resty.R().
			SetResult(&RollupResponse{}).
			SetBody(data).
			Post(uri)
		if err != nil {
			fmt.Printf("rest error: %v\n", err)
		}
		result := resp.Result().(*RollupResponse)
		if result.Success != 0 || result.Failed != 0 {
			fmt.Printf("Response Summary: Success %d Failed %d\n", result.Success, result.Failed)
		} else {
			log.Println("PostRollup no results")
		}
	}
}

//GetTSListWorkerPool spawns workers
func GetTSListWorkerPool(noOfWorkers int, rr *RoundRobin, wait *sync.WaitGroup) {
	var wg sync.WaitGroup
	for i := 0; i < noOfWorkers; i++ {
		wg.Add(1)
		go GetTSList(rr, &wg)
	}
	wg.Wait()
	close(gettslist)
	wait.Done()
}

//GetRollupWorkerPool spawns workers
func GetRollupWorkerPool(noOfWorkers int, endTime int64, startTime int64, rr *RoundRobin, wait *sync.WaitGroup) {

	var wg sync.WaitGroup
	for i := 0; i < noOfWorkers; i++ {
		wg.Add(1)
		go GetRollup(endTime, startTime, rr, &wg)
	}
	wg.Wait()
	close(getrollup)
	wait.Done()
}

//PostRollupWorkerPool spawns workers
func PostRollupWorkerPool(noOfWorkers int, wr *RoundRobin, wait *sync.WaitGroup) {
	var wg sync.WaitGroup
	for i := 0; i < noOfWorkers; i++ {
		wg.Add(1)
		PostRollup(wr, &wg)
	}
	wg.Wait()
	wait.Done()
}

func main() {
	// Start Timer
	sTime := time.Now()

	// Prase CLI flags
	flag.Parse()

	// Config
	if _, err := toml.DecodeFile("./opentsdbrollup.toml", &config); err != nil {
		fmt.Println(err)
	}

	resty.SetDebug(false)
	resty.SetRetryCount(3)
	resty.SetHeader("Accept", "application/json")

	// API Servers
	rr := NewRoundRobin(config.Servers.ReadEndpoint)
	wr := NewRoundRobin(config.Servers.WriteEndpoint)

	// Set Time Window
	originalTime := time.Now().UTC()
	if *to > 0 {
		originalTime = time.Unix(*to, 0).UTC()
	}
	hourspast := config.API.HoursPast
	if *window > 0 {
		hourspast = *window * -1
		fmt.Printf("Rollup Window Size override %d hours\n", hourspast)
	}
	// originalTime := time. time.Duration(time.Duration(end) * time.Hour)
	endTime := time.Date(originalTime.Year(), originalTime.Month(), originalTime.Day(), originalTime.Hour(), 0, 0, 0, originalTime.Location())
	startTime := endTime.Add(time.Duration(time.Duration(hourspast) * time.Hour))
	fmt.Printf("Rollup Window - StartTime: %s EndTime: %s \n", startTime.Format(time.UnixDate), endTime.Format(time.UnixDate))

	// Start pipeline
	wait.Add(4)
	go GetMetrics(rr, &wait)
	go GetTSListWorkerPool(config.API.ReadWorkers, rr, &wait)
	go GetRollupWorkerPool(config.API.ReadWorkers, endTime.Unix(), startTime.Unix(), rr, &wait)
	go PostRollupWorkerPool(config.API.WriteWorkers, wr, &wait)
	wait.Wait()

	// End Timer
	eTime := time.Now()
	diff := eTime.Sub(sTime)
	fmt.Println("total time taken ", diff.Seconds(), "seconds")
	os.Exit(0)
}
