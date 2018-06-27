package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
)

// Vars
var wait = sync.WaitGroup{}
var from = flag.Int64("from", 0, "start of window.")
var to = flag.Int64("to", 0, "end of window.")
var window = flag.Int("window", 0, "window size.")
var getrollup = make(chan []Rollup, 64)
var gettslist = make(chan []string, 64)
var getmetrics = make(chan string, 64)
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

		req, err := http.NewRequest("GET", uri, nil)
		if err != nil {
			log.Fatal(err)
		}

		q := req.URL.Query()
		q.Add("type", "metrics")
		q.Add("max", config.API.SuggestMax)
		q.Add("q", metric)
		req.URL.RawQuery = q.Encode()

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			log.Fatal(err)
		}
		defer resp.Body.Close()
		result, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Fatal(err.Error())
		}

		var ml []string
		json.Unmarshal(result, &ml)

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
		req, err := http.NewRequest("GET", uri, nil)
		if err != nil {
			log.Fatal(err)
		}

		q := req.URL.Query()
		q.Add("useMeta", config.API.LookupUseMeta)
		q.Add("limit", config.API.LookupLimit)
		q.Add("m", metric)
		req.URL.RawQuery = q.Encode()

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			log.Fatal(err)
		}
		defer resp.Body.Close()
		var tslist *Lookup
		json.NewDecoder(resp.Body).Decode(&tslist)

		//fmt.Printf("tslist: %v \n", tslist)
		if tslist == nil {
			log.Fatal("fail")
		}

		for i := 0; i < len(tslist.Results); i += config.API.Batch {
			var k []string
			j := i + config.API.Batch
			if j > len(tslist.Results) {
				j = len(tslist.Results)
			}
			//fmt.Printf("tslist.Results[%v:%v]\n", i, j)
			for _, t := range tslist.Results[i:j] {
				//fmt.Printf("t %v\n", string(t.Tsuid))
				k = append(k, string(t.Tsuid))
			}
			//ts = append(ts, k)
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
		b := new(bytes.Buffer)
		json.NewEncoder(b).Encode(jsondata)

		uri := rr.Get() + "/api/query"
		resp, _ := http.Post(uri, "application/json; charset=utf-8", b)
		//io.Copy(os.Stdout, resp.Body)

		var result *QueryRespItem
		json.NewDecoder(resp.Body).Decode(&result)
		defer resp.Body.Close()
		if *result != nil {
			//fmt.Printf("getrollup result: %v\n", result)
			converted := convertRollup(result)
			getrollup <- converted
		} else {
			log.Println("GetRollup no results")
		}
	}

}

// convertRollup takes aggregated rollup data and formated it for posting to rollup api
func convertRollup(in *QueryRespItem) []Rollup {
	var test Rollup
	var result []Rollup
	a := len(*in) / 2
	for i, item := range *in {
		//fmt.Printf("convertrollup item: %v\n", item)
		agg := "SUM"
		if i < a {
			agg = "COUNT"
		}
		for dps, value := range item.Dps {
			test.Aggregator = agg
			test.Interval = "1h"
			test.Metric = item.Metric
			test.Tags = item.Tags
			test.Timestamp = dps
			test.Value = value
			result = append(result, test)
		}
	}
	//fmt.Printf("convertrollup: %v\n", result)
	return result
}

// PostRollup send rollup datapoints for time series
func PostRollup(wr *RoundRobin, wait *sync.WaitGroup) {
	defer wait.Done()
	for data := range getrollup {
		//fmt.Printf("post data: %v\n", data)

		b := new(bytes.Buffer)
		json.NewEncoder(b).Encode(data)

		uri := wr.Get() + "/api/rollup?summary"
		resp, _ := http.Post(uri, "application/json; charset=utf-8", b)

		var result *RollupResponse
		json.NewDecoder(resp.Body).Decode(&result)
		defer resp.Body.Close()
		if result.Success != 0 || result.Failed != 0 {
			//fmt.Printf("postrollup result: %v\n", result)
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
	sTime := time.Now()

	flag.Parse()
	// Config

	if _, err := toml.DecodeFile("./opentsdbrollup.toml", &config); err != nil {
		fmt.Println(err)
	}

	done := make(chan bool)
	defer close(done)

	rr := NewRoundRobin(config.Servers.ReadEndpoint)
	wr := NewRoundRobin(config.Servers.WriteEndpoint)

	// set time range
	originalTime := time.Now().UTC()
	if *to > 0 {
		originalTime = time.Unix(*to, 0).UTC()
	}
	hourspast := config.API.HoursPast
	if *window > 0 {
		hourspast = *window * -1
		fmt.Printf("Rollup Window Size override %d hours\n", hourspast)
	}
	//originalTime := time. time.Duration(time.Duration(end) * time.Hour)
	endTime := time.Date(originalTime.Year(), originalTime.Month(), originalTime.Day(), originalTime.Hour(), 0, 0, 0, originalTime.Location())
	startTime := endTime.Add(time.Duration(time.Duration(hourspast) * time.Hour))
	fmt.Printf("Rollup Window - StartTime: %s EndTime: %s \n", startTime.Format(time.UnixDate), endTime.Format(time.UnixDate))

	// build metric list
	wait.Add(4)
	go GetMetrics(rr, &wait)
	go GetTSListWorkerPool(config.API.ReadWorkers, rr, &wait)
	go GetRollupWorkerPool(config.API.ReadWorkers, endTime.Unix(), startTime.Unix(), rr, &wait)
	go PostRollupWorkerPool(config.API.WriteWorkers, wr, &wait)
	wait.Wait()

	eTime := time.Now()
	diff := eTime.Sub(sTime)
	fmt.Println("total time taken ", diff.Seconds(), "seconds")
	os.Exit(0)
}
