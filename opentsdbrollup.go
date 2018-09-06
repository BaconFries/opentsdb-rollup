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
	"github.com/araddon/dateparse"
	resty "gopkg.in/resty.v1"
)

// Vars
var wait = sync.WaitGroup{}
var from = flag.String("from", "", "start of window.")
var to = flag.String("to", "", "end of window.")
var window = flag.Int("window", 0, "window size.")
var getrollup = make(chan Getrollup, 400)
var gettslist = make(chan Gettslist, 300)
var getmetrics = make(chan string, 300)
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
		time.Sleep(25 * time.Millisecond)
		uri := rr.Get() + "/api/suggest"
		resp, err := resty.R().
			SetQueryParams(map[string]string{
				"type": "metrics",
				"max":  config.API.SuggestMax,
				"q":    metric,
			}).
			Get(uri)
		if err != nil {
			log.Printf("GetMetrics: Error %v", err)
		}
		var ml []string
		json.Unmarshal(resp.Body(), &ml)

		for _, m := range ml {
			if config.Settings.LogLevel >= 4 {
				log.Printf("GetMetrics: %s", m)
			}
			getmetrics <- m
		}
	}
	close(getmetrics)
}

// GetTSList gets timeseries list
func GetTSList(rr *RoundRobin, wait *sync.WaitGroup) {
	defer wait.Done()
	for metric := range getmetrics {
		time.Sleep(25 * time.Millisecond)
		if config.Settings.LogLevel >= 4 {
			log.Println("GetTSList: ", metric)
		}
		uri := rr.Get() + "/api/search/lookup"
		jsondata := `{"metric":"` + metric + `","tags":[{"key":"host","value":"*"}],"useMeta":` + config.API.LookupUseMeta + `,"limit":` + config.API.LookupLimit + `}`

		resp, err := resty.R().
			SetResult(&Lookup{}).
			SetBody(jsondata).
			Post(uri)
		if err != nil {
			log.Printf("GetTSList: Error %v", err)
		}
		tslist := resp.Result().(*Lookup)
		fmt.Printf("GetTSList total results %s: %d\n", metric, tslist.TotalResults)
		if tslist != nil {
			for i := 0; i < len(tslist.Results); i += config.Settings.Batch {
				var k []string
				j := i + config.Settings.Batch
				if j > len(tslist.Results) {
					j = len(tslist.Results)
				}
				for _, t := range tslist.Results[i:j] {
					k = append(k, string(t.Tsuid))
				}
				jobid := fmt.Sprintf("%s_%d", metric, i)
				gettslist <- Gettslist{jobid, k}
			}
		}
	}
}

// GetRollup get rollup datapoints for time series
func GetRollup(endTime int64, startTime int64, interval string, rr *RoundRobin, wait *sync.WaitGroup) {
	defer wait.Done()
	for tsuid := range gettslist {
		//fmt.Println("gettslist capacity is", cap(gettslist))
		//fmt.Println("gettslist length is", len(gettslist))
		//fmt.Printf("getrollup tsuid: %v\n", tsuid)

		var jsondata Query
		var queries SubQuery
		jsondata.Start = startTime
		jsondata.End = endTime
		queries.Aggregator = "none"
		queries.Downsample = interval + "-avg"
		for _, chunk := range tsuid.list {
			queries.Tsuids = append(queries.Tsuids, chunk)
		}
		jsondata.Queries = append(jsondata.Queries, queries)
		time.Sleep(480 * time.Millisecond)
		uri := rr.Get() + "/api/query"
		resp, err := resty.R().
			SetResult(&QueryRespItem{}).
			SetBody(jsondata).
			Post(uri)
		if err != nil {
			log.Printf("GetRollup: Job ID %s Error %v", tsuid.jobid, err)
		}
		time.Sleep(25 * time.Millisecond)
		result := resp.Result().(*QueryRespItem)
		//fmt.Println(result)
		if len(*result) != 0 {
			if config.Settings.LogLevel >= 3 {
				log.Println("GetRollup: Job ID", tsuid.jobid)
			}
			converted := convertRollup(result, interval)
			getrollup <- Getrollup{tsuid.jobid, converted}
		} else {
			if config.Settings.LogLevel >= 4 {
				log.Printf("GetRollup: Job ID %s No Results", tsuid.jobid)
			}
		}
	}
}

// convertRollup takes aggregated rollup data and formated it for posting to rollup api
func convertRollup(in *QueryRespItem, interval string) []Rollup {
	var temp Rollup
	var result []Rollup

	for _, item := range *in {
		for dps, value := range item.Dps {
			temp.Metric = item.Metric
			temp.Tags = item.Tags
			if _, ok := temp.Tags["_aggregate"]; ok {
				delete(temp.Tags, "_aggregate")
			}
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
		//fmt.Println("DATA ROLLUP: ", data.rollup)
		uri := wr.Get() + "/api/put?summary"
		resp, err := resty.R().
			SetResult(&RollupResponse{}).
			SetBody(data.rollup).
			Post(uri)
		if err != nil {
			log.Printf("PostRollup: Job ID %s Error %v", data.jobid, err)
		}
		result := resp.Result().(*RollupResponse)
		if result.Success != 0 || result.Failed != 0 {
			if config.Settings.LogLevel >= 2 {
				size := len(data.rollup)
				log.Printf("PostRollup: Job ID %s Size %d Results Success %d Failed %d", data.jobid, size, result.Success, result.Failed)
			}
		} else {
			if config.Settings.LogLevel >= 4 {
				log.Printf("PostRollup: Job ID %s No Results", data.jobid)
			}
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
	log.Println("GetTSListWorkerPool Done")
	wait.Done()
}

//GetRollupWorkerPool spawns workers
func GetRollupWorkerPool(noOfWorkers int, endTime int64, startTime int64, interval string, rr *RoundRobin, wait *sync.WaitGroup) {
	var wg sync.WaitGroup
	for i := 0; i < noOfWorkers; i++ {
		wg.Add(1)
		go GetRollup(endTime, startTime, interval, rr, &wg)
	}
	wg.Wait()
	close(getrollup)
	log.Println("GetRollupWorkerPool Done")
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
	log.Println("PostRollupWorkerPool Done")
	wait.Done()
}

func main() {
	// Start Timer
	sTime := time.Now()

	// Prase CLI flags
	flag.Parse()

	// Config
	if _, err := toml.DecodeFile("./opentsdbrollup.toml", &config); err != nil {
		log.Println("Read Config Error", err)
	}

	resty.
		SetDebug(config.Rest.Debug).
		SetRetryCount(config.Rest.RetryCount).
		SetRetryWaitTime(time.Duration(config.Rest.RetryWaitTime)*time.Millisecond).
		SetRetryMaxWaitTime(time.Duration(config.Rest.RetryMaxWaitTime)*time.Second).
		SetTimeout(time.Duration(config.Rest.Timeout)*time.Minute).
		SetHeader("Accept", "application/json")

	// API Servers
	rr := NewRoundRobin(config.Servers.ReadEndpoint)
	wr := NewRoundRobin(config.Servers.WriteEndpoint)

	// Set Time Window
	// default to now
	toTime := time.Now().UTC()
	fromTime := time.Now().UTC()

	// parse flag to time
	if *to != "" {
		toTim, err := dateparse.ParseAny(*to)
		if err != nil {
			log.Fatal("Parse to time input error:", err.Error())
		}
		toTime = toTim
		//toTime = time.Unix(*to, 0).UTC()
	}

	// set end time by rounding to time to whole hour
	endTime := time.Date(toTime.Year(), toTime.Month(), toTime.Day(), toTime.Hour(), 0, 0, 0, toTime.Location())

	// parse hour window
	hourspast := config.Settings.HoursPast
	if *window > 0 {
		hourspast = *window * -1
	}
	log.Printf("Rollup Window Size set %d hours\n", hourspast)
	// parse flag from time and if set override window.
	if *from != "" {

		fromTim, err := dateparse.ParseAny(*from)
		if err != nil {
			log.Fatal("Parse from time input error:", err.Error())
		}
		fromTime = fromTim
		log.Println("Rollup From set Window Size overridden")
	} else {
		fromTime = endTime.Add(time.Duration(time.Duration(hourspast) * time.Hour))
	}
	startTime := time.Date(fromTime.Year(), fromTime.Month(), fromTime.Day(), fromTime.Hour(), 0, 0, 0, fromTime.Location())

	// Final rollup window
	log.Printf("Rollup Window - StartTime: %s EndTime: %s Interval: %s\n", startTime.Format(time.UnixDate), endTime.Format(time.UnixDate), config.Settings.Interval)

	// check from time inversion
	checktime := endTime.Sub(startTime)
	if checktime.Seconds() < 1 {
		log.Fatal("window size in wrong direction:", checktime.Seconds(), " seconds")
	}

	// Start pipeline
	wait.Add(4)
	go GetMetrics(rr, &wait)
	go GetTSListWorkerPool(config.Settings.ReadWorkers, rr, &wait)
	go GetRollupWorkerPool(config.Settings.ReadWorkers, endTime.Unix(), startTime.Unix(), config.Settings.Interval, rr, &wait)
	go PostRollupWorkerPool(config.Settings.WriteWorkers, wr, &wait)
	wait.Wait()

	// End Timer
	eTime := time.Now()
	diff := eTime.Sub(sTime)
	log.Println("total time taken ", diff.Seconds(), "seconds")
	os.Exit(0)
}
