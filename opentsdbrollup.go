package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
	"gopkg.in/resty.v1"
)

// NewRoundRobin ...
func NewRoundRobin(config tomlConfig) *RoundRobin {
	return &RoundRobin{
		current: 0,
		pool:    config.Servers.ReadEndpoint,
	}
}

// Get ...
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
func GetMetrics(config tomlConfig, rr *RoundRobin) []string {
	var metriclist []string
	for _, metric := range config.Metric.List {
		resp, err := resty.R().
			SetQueryParams(map[string]string{
				"type": "metrics",
				"max":  config.API.SuggestMax,
				"q":    metric,
			}).
			Get(rr.Get() + "/api/suggest")
		if err != nil {
			panic(err)
		}
		var ml []string
		json.Unmarshal(resp.Body(), &ml)

		for _, m := range ml {
			metriclist = append(metriclist, m)
		}
	}

	return metriclist

}

// GetTSList gets timeseries list
func GetTSList(getmetrics chan string, gettslist chan []string, config tomlConfig, rr *RoundRobin) {
	wait.Add(1)
	for metric := range getmetrics {

		//var ts [][]string
		url := rr.Get() + "/api/search/lookup"
		fmt.Println(url)
		resp, err := resty.R().
			SetResult(&Lookup{}).
			SetQueryParams(map[string]string{
				"useMeta": config.API.LookupUseMeta,
				"limit":   config.API.LookupLimit,
				"m":       metric,
			}).
			Get(url)
		if err != nil {
			panic(err)
		}
		tslist := resp.Result().(*Lookup)
		//fmt.Printf("tslist: %v \n", tslist)

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
	wait.Done()

}

// GetRollup get rollup datapoints for time series
func GetRollup(gettslist chan []string, getrollup chan []Rollup, endTime int64, startTime int64, config tomlConfig, rr *RoundRobin) {
	wait.Add(1)
	for tsuid := range gettslist {

		//fmt.Printf("getrollup tsuid: %v\n", tsuid)

		var jsondata Query
		var queries SubQuery
		jsondata.Start = startTime
		jsondata.End = endTime
		queries.Aggregator = "none"
		queries.Downsample = "1h-count"

		//queries.Tsuids = tsuid
		for _, chunk := range tsuid {
			queries.Tsuids = append(queries.Tsuids, chunk)
		}

		jsondata.Queries = append(jsondata.Queries, queries)
		queries.Downsample = "1h-sum"
		jsondata.Queries = append(jsondata.Queries, queries)

		url := rr.Get() + "/api/query"
		//fmt.Printf("getrollup jsondata: %v\n", jsondata)
		resp, err := resty.R().
			SetResult(&QueryRespItem{}).
			SetBody(jsondata).
			Post(url)
		if err != nil {
			panic(err)
		}
		//fmt.Printf("GetRollup, %v, %v, %v, %v\n", config.Servers.ReadEndpoint[0], tsuid, endTime, startTime)
		result := resp.Result().(*QueryRespItem)

		if len(*result) != 0 {
			//fmt.Printf("getrollup result: %v\n", result)
			converted := convertRollup(result)
			getrollup <- converted
		} else {
			log.Println("GetRollup no results")
		}
	}
	wait.Done()

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
func PostRollup(input chan []Rollup, config tomlConfig) {
	// Increment the wait group counter
	wait.Add(1)
	for data := range input {
		//fmt.Printf("post data: %v\n", data)
		resp, err := resty.R().
			SetBody(data).
			Post(config.Servers.WriteEndpoint + "/api/rollup?details")
		if err != nil {
			fmt.Printf("rest error: %v\n", err)
		}
		//fmt.Printf("Error: %v\n", err)
		//fmt.Printf("Response Status Code: %v\n", resp.StatusCode())
		//fmt.Printf("Response Status: %v\n", resp.Status())
		//fmt.Printf("Response Time: %v\n", resp.Time())
		//fmt.Printf("Response Received At: %v\n", resp.ReceivedAt())
		fmt.Printf("Response Body: %v\n", resp)
	}
	wait.Done()
}

var wait = sync.WaitGroup{}
var end = flag.Int64("end", 0, "end of window. (Required)")

func main() {

	flag.Parse()
	// Config
	var config tomlConfig
	if _, err := toml.DecodeFile("./opentsdbrollup.toml", &config); err != nil {
		fmt.Println(err)
	}

	// Concurrency
	getrollup := make(chan []Rollup, 200)
	gettslist := make(chan []string, 200)
	getmetrics := make(chan string, 100)
	rr := NewRoundRobin(config)

	// set http rest client defaultsq
	resty.SetDebug(false)
	resty.SetRetryCount(6)
	resty.SetHeader("Accept", "application/json")

	// set time range
	//originalTime := time.Now().UTC()
	originalTime := time.Unix(*end, 0).UTC()
	//originalTime := time. time.Duration(time.Duration(end) * time.Hour)

	endTime := time.Date(originalTime.Year(), originalTime.Month(), originalTime.Day(), originalTime.Hour(), 0, 0, 0, originalTime.Location())
	startTime := endTime.Add(time.Duration(time.Duration(config.API.HoursPast) * time.Hour))
	fmt.Printf("Rollup Window - StartTime: %s EndTime: %s \n", startTime.Format(time.UnixDate), endTime.Format(time.UnixDate))
	//os.Exit(3)
	// build metric list
	metriclist := GetMetrics(config, rr)

	wc := config.API.Concurrency / 2

	// Start PostRollup workers
	for i := 0; i < config.API.Concurrency; i++ {
		go PostRollup(getrollup, config)
	}

	// Start GetRollup workers
	for i := 0; i < config.API.Concurrency; i++ {
		go GetRollup(gettslist, getrollup, endTime.Unix(), startTime.Unix(), config, rr)
	}

	// Start GetTSList workers
	for i := 0; i < wc; i++ {
		go GetTSList(getmetrics, gettslist, config, rr)
	}

	for _, m := range metriclist {
		fmt.Println(m)
		getmetrics <- m
	}

	//wait.Wait()
	//close(getmetrics)
	//close(gettslist)
	//close(getrollup)
}
