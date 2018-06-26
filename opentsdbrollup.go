package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
)

// NewRoundRobin ...
func NewRoundRobin(pool []string) *RoundRobin {
	return &RoundRobin{
		current: 0,
		pool:    pool,
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
func GetMetrics(getmetrics chan<- string, config tomlConfig, rr *RoundRobin, wait *sync.WaitGroup) {
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
			//fmt.Printf("GetMetrics: %s\n", m)
			getmetrics <- m
		}
	}
	close(getmetrics)
	wait.Done()
}

// GetTSList gets timeseries list
func GetTSList(getmetrics chan string, gettslist chan<- []string, config tomlConfig, rr *RoundRobin, wait *sync.WaitGroup) {
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
	close(gettslist)
	wait.Done()
}

// GetRollup get rollup datapoints for time series
func GetRollup(gettslist chan []string, getrollup chan<- []Rollup, endTime int64, startTime int64, config tomlConfig, rr *RoundRobin, wait *sync.WaitGroup) {

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
	close(getrollup)
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
func PostRollup(input chan []Rollup, wr *RoundRobin, wait *sync.WaitGroup) {

	for data := range input {
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
	wait.Done()
}

var wait = sync.WaitGroup{}
var end = flag.Int64("end", 0, "end of window.")
var hours = flag.Int("hours", 0, "window size.")

func main() {

	flag.Parse()
	// Config
	var config tomlConfig
	if _, err := toml.DecodeFile("./opentsdbrollup.toml", &config); err != nil {
		fmt.Println(err)
	}

	// Concurrency
	getrollup := make(chan []Rollup, 500)
	gettslist := make(chan []string, 500)
	getmetrics := make(chan string, 500)
	rr := NewRoundRobin(config.Servers.ReadEndpoint)
	wr := NewRoundRobin(config.Servers.WriteEndpoint)

	// set time range
	originalTime := time.Now().UTC()
	if *end > 0 {
		originalTime = time.Unix(*end, 0).UTC()
	}
	hourspast := config.API.HoursPast
	if *hours > 0 {
		hourspast = *hours * -1
		fmt.Printf("Rollup Window Size override %d hours\n", hourspast)
	}
	//originalTime := time. time.Duration(time.Duration(end) * time.Hour)
	endTime := time.Date(originalTime.Year(), originalTime.Month(), originalTime.Day(), originalTime.Hour(), 0, 0, 0, originalTime.Location())
	startTime := endTime.Add(time.Duration(time.Duration(hourspast) * time.Hour))
	fmt.Printf("Rollup Window - StartTime: %s EndTime: %s \n", startTime.Format(time.UnixDate), endTime.Format(time.UnixDate))

	// build metric list
	wait.Add(16)
	go GetMetrics(getmetrics, config, rr, &wait)
	for i := 0; i < 10; i++ {
		go GetTSList(getmetrics, gettslist, config, rr, &wait)
		go GetRollup(gettslist, getrollup, endTime.Unix(), startTime.Unix(), config, rr, &wait)
	}
	for i := 0; i < 5; i++ {
		go PostRollup(getrollup, wr, &wait)
	}
	wait.Wait()

}
