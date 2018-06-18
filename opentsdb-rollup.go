package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
	"gopkg.in/resty.v1"
)

// GetMetrics search metric names from suggest api call
func GetMetrics(q string, config tomlConfig) []string {
	resp, err := resty.R().
		SetQueryParams(map[string]string{
			"type": "metrics",
			"max":  config.API.SuggestMax,
			"q":    q,
		}).
		Get(config.Servers.ReadEndpoint[0] + "/api/suggest")
	if err != nil {
		panic(err)
	}
	var metriclist []string
	json.Unmarshal(resp.Body(), &metriclist)
	//fmt.Printf("metriclist1: %v\n", metriclist)
	return metriclist
}

// GetTSList gets timeseries list
func GetTSList(metric string, config tomlConfig) *Lookup {
	resp, err := resty.R().
		SetResult(&Lookup{}).
		SetQueryParams(map[string]string{
			"useMeta": config.API.LookupUseMeta,
			"limit":   config.API.LookupLimit,
			"m":       metric,
		}).
		Get(config.Servers.ReadEndpoint[0] + "/api/search/lookup")
	if err != nil {
		panic(err)
	}
	tslist := resp.Result().(*Lookup)
	//fmt.Printf("tslist: %v \n", tslist)
	return tslist
}

// GetRollup get rollup datapoints for time series
func GetRollup(url []string, tsuid []string, startTime int64, endTime int64) *QueryRespItem {

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
	resp, err := resty.R().
		SetResult(&QueryRespItem{}).
		SetBody(jsondata).
		Post(url[0] + "/api/query")
	if err != nil {
		panic(err)
	}
	result := resp.Result().(*QueryRespItem)
	return result
}

// PostRollup send rollup datapoints for time series
func PostRollup(url string, data []Rollup) {
	resp, err := resty.R().
		SetBody(data).
		Post(url + "/api/rollup?details")
	if err != nil {
		panic(err)
	}
	resp.Result()
	return
}

func convertRollup(in *QueryRespItem) []Rollup {
	var test Rollup
	var result []Rollup
	for i, item := range *in {
		agg := "SUM"
		if i == 1 {
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
	return result
}

var jobs = make(chan TSJob, 10)
var results = make(chan TSResult, 10)
var rujobs = make(chan RUJob, 10)
var ruresults = make(chan RUResult, 10)

func ruworker(wg *sync.WaitGroup, config tomlConfig, startTime int64, endTime int64) {
	for job := range rujobs {

		rollupdata := GetRollup(config.Servers.ReadEndpoint, job.tsuid, startTime, endTime)

		out := RUResult{job, rollupdata}
		ruresults <- out
	}
	wg.Done()
}
func rucreateWorkerPool(noOfWorkers int, config tomlConfig, startTime int64, endTime int64) {
	var wg sync.WaitGroup
	for i := 0; i < noOfWorkers; i++ {
		wg.Add(1)
		go ruworker(&wg, config, startTime, endTime)
	}
	wg.Wait()
	close(results)
}
func ruallocate(tsuidlist TSResult) {

	fmt.Printf("ruallocate %v\n", tsuidlist)

	batch := 20
	for i := 0; i < len(tsuidlist.tsuid); i += batch {
		j := i + batch
		if j > len(tsuidlist.tsuid) {
			j = len(tsuidlist.tsuid)
		}
		var tsuid []string
		for _, id := range tsuidlist.tsuid[i:j] {
			fmt.Printf("ruallocateresult %v\n", id)

			tsuid = append(tsuid, id)
		}
		job := RUJob{tsuid}
		rujobs <- job
	}
	close(rujobs)
}
func ruresult(done chan bool) {
	for result := range results {
		fmt.Printf("rureslt Job id %d, %v, %v\n", result.job.id, result.job.metric, result.tsuid)
	}
	done <- true
}

func tsworker(wg *sync.WaitGroup, config tomlConfig) {
	for job := range jobs {

		var tsuidlist []string
		output := GetTSList(job.metric, config)
		for _, res := range output.Results {
			result := res.(map[string]interface{})
			tsuidlist = append(tsuidlist, result["tsuid"].(string))
		}
		out := TSResult{job, tsuidlist}
		results <- out
	}
	wg.Done()
}
func createWorkerPool(noOfWorkers int, config tomlConfig) {
	var wg sync.WaitGroup
	for i := 0; i < noOfWorkers; i++ {
		wg.Add(1)
		go tsworker(&wg, config)
	}
	wg.Wait()
	close(results)
}
func allocate(metriclist []string) {
	for i, metric := range metriclist {
		job := TSJob{i, metric}
		jobs <- job
	}
	close(jobs)
}

func result(done chan bool, noOfWorkers int, config tomlConfig, endTime int64, startTime int64) {
	for result := range results {

		// Start rollup worker pool
		go ruallocate(result)
		rudone := make(chan bool)
		go ruresult(rudone)
		rucreateWorkerPool(noOfWorkers, config, endTime, startTime)
		<-rudone
		fmt.Printf("Job id %d, %v, %v\n", result.job.id, result.job.metric, result.tsuid)
	}
	done <- true
}

func main() {

	sTime := time.Now()

	//noOfJobs := 10
	noOfWorkers := 10

	// Config
	var config tomlConfig
	if _, err := toml.DecodeFile("./opentsdb-rollup.toml", &config); err != nil {
		fmt.Println(err)
	}
	fmt.Printf("%#v\n", config)

	// set http rest client defaults
	resty.SetDebug(false)
	resty.SetRetryCount(3)
	resty.SetHeader("Accept", "application/json")

	// set time range
	originalTime := time.Now()
	endTime := time.Date(originalTime.Year(), originalTime.Month(), originalTime.Day(), originalTime.Hour(), 0, 0, 0, originalTime.Location())
	startTime := endTime.Add(time.Duration(time.Duration(-1) * time.Hour))

	// build metric list
	start := time.Now()
	var metriclist []string
	for _, metric := range config.Metric.List {
		ml := GetMetrics(metric, config)
		for _, m := range ml {
			metriclist = append(metriclist, m)
		}
	}
	elapsed := time.Since(start)
	log.Printf("build metric list %s", elapsed)

	// Start time series uid list worker pool
	go allocate(metriclist)
	done := make(chan bool)
	go result(done, noOfWorkers, config, endTime.Unix(), startTime.Unix())
	createWorkerPool(noOfWorkers, config)
	<-done

	eTime := time.Now()
	diff := eTime.Sub(sTime)
	fmt.Println("\ncleartotal time taken ", diff.Seconds(), "seconds")

	/*
		batch := 20
		for i := 0; i < len(tsuidlist); i += batch {
			j := i + batch
			if j > len(tsuidlist) {
				j = len(tsuidlist)
			}
			fmt.Printf("tsuidlist[%v:%v]\n", i, j)
			rollupdata := GetRollup(config.Servers.ReadEndpoint, tsuidlist, i, j, startTime.Unix(), endTime.Unix())
			fmt.Printf("rollupdata %v\n", rollupdata)
		}
	*/
	//for _, res := range tsuidlist {
	//	rollupdata := GetRollup(config.Servers.ReadEndpoint, res, startTime.Unix(), endTime.Unix())
	//postrollup := convertRollup(rollupdata)
	//PostRollup(config.Servers.WriteEndpoint, postrollup)
	//}

}
