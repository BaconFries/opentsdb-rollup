package main

import (
	"encoding/json"
	"fmt"
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
func GetRollup(url []string, tsuid []string, endTime int64, startTime int64) *QueryRespItem {

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
	//fmt.Printf("GetRollup, %v, %v, %v, %v\n", url[0], tsuid, endTime, startTime)
	result := resp.Result().(*QueryRespItem)
	return result
}

// PostRollup send rollup datapoints for time series
func PostRollup(url string, data Rollup) interface{} {
	resp, err := resty.R().
		SetBody(data).
		Post(url + "/api/rollup?details")
	if err != nil {
		panic(err)
	}

	return resp.StatusCode()
}

// convertRollup takes aggregated rollup data and formated it for posting to rollup api
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

// channels
var jobs = make(chan TSJob, 10)
var results = make(chan TSResult, 10)
var rujobs = make(chan RUJob, 10)
var ruresults = make(chan RUResult, 10)
var pjobs = make(chan PJob, 10)
var presults = make(chan PResult, 10)

func createWorkerPool(config tomlConfig) {
	var twg sync.WaitGroup
	for i := 0; i < config.API.NoOfWorkers; i++ {
		twg.Add(1)
		go tsworker(&twg, config)
	}
	twg.Wait()
	close(results)
}
func rucreateWorkerPool(config tomlConfig, endTime int64, startTime int64) {
	var rwg sync.WaitGroup
	for i := 0; i < config.API.NoOfWorkers; i++ {
		rwg.Add(1)
		go ruworker(&rwg, config, startTime, endTime)
	}
	rwg.Wait()
	close(ruresults)
}
func pcreateWorkerPool(config tomlConfig) {
	var pwg sync.WaitGroup
	for i := 0; i < config.API.NoOfWorkers; i++ {
		pwg.Add(1)
		go pworker(&pwg, config)
	}
	pwg.Wait()
	close(presults)
}
func allocate(metriclist []string) {
	for i, metric := range metriclist {
		job := TSJob{i, metric}
		jobs <- job
	}
	//close(jobs)
}
func ruallocate(tsuidlist TSResult) {

	//fmt.Printf("ruallocate %v\n", tsuidlist)

	batch := 20
	for i := 0; i < len(tsuidlist.tsuid); i += batch {
		j := i + batch
		if j > len(tsuidlist.tsuid) {
			j = len(tsuidlist.tsuid)
		}
		var tsuid []string
		for _, id := range tsuidlist.tsuid[i:j] {

			tsuid = append(tsuid, id)
		}
		//fmt.Printf("ruallocateresult %v\n", tsuid)
		job := RUJob{tsuid}
		rujobs <- job
	}
	//close(rujobs)
}
func pallocate(postlist []Rollup) {
	for i, pl := range postlist {
		//fmt.Printf("postlist %d, %v\n", i, pl)
		job := PJob{i, pl}
		pjobs <- job
	}
	close(pjobs)
}
func tsworker(twg *sync.WaitGroup, config tomlConfig) {
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
	twg.Done()
}
func ruworker(rwg *sync.WaitGroup, config tomlConfig, endTime int64, startTime int64) {
	for job := range rujobs {

		rollupdata := GetRollup(config.Servers.ReadEndpoint, job.tsuid, endTime, startTime)
		//fmt.Printf("ruworker %v\n", rollupdata)
		out := RUResult{job, rollupdata}
		ruresults <- out
	}
	rwg.Done()
}
func pworker(pwg *sync.WaitGroup, config tomlConfig) {
	for job := range pjobs {
		var post interface{}
		//fmt.Printf("PostRollup Worker %d, %v\n", job.id, job.post)
		post = PostRollup(config.Servers.WriteEndpoint, job.post)
		//fmt.Printf("PostRollup Worker result %v\n", post)
		out := PResult{job, post}
		presults <- out
	}
	pwg.Done()
}
func result(done chan bool, config tomlConfig, endTime int64, startTime int64) {
	for result := range results {

		// Start rollup worker pool
		go ruallocate(result)
		rudone := make(chan bool)
		go ruresult(rudone, config)
		rucreateWorkerPool(config, endTime, startTime)
		<-rudone
		//fmt.Printf("Job id %d, %v, %v\n", result.job.id, result.job.metric, result.tsuid)
	}
	done <- true
}
func ruresult(rudone chan bool, config tomlConfig) {
	for result := range ruresults {
		//fmt.Printf("ruresult Job id %v, %v\n", result.job, result.resp)
		postrollup := convertRollup(result.resp)

		go pallocate(postrollup)
		pdone := make(chan bool)
		go presult(pdone, config)
		pcreateWorkerPool(config)
		<-pdone
	}
	rudone <- true
}
func presult(pdone chan bool, config tomlConfig) {
	for result := range presults {
		fmt.Printf("presult Job id %v, %v\n", result.job, result.resp)
		time.Sleep(time.Millisecond)
	}
	pdone <- true
}
func main() {

	sTime := time.Now()

	// Config
	var config tomlConfig
	if _, err := toml.DecodeFile("./opentsdb-rollup.toml", &config); err != nil {
		fmt.Println(err)
	}
	//fmt.Printf("%#v\n", config)

	// set http rest client defaults
	resty.SetDebug(false)
	resty.SetRetryCount(3)
	resty.SetHeader("Accept", "application/json")

	// set time range
	originalTime := time.Now()
	startTime := time.Date(originalTime.Year(), originalTime.Month(), originalTime.Day(), originalTime.Hour(), 0, 0, 0, originalTime.Location())
	endTime := startTime.Add(time.Duration(time.Duration(-1) * time.Hour))

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
	fmt.Printf("build metric list %s\n", elapsed)

	// Start time series uid list worker pool
	go allocate(metriclist)
	done := make(chan bool)
	go result(done, config, endTime.Unix(), startTime.Unix())
	createWorkerPool(config)
	<-done

	eTime := time.Now()
	diff := eTime.Sub(sTime)
	fmt.Println("\ncleartotal time taken ", diff.Seconds(), "seconds")

}
