package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	aggColNo  = 1
	bulkCount = 5000
	//maxWorkers = 4
	maxQueues = 10000
)

func processData(r [][]string) map[string]int {
	var m = map[string]int{}
	for _, rec := range r {
		m[rec[aggColNo]]++
	}
	// log.Printf("Goroutine cocurrency:%d  mapsize=%d", runtime.NumGoroutine(), len(m))
	return m
}

var filename = flag.String("f", "REQUIRED", "source CSV file")
var numChannels = flag.Int("c", 4, "num of parallel channels")

func main() {
	flag.Parse()
	fmt.Print(strings.Join(flag.Args(), "\n"))
	if *filename == "REQUIRED" {
		return
	}

	start := time.Now()
	//d := NewDispatcher()
	//
	//      CSV File Open
	//
	csvfile, err := os.Open(*filename)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer csvfile.Close()

	reader := csv.NewReader(csvfile)
	reader.FieldsPerRecord = -1 // CSVの列数が足りない行を無視する

	jobCh := make(chan [][]string, maxQueues)
	workerCh := make(chan bool, *numChannels)
	sinkCh := make(chan map[string]int, *numChannels)
	//quitChan := make(chan bool)
	go func() {
		defer close(jobCh)
		running := true
		for running {
			records := make([][]string, 0, bulkCount)
			// バルク単位でCSVを読み込む
			for bi := 0; bi < bulkCount; bi++ {
				record, err := reader.Read()
				if err == io.EOF {
					running = false
					break
				} else if err != nil {
					fmt.Println("file read error : ", err)
					panic(err)
				}
				records = append(records, record)
			}
			// バルクレコードをworkerへdispatch
			jobCh <- records
		}
	}()
	go func() {
		var wg sync.WaitGroup
		for job := range jobCh {
			workerCh <- true
			wg.Add(1)
			m := map[string]int{}
			go func(v [][]string) {
				defer func() { <-workerCh }()
				defer wg.Done()
				wordMap := processData(v)
				for mk, mv := range wordMap {
					m[mk] += mv
				}
				sinkCh <- m
			}(job)
		}
		fmt.Println("quit")
		wg.Wait()
		close(sinkCh)
	}()
	resMap := map[string]int{}
	for m := range sinkCh {
		for k, v := range m {
			resMap[k] += v
		}
	}

	// 集計結果をarray化してソートする

	results := []Entry{}
	for k, v := range resMap {
		e := Entry{k, v}
		results = append(results, e)
	}
	sort.Slice(results, func(i, j int) bool {
		if results[i].value == results[j].value {
			return (results[i].name < results[j].name)
		}
		return (results[i].value > results[j].value)
	})
	for i := 0; i < 10 && i < len(results); i++ {
		fmt.Println(results[i])
	}

	resuleTime := fmt.Sprintf("\n%2fs", time.Since(start).Seconds())
	fmt.Println(resuleTime)
}

type Entry struct {
	name  string
	value int
}
