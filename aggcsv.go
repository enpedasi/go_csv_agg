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

// usage : aggcsv -f test_300000.csv

var filename = flag.String("f", "REQUIRED", "source CSV file")
var numChannels = flag.Int("c", 4, "num of parallel channels")

const BulkCount = 500
const AggColNo = 1

//var bufferedChannels = flag.Bool("b", false, "enable buffered channels")

func main() {

	flag.Parse()
	fmt.Print(strings.Join(flag.Args(), "\n"))
	if *filename == "REQUIRED" {
		return
	}

	start := time.Now()

	csvfile, err := os.Open(*filename)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer csvfile.Close()

	reader := csv.NewReader(csvfile)
	reader.FieldsPerRecord = -1 // CSVの列数が足りない行を無視する

	i := 0
	ch := make(chan map[string]int)
	var wg sync.WaitGroup
	for {
		records := make([][]string, 0, BulkCount)
		isLast := false

		for bi := 0; bi < BulkCount; bi++ {

			record, err := reader.Read()
			if err == io.EOF {
				isLast = true
				break
			} else if err != nil {
				fmt.Println("file read error : ", err)
				panic(err)
			}
			records = append(records, record)
		}
		i++
		wg.Add(1)
		go func(r [][]string, i int) {
			defer wg.Done()
			ch <- processData(i, r)
			// processData(i, r)
			// ch <- r
		}(records, i)
		if isLast {
			break
		}
		//		fmt.Printf("\rgo %d", i)
	}

	// closer
	go func() {
		wg.Wait()
		close(ch)
	}()

	// print channel results (necessary to prevent exit programm before)
	sum_map := map[string]int{}
	j := 0
	for rec := range ch {
		j++
		// fmt.Printf("\r\t\t\t\t | done %d ", j, rec)
		for mk, mv := range rec {
			sum_map[mk] += mv
		}
	}

	// fmt.Println(sum_map)
	results := List{}
	for k, v := range sum_map {
		e := Entry{k, v}
		results = append(results, e)
	}

	result_time := fmt.Sprintf("\n%2fs", time.Since(start).Seconds())
	sort.Sort(results)

	for i := 0; i < 5; i++ {
		fmt.Println(results[i])
	}
	fmt.Println(result_time)
}

func processData(i int, r [][]string) map[string]int {
	// time.Sleep(time.Duration(1000+rand.Intn(8000)) * time.Millisecond)
	var m = map[string]int{}
	// fmt.Printf("\r\t\t| proc %d", i)
	for _, rec := range r {
		// fmt.Println(rec)
		m[rec[AggColNo]]++
	}

	// for _, wd := range r {	m[wd]++	}
	return m
}

// for sort
type Entry struct {
	name  string
	value int
}
type List []Entry

func (l List) Len() int {
	return len(l)
}

func (l List) Swap(i, j int) {
	l[i], l[j] = l[j], l[i]
}

func (l List) Less(i, j int) bool {
	if l[i].value == l[j].value {
		return (l[i].name < l[j].name)
	} else {
		return (l[i].value > l[j].value)
	}
}
