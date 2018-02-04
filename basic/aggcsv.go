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

const (
	BulkCount = 500
	AggColNo  = 1
)

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

		// バルク単位でCSVを読み込む
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
		}(records, i)
		if isLast {
			break
		}
	}
	// closer
	go func() {
		wg.Wait()
		close(ch)
	}()

	// チャネルからクラスタ化された集計結果を集める

	sumMap := map[string]int{}
	for rec := range ch {
		for mk, mv := range rec {
			sumMap[mk] += mv
		}
	}

	// 集計結果をarray化してソートする

	results := List{}
	for k, v := range sumMap {
		e := Entry{k, v}
		results = append(results, e)
	}

	resuleTime := fmt.Sprintf("\n%2fs", time.Since(start).Seconds())
	sort.Sort(results)

	for i := 0; i < 10 && i < len(results); i++ {
		fmt.Println(results[i])
	}
	fmt.Println(resuleTime)
}

func processData(i int, r [][]string) map[string]int {
	var m = map[string]int{}
	for _, rec := range r {
		m[rec[AggColNo]]++
	}
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
