package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"sync"
	"time"
)

func main() {
	var fname string
	var concurrency int
	flag.StringVar(&fname, "f", "test_3_000_000.csv", "CSV file")
	flag.IntVar(&concurrency, "c", 16, "concurrency")
	flag.Parse()
	fp, err := os.Open(fname)
	if err != nil {
		log.Fatalln(err)
	}
	begin := time.Now()
  // -----------------------------------------------------------------
  // 速度優先のプラン
  // 汎用的に利用する場合は、コメント下記ブロックをコメント内と入れ替えること
  // --------------------------- ここから -----------------------------
	task := []chan string{}
	res := []map[string]int{}
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		ch := make(chan string, 1000)
		task = append(task, ch)
		m := map[string]int{}
		res = append(res, m)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for s := range ch {
				m[s]++
  // --------------------------- ここまで -----------------------------
  /*
  --------------------------------------------------------------------
  --   Generic Plan
  ------------------------------ ここから -----------------------------
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
  ------------------------------ ここまで ----------------------------- */
			}
		}()
	}
	reader := bufio.NewScanner(fp)
	i := 0
	for reader.Scan() {
		b := reader.Bytes()
		start := bytes.IndexByte(b, ',')
		if start < 0 {
			continue
		}
		start += 2
		offset := bytes.IndexByte(b[start:], ',') - 1
		name := string(b[start : start+offset])
		task[i%concurrency] <- name
		i++
	}
	for i := 0; i < concurrency; i++ {
		close(task[i])
	}
	wg.Wait()
	m := map[string]int{}
	for _, r := range res {
		for k, v := range r {
			m[k] += v
		}
	}
	resultTime := time.Since(begin)

	results := []Entry{}
	for k, v := range m {
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

	fmt.Printf("\n%s\n", resultTime)
}

type Entry struct {
	name  string
	value int
}
