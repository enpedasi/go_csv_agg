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

type (
	// Dispatcher represents a management workers.
	Dispatcher struct {
		pool    chan *worker
		queue   chan interface{}
		workers []*worker
		wg      sync.WaitGroup
		quit    chan struct{}
		sink    chan map[string]int
	}

	// worker represents the worker that executes the job.
	worker struct {
		dispatcher *Dispatcher
		data       chan interface{}
		quit       chan struct{}
	}
)

const (
	aggColNo   = 1
	bulkCount  = 500
	maxWorkers = 10
	maxQueues  = 20000
)

// NewDispatcher returns a pointer of Dispatcher.
func NewDispatcher() *Dispatcher {
	d := &Dispatcher{
		pool:  make(chan *worker, maxWorkers),
		queue: make(chan interface{}, maxQueues),
		quit:  make(chan struct{}),
		sink:  make(chan map[string]int),
	}
	d.workers = make([]*worker, cap(d.pool))
	for i := 0; i < cap(d.pool); i++ {
		w := worker{
			dispatcher: d,
			data:       make(chan interface{}),
			quit:       make(chan struct{}),
		}
		d.workers[i] = &w
	}
	return d
}

// Add adds a given value to the queue of the dispatcher.
func (d *Dispatcher) Add(v interface{}) {
	d.wg.Add(1)
	d.queue <- v
}

// Start starts the specified dispatcher but does not wait for it to complete.
func (d *Dispatcher) Start() {
	for _, w := range d.workers {
		w.start()
	}
	go func() {
		for {
			select {
			case v := <-d.queue:
				(<-d.pool).data <- v

			case <-d.quit:
				return
			}
		}
	}()
}

// Wait waits for the dispatcher to exit. It must have been started by Start.
func (d *Dispatcher) Wait() {
	d.wg.Wait()
}

// Stop stops the dispatcher to execute. The dispatcher stops gracefully
// if the given boolean is false.
func (d *Dispatcher) Stop(immediately bool) {
	if !immediately {
		d.Wait()
	}

	d.quit <- struct{}{}
	for _, w := range d.workers {
		w.quit <- struct{}{}
	}
}

func (w *worker) start() {
	go func() {
		for {
			// register the current worker into the dispatch pool
			w.dispatcher.pool <- w
			select {
			case v := <-w.data:
				if bulkstr, ok := v.([][]string); ok {
					// fmt.Println(bulkstr)
					wordMap := processData(bulkstr)
					// fmt.Println(wordMap)
					w.dispatcher.sink <- wordMap
				}
				w.dispatcher.wg.Done()
			case <-w.quit:
				return
			}
		}
	}()
}

func processData(r [][]string) map[string]int {
	var m = map[string]int{}
	for _, rec := range r {
		m[rec[aggColNo]]++
	}
	// log.Printf("Goroutine cocurrency:%d  mapsize=%d", runtime.NumGoroutine(), len(m))
	return m
}

// チャネルからクラスタ化された集計結果を集める

func waitAndSum(sumMap map[string]int, d *Dispatcher, quit chan bool) map[string]int {
	for {
		select {
		case rec := <-d.sink:
			for mk, mv := range rec {
				sumMap[mk] += mv
			}
			// fmt.Println(sumMap)
		case <-quit:
			fmt.Println("quit")
			return sumMap
		default:
		}
	}
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
	d := NewDispatcher()
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

	sumMap := map[string]int{}
	quitChan := make(chan bool)
	d.Start()
	for {
		records := make([][]string, 0, bulkCount)
		isLast := false

		// バルク単位でCSVを読み込む
		for bi := 0; bi < bulkCount; bi++ {
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
		// バルクレコードをworkerへdispatch
		d.Add(records)
		if isLast {
			break
		}
	}
	// closer
	go func() {
		d.Wait()
		quitChan <- true
	}()
	resMap := waitAndSum(sumMap, d, quitChan)

	// 集計結果をarray化してソートする

	results := List{}
	for k, v := range resMap {
		e := Entry{k, v}
		results = append(results, e)
	}
	sort.Sort(results)

	for i := 0; i < 10 && i < len(results); i++ {
		fmt.Println(results[i])
	}

	resuleTime := fmt.Sprintf("\n%2fs", time.Since(start).Seconds())
	fmt.Println(resuleTime)
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
