# CSV Aggregation with Goroutine


## Basic version


```bash
cd basic
go run aggcsv.go -f ..\test.csv
go run aggcsv.go -f ..\test_300000.csv

```

## Dispacher version
```bash
cd dispathcer
go run aggcsv_d.go -f ..\test.csv
go run aggcsv_D.go -f ..\test_300000.csv

```

## Reference

[golang の channel を使って Dispatcher-Worker を作り goroutine 爆発させないようにする](http://blog.kaneshin.co/entry/2016/08/18/190435)

[Golangで大きなcsvのインポートを速くする](http://blog.yudppp.com/posts/csv_fast_upload/)


