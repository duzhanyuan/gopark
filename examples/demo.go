package main

import (
	"flag"
	"fmt"
	"github.com/xiexiao/gopark"
	"log"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"runtime"
	"time"
)

var count = gopark.NewFloat64Accumulator(0)
var random_once = gopark.NewLoopFunc("random_once", func(_ interface{}) {
	x := rand.Float32()
	y := rand.Float32()
	if x*x+y*y < 1 {
		count.Add(float64(1))
	}
})

func init() {
	gopark.RegisterFunc(random_once)
	gopark.RegisterFunc(map_func)
	gopark.RegisterFunc(reducer_func)
}

var map_func = gopark.NewMapperFunc("map_func",
	func(_ interface{}) interface{} {
		x := rand.Float32()
		y := rand.Float32()
		if x*x+y*y < 1 {
			return 1
		} else {
			return 0
		}
	})

var reducer_func = gopark.NewReducerFunc("reducer_func",
	func(x, y interface{}) interface{} {
		return x.(int) + y.(int)
	})

func main() {
	go func() {
		var m runtime.MemStats
		for {
			runtime.ReadMemStats(&m)
			fmt.Printf("MemStats: %d,%d,%d,%d\n", m.HeapSys, m.HeapAlloc,
				m.HeapIdle, m.HeapReleased)
			time.Sleep(1 * time.Second)
		}
	}()

	go func() {
		log.Println(http.ListenAndServe("0.0.0.0:6060", nil))
	}()
	CountPI2()
}

func CountPI2() {
	flag.Parse()
	N := 5000
	ctx := gopark.NewContext("gopark")
	defer ctx.Stop()

	data := make([]interface{}, N)

	iters := ctx.ParallelizeN(data, 10)
	m := iters.Map(map_func)

	count := m.Reduce(reducer_func).(int)
	fmt.Println(count)
	fmt.Printf("Pi = %f \n", (4.0 * float64(count) / float64(N)))
}

func CountPI1() {
	flag.Parse()
	ctx := gopark.NewContext("gopark")
	defer ctx.Stop()
	N := 10000
	data := make([]interface{}, N)
	iters := ctx.ParallelizeN(data, 20)

	iters.Foreach(random_once)
	val := count.Value().(float64)
	fmt.Println(val)
	fmt.Printf("Pi = %f \n", (4.0 * val / float64(N)))
}
