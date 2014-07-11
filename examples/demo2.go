package main

import (
	"flag"
	"fmt"
	"github.com/xiexiao/gopark"
	"math/rand"
	"strings"
)

var count = gopark.NewFloat64Accumulator(0)
var random_once = gopark.NewLoopFunc("random_once", func(_ interface{}) {
	x := rand.Float32()
	y := rand.Float32()
	if x*x+y*y < 1 {
		count.Add(float64(1))
	}
})

func testPi() {
	flag.Parse()
	ctx := gopark.NewContext("gopark")
	defer ctx.Stop()
	N := 100
	data := make([]interface{}, N)
	iters := ctx.ParallelizeN(data, 20)
	iters.Foreach(random_once)
	fmt.Println("Pi =", (4.0 * count.Value().(float64) / float64(N)))
}

func main() {
	testTextFile()
}

var flatmap_func = gopark.NewFlatMapperFunc("flatmap_func",
	func(line interface{}) []interface{} {
		vs := strings.Fields(line.(string))
		set := make(map[string]bool)
		for _, v := range vs {
			if _, ok := set[v]; !ok {
				set[v] = true
			}
		}
		words := make([]interface{}, len(set))
		idx := 0
		for k, _ := range set {
			words[idx] = k
			idx += 1
		}
		return words
	})
var reducer_func = gopark.NewReducerFunc("reducer_func",
	func(x, y interface{}) interface{} {
		return x.(int) + y.(int)
	})

func init() {
	gopark.RegisterFunc(random_once)
	gopark.RegisterFunc(map_func)
	gopark.RegisterFunc(flatmap_func)
	gopark.RegisterFunc(reducer_func)
	gopark.RegisterFunc(filter_func)
	gopark.RegisterFunc(less_func)
}

var map_func = gopark.NewMapperFunc("map_func",
	func(x interface{}) interface{} {
		return &gopark.KeyValue{x, 1}
	})

var filter_func = gopark.NewFilterFunc("filter_func",
	func(x interface{}) bool {
		result := x.(*gopark.KeyValue).Value.(int) > 30
		return result
	})
var less_func = gopark.NewLessFunc("less_func",
	func(x, y interface{}) bool {
		a := x.(*gopark.KeyValue).Value.(int)
		b := y.(*gopark.KeyValue).Value.(int)
		return a < b
	})

func testTextFile() {
	flag.Parse()
	ctx := gopark.NewContext("gopark")
	defer ctx.Stop()
	p := "."
	txt := ctx.TextFileExt(p, "")
	fm := txt.FlatMap(flatmap_func)
	fm = fm.Map(map_func)
	fm = fm.ReduceByKey(reducer_func)
	counts := fm

	ff := counts.Filter(filter_func)

	mm := ff.CollectAsMap()
	fmt.Printf("Word count > 30:%#v\n", mm)

	fmt.Println("TopN=-10\n==========")
	t1 := counts.TopN(-10, less_func)
	for k, v := range t1 {
		fmt.Printf("k, v: %#v %#v\n", k, v)
	}

	fmt.Println("Top10\n==========")
	t2 := counts.Top10(less_func)
	for k, v := range t2 {
		fmt.Printf("k, v: %#v %#v\n", k, v)
	}

}
