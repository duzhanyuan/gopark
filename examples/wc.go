package main

import (
	"fmt"
	"github.com/xiexiao/gopark"
	"strings"
)

var (
	fn = gopark.NewFlatMapperFunc("fn", func(x interface{}) []interface{} {
		vals := strings.Fields(x.(string))
		results := make([]interface{}, len(vals))
		for idx, v := range vals {
			results[idx] = v
		}
		return results
	})
	fnMap = gopark.NewMapperFunc("fnMap", func(x interface{}) interface{} {
		return &gopark.KeyValue{x, 1}
	})

	fnReduce = gopark.NewReducerFunc("fnReduce", func(x, y interface{}) interface{} {
		return x.(int) + y.(int)
	})
)

func init() {
	gopark.RegisterFunc(fn)
	gopark.RegisterFunc(fnMap)
	gopark.RegisterFunc(fnReduce)
}
func WC() {
	ctx := gopark.NewContext("gopark")
	defer ctx.Stop()
	file := ctx.TextFile("wc.go")
	words := file.FlatMap(fn).Map(fnMap)

	wc := words.ReduceByKey(fnReduce).CollectAsMap()
	if len(wc) <= 0 {
		panic("wc is nil")
	} else {
		fmt.Printf("=====>>%d  func count %d\n", len(wc), wc[interface{}("func")])
		for k, v := range wc {
			fmt.Printf("k:%#v, v:%#v\n", k, v)
		}
	}
}

func main() {
	//WC()
	//Top10()
	//textSearch()
	saveTextFile()
}

var fnMap_line = gopark.NewMapperFunc("fnMap_line", func(x interface{}) interface{} {
	return strings.TrimSpace(x.(string))
})

var filter_fn = gopark.NewFilterFunc("filter_fn", func(x interface{}) bool {
	return strings.Contains(x.(string), "glog")
})

var filter_fatal = gopark.NewFilterFunc("filter_fatal", func(x interface{}) bool {
	return strings.Contains(x.(string), "Fatal")
})

var filter_info = gopark.NewFilterFunc("filter_info", func(x interface{}) bool {
	return strings.Contains(x.(string), "Info")
})

func init() {
	gopark.RegisterFunc(fnMap_line)
	gopark.RegisterFunc(filter_fn)
	gopark.RegisterFunc(filter_fatal)
	gopark.RegisterFunc(filter_info)
}

func textSearch() {
	ctx := gopark.NewContext("gopark")
	defer ctx.Stop()

	textfile := ctx.TextFileExt("./", ".go")
	f := textfile.Map(fnMap_line)
	log := f.Filter(filter_fn).Cache()
	results := log.Collect()
	fmt.Printf("results.: %#v\n", results)
	fmt.Printf("glog.: %d\n", log.Count())
	fmt.Printf("fatal: %d\n", log.Filter(filter_fatal).Count())
	for _, line := range log.Filter(filter_info).Collect() {
		fmt.Println(line)
	}

}

var fnLess = gopark.NewLessFunc("fnLess",
	func(x, y interface{}) bool {
		a := x.(*gopark.KeyValue).Value.(int)
		b := y.(*gopark.KeyValue).Value.(int)
		return a < b
	})

func init() {
	gopark.RegisterFunc(fnLess)
}

func Top10() {
	ctx := gopark.NewContext("gopark")
	defer ctx.Stop()
	file := ctx.TextFile("wc.go")
	words := file.FlatMap(fn).Map(fnMap)

	fm := words.ReduceByKey(fnReduce)

	fmt.Println("Top10\n==========")
	t2 := fm.Top10(fnLess)
	for k, v := range t2 {
		fmt.Printf("k, v: %#v %#v\n", k, v)
	}

}

var map_string = gopark.NewMapperFunc("map_string",
	func(x interface{}) interface{} {
		return fmt.Sprintf("%#v", x)
	})

func init() {
	gopark.RegisterFunc(map_string)
}

func saveTextFile() {
	ctx := gopark.NewContext("gopark")
	defer ctx.Stop()
	file := ctx.TextFile("wc.go")
	words := file.FlatMap(fn).Map(fnMap)

	fm := words.ReduceByKey(fnReduce)

	tt := fm.Map(map_string)
	abc := tt.SaveAsTextFileM("abc/", "", true, true)
	fmt.Printf("%#v\n", abc)
}
