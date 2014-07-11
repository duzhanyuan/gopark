package gopark

import (
	"math/rand"
	"strings"
	"testing"
)

func TestParallelizeN(t *testing.T) {
	N := 31
	ctx := NewContext("gopark")

	data := make([]interface{}, N)
	iters := ctx.ParallelizeN(data, 20)
	if iters == nil {
		t.Fatal("iters is nil")
	} else {
		count := 0
		random_once := NewLoopFunc("foreach", func(_ interface{}) {
			x := rand.Float32()
			y := rand.Float32()
			if x*x+y*y < 1 {
				//fmt.Println(1)
				count += 1
			}
		})
		iters.Foreach(random_once)
		t.Logf("Pi = %f\n", (4.0 * float64(count) / float64(N)))
	}
	ctx.Stop()
}

func TestWordCount(t *testing.T) {
	ctx := NewContext("gopark")
	file := ctx.TextFile("rdd.go")
	fn := NewFlatMapperFunc("fn", func(x interface{}) []interface{} {
		vals := strings.Fields(x.(string))
		results := make([]interface{}, len(vals))
		for idx, v := range vals {
			results[idx] = v
		}
		return results
	})
	fnMap := NewMapperFunc("fnMap", func(x interface{}) interface{} {
		return &KeyValue{x, 1}
	})

	fnReduce := NewReducerFunc("fnReduce", func(x, y interface{}) interface{} {
		return x.(int) + y.(int)
	})
	words := file.FlatMap(fn).Map(fnMap)

	wc := words.ReduceByKey(fnReduce).CollectAsMap()
	if len(wc) <= 0 {
		t.Fatal("iters is nil")
	} else {
		t.Logf("%#v\n", wc)
	}
	ctx.Stop()
}

func TestMap(t *testing.T) {
	N := 31
	ctx := NewContext("gopark")
	defer ctx.Stop()

	data := make([]interface{}, N)

	iters := ctx.ParallelizeN(data, 20)
	if iters == nil {
		t.Fatal("iters is nil")
	} else {
		map_func := NewMapperFunc("map_func", func(_ interface{}) interface{} {
			x := rand.Float32()
			y := rand.Float32()
			if x*x+y*y < 1 {
				return 1
			} else {
				return 0
			}
		})
		m := iters.Map(map_func)
		if m == nil {
			t.Fatal("map result is nil")
		} else {
			reduce_func := NewReducerFunc("reduce_func", func(x, y interface{}) interface{} {
				return x.(int) + y.(int)
			})
			count := m.Reduce(reduce_func).(int)
			if count > 0 {
				t.Logf("Pi = %f\n", (4.0 * float64(count) / float64(N)))
			} else {
				t.Fatal("count must >0 ")
			}
		}
	}
}

// TestWordCount_Process

var (
	fn = NewFlatMapperFunc("fn", func(x interface{}) []interface{} {
		vals := strings.Fields(x.(string))
		results := make([]interface{}, len(vals))
		for idx, v := range vals {
			results[idx] = v
		}
		return results
	})
	fnMap = NewMapperFunc("fnMap", func(x interface{}) interface{} {
		return &KeyValue{x, 1}
	})

	fnReduce = NewReducerFunc("fnReduce", func(x, y interface{}) interface{} {
		return x.(int) + y.(int)
	})
)

func init() {
	RegisterFunc(fn)
	RegisterFunc(fnMap)
	RegisterFunc(fnReduce)
}

func TestWordCount_Process(t *testing.T) {
	_opts.master = "process"
	ctx := NewContext("gopark")
	file := ctx.TextFile("rdd_test.go")
	words := file.FlatMap(fn).Map(fnMap)

	wc := words.ReduceByKey(fnReduce).CollectAsMap()
	if len(wc) <= 0 {
		t.Fatal("wc is nil")
	} else {
		t.Logf("%#v\n", wc)
	}
	ctx.Stop()
}
