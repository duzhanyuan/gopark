package gopark

import (
	"testing"
)

func TestMaxUint64(t *testing.T) {
	r1 := MaxUint64(1, 2)
	if r1 != 2 {
		t.Fatal("error")
	}
	r2 := MaxUint64(1999, 2)
	if r2 != 1999 {
		t.Fatal("error")
	}
}

func TestMinUint64(t *testing.T) {
	r1 := MinUint64(1, 2)
	if r1 != 1 {
		t.Fatal("error")
	}
	r2 := MinUint64(1999, 2)
	if r2 != 2 {
		t.Fatal("error")
	}
}

func TestMaxInt64(t *testing.T) {
	r1 := MaxInt64(1, 2)
	if r1 != 2 {
		t.Fatal("error")
	}
	r2 := MaxInt64(1999, 2)
	if r2 != 1999 {
		t.Fatal("error")
	}
}

func TestMaxInt(t *testing.T) {
	r1 := MaxInt(1, 2)
	if r1 != 2 {
		t.Fatal("error")
	}
	r2 := MaxInt(1999, 2)
	if r2 != 1999 {
		t.Fatal("error")
	}
}

func TestMinInt64(t *testing.T) {
	r1 := MinInt64(1, 2)
	if r1 != 1 {
		t.Fatal("error")
	}
	r2 := MinInt64(1999, 2)
	if r2 != 2 {
		t.Fatal("error")
	}
}

func TestMinInt(t *testing.T) {
	r1 := MinInt(1, 2)
	if r1 != 1 {
		t.Fatal("error")
	}
	r2 := MinInt(1999, 2)
	if r2 != 2 {
		t.Fatal("error")
	}
}

func TestMergeSort(t *testing.T) {
	abc := []interface{}{4, 6, 1, 2, 19, 123, 8, 23, 1990}
	fn := func(x interface{}, y interface{}) bool {
		return x.(int) < y.(int)
	}
	abc = mergeSort(abc, fn, false)
	if abc[0].(int) != 1990 {
		t.Log(abc)
		t.Fatal("error")
	}
	abc = mergeSort(abc, fn, true)
	if abc[0].(int) != 1 {
		t.Log(abc)
		t.Fatal("error")
	}
}
