package gopark

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"fmt"
	"hash/fnv"
	"os"
)

func MinFloat64s(vals []float64) float64 {
	//TODO: use mergesort
	if len(vals) > 1 {
		result := vals[0]
		for idx, v := range vals {
			if idx > 0 {
				if result > v {
					result = v
				}
			}
		}
		return result
	} else if len(vals) == 1 {
		return vals[0]
	} else {
		return 0
	}
}

func MaxFloat64s(vals []float64) float64 {
	if len(vals) > 1 {
		result := vals[0]
		for idx, v := range vals {
			if idx > 0 {
				if result < v {
					result = v
				}
			}
		}
		return result
	} else if len(vals) == 1 {
		return vals[0]
	} else {
		return 0
	}
}

func MaxUint64s(vals []uint64) uint64 {
	if len(vals) > 1 {
		result := vals[0]
		for idx, v := range vals {
			if idx > 0 {
				if result < v {
					result = v
				}
			}
		}
		return result
	} else if len(vals) == 1 {
		return vals[0]
	} else {
		return 0
	}
}

func MaxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func MinUint64(a, b uint64) uint64 {
	if a > b {
		return b
	}
	return a
}

func MaxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func MinInt64(a, b int64) int64 {
	if a > b {
		return b
	}
	return a
}

func MaxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func MinInt(a, b int) int {
	if a > b {
		return b
	}
	return a
}

func readLine(r *bufio.Reader) (string, error) {
	var (
		isPrefix bool  = true
		err      error = nil
		line, ln []byte
	)
	for isPrefix && err == nil {
		line, isPrefix, err = r.ReadLine()
		ln = append(ln, line...)
	}
	return string(ln), err
}

var _ = fmt.Println

func seekToNewLine(f gpFile, start int64) (int64, error) {
	_, err := f.Seek(start, 0)
	if err != nil {
		return start, err
	}
	b := make([]byte, 1)
	_, err = f.Read(b)
	start++
	if err != nil {
		return start, err
	}
	for b[0] != '\n' {
		_, err := f.Read(b)
		if err != nil {
			return start, err
		}
		start++
	}
	return start, nil
}

func HashCode(value interface{}) int64 {
	if value == nil {
		return 0
	}
	hash := fnv.New32()
	buffer := new(bytes.Buffer)
	encoder := gob.NewEncoder(buffer)
	encoder.Encode(value)
	hash.Write(buffer.Bytes())
	var code int64 = 0
	hashBytes := hash.Sum(nil)
	for _, hashByte := range hashBytes {
		code = code*256 + int64(hashByte)
	}
	return code
}

func ExistPath(filename string) (os.FileInfo, bool) {
	info, err := os.Stat(filename)
	return info, err == nil || os.IsExist(err)
}

func _merge(l, r []interface{}, lessFunc func(interface{}, interface{}) bool, reverse bool) []interface{} {
	ret := make([]interface{}, 0, len(l)+len(r))
	for len(l) > 0 || len(r) > 0 {
		if len(l) == 0 {
			return append(ret, r...)
		}
		if len(r) == 0 {
			return append(ret, l...)
		}
		less := lessFunc(l[0], r[0])
		if reverse {
			less = !less
		}
		if less {
			ret = append(ret, r[0])
			r = r[1:]
		} else {
			ret = append(ret, l[0])
			l = l[1:]
		}
	}
	return ret
}

func mergeSort(s []interface{}, lessFunc func(interface{}, interface{}) bool, reverse bool) []interface{} {
	if len(s) <= 1 {
		return s
	}
	n := len(s) / 2
	l := mergeSort(s[:n], lessFunc, reverse)
	r := mergeSort(s[n:], lessFunc, reverse)
	return _merge(l, r, lessFunc, reverse)
}

func long2ip(ip uint32) string {
	return fmt.Sprintf("%d.%d.%d.%d", ip>>24, ip<<8>>24, ip<<16>>24, ip<<24>>24)
}
