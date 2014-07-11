GoPark
==========

GoPark is a Go version of [Dpark](https://github.com/douban/dpark), MapReduce(R) alike computing framework supporting iterative computation. 

It took some code from old version [GoPark](https://github.com/mijia/gopark) by Jia Mi.

After read the code of Dpark and old version [GoPark], we got this code.

It just a toy code by now, the source code of it may be changed soon after.

Just read the code and have fun.

#### Change from old version:
- moosefs support code add
- Mesos support with bugs
- the code of yield changed
- run task with the child process
- distribute functions by executable file distribute

#### Examples
Computing PI:
```
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
}

func main() {
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
```

#### gopark.RegisterFunc
When run GoPark on process or mesos mode, we need to distribute the whole executable file to child node. 
When task run, it can meet the functions.

Dpark can serialize functions and distribute it to the child node. 
Go can't serialize functions, so we need to do register:
```
func init() {
	gopark.RegisterFunc(random_once)
}
```
. When child task run, it can get the function by its name.

#### TODO
- bug: when task number is too much, run on Mesos will zombie.
- add more RDD function implemented, TODO in code, code refactor
- read the cluster code of Spark

#### Acknowledgements
- [Spark](http://spark.apache.org/) RDD, Cluster
- [DPark](https://github.com/douban/dpark), porting most of the code
- [GoPark](https://github.com/mijia/gopark), old version GoPark

