GoPark
==========

GoPark is a Go version of [Dpark][2], MapReduce(R) alike computing framework supporting iterative computation. 

It took some code from old version [GoPark][3] by Jia Mi.

After read the code of Dpark and old version [GoPark][3], we got this code. 
You need a Mesos lib install when build the code under Linux. When under windows, it will not build the mesos mode.

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
- [Spark][1] RDD, Cluster
- [DPark][2], porting most of the code
- [GoPark][3], old version GoPark
- [mesos-go][4], Go language bindings for Apache Mesos 

[1]: http://spark.apache.org/
[2]: https://github.com/douban/dpark
[3]: https://github.com/mijia/gopark
[4]: https://github.com/mesosphere/mesos-go

