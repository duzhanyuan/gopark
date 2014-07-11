package gopark

import (
	"flag"
	"fmt"
	"github.com/golang/glog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

var _ = fmt.Println

type Context struct {
	name        string
	master      string
	initialized bool

	started   bool
	startTime time.Time

	defaultParallelism int
	defaultMinSplits   int

	scheduler Scheduler
	isLocal   bool
	options   *Options
}

func NewContext(name string) *Context {
	flag.Parse()
	if _opts.process_child != "" {
		addr := _opts.process_child
		do_process_job(addr)
		os.Exit(0)
	} else if _opts.mesos_executor != "" {
		addr := _opts.mesos_executor
		do_executor_job(addr)
		os.Exit(0)
	}
	ctx := &Context{
		name:               name,
		defaultParallelism: 2,
		initialized:        false,
		started:            false,
	}
	return ctx
}

func (ctx *Context) init() {
	if ctx.initialized {
		return
	}

	//init env
	ctx.options = _opts
	_env.conf = SetupConf(_opts)

	master := ctx.master
	if master == "" {
		master = _opts.master
	}

	if master == "local" {
		ctx.scheduler = newLocalScheduler(_opts.parallel)
		ctx.isLocal = true
	} else if master == "process" {
		ctx.scheduler = newMultiProcessScheduler(_opts.parallel)
		ctx.isLocal = false
	} else {
		if master == "mesos" {
			master = _env.conf.MESOS_MASTER
		}

		if strings.Index(master, "mesos://") == 0 {
			idx := strings.Index(master, "@")
			if idx > -1 {
				master = master[idx+1:]
			} else {
				idx2 := strings.Index(master, "//") + 2
				master = master[idx2:]
			}
		} else if strings.Index(master, "zoo://") == 0 {
			master = "zk" + master[3:]
		}

		if strings.IndexByte(master, ':') == -1 {
			master += ":5050"
		}
		ctx.scheduler = newMesosScheduler(master, _opts)
		ctx.isLocal = false
	}
	if _opts.parallel > 0 {
		ctx.defaultParallelism = _opts.parallel
	} else {
		ctx.defaultParallelism = ctx.scheduler.defaultParallelism()
	}
	ctx.defaultMinSplits = MaxInt(ctx.defaultParallelism, 2)

	ctx.master = master

	ctx.initialized = true
}

func (ctx *Context) start() {
	if ctx.started {
		return
	}
	ctx.init()

	_env.start(true, nil, ctx.isLocal)
	ctx.scheduler.start()

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGABRT)
	go func() {
		s := <-signalChan
		glog.Errorf("got the signal %v, exit now.", s)
		glog.Flush()
		ctx.Stop()
		os.Exit(2)
	}()
	ctx.started = true
	ctx.startTime = time.Now()
	glog.Infof("context-%s is started.", ctx.name)
}

func (ctx *Context) Broadcast(v interface{}) Broadcaster {
	ctx.start()
	return newBroadcast(v)
}

func (ctx *Context) Parallelize(seq []interface{}) RDD {
	return ctx.ParallelizeN(seq, 0)
}

func (ctx *Context) ParallelizeN(seq []interface{}, numSlices int) RDD {
	ctx.init()
	if numSlices <= 0 {
		numSlices = ctx.defaultParallelism
	}
	return newParallelRDD(ctx, seq, numSlices)
}

func (ctx *Context) create_rdd(p string) RDD {
	if strings.HasSuffix(p, ".bz2") {
		//TODO:gz
	} else if strings.HasSuffix(p, ".gz") {
		//TODO:gz
	}
	return newTextFileRDD(ctx, p, _env.parallel)
}

func (ctx *Context) TextFile(pathname string) RDD {
	return ctx.TextFileExt(pathname, "")
}
func (ctx *Context) TextFileExt(pathname string, ext string) RDD {
	return ctx.TextFileM(pathname, ext, true, 0)
}

func (ctx *Context) TextFileM(pathname string, ext string, followLink bool,
	maxDepth int) RDD {
	//TODO: maxDepth
	absPathname, err := filepath.Abs(pathname)
	if err != nil {
		glog.Fatal(err)
		return nil
	}
	if fStat, err := os.Stat(absPathname); err != nil {
		glog.Fatal(err)
		return nil
	} else {
		if !fStat.IsDir() {
			return ctx.create_rdd(absPathname)
		} else {
			pathNames := make([]string, 0)
			err = filepath.Walk(absPathname, func(path string, info os.FileInfo, err error) error {
				if !info.IsDir() {
					if ext != "" {
						if strings.HasSuffix(path, ext) && !strings.HasPrefix(path, ".") {
							pathNames = append(pathNames, path)
						}
					} else {
						pathNames = append(pathNames, path)
					}
				}
				return nil
			})
			if err != nil {
				glog.Fatal(err)
			}
			rdds := make([]RDD, len(pathNames))
			for i := range pathNames {
				rdds[i] = ctx.create_rdd(pathNames[i])
			}
			return ctx.Union(rdds)
		}
	}
}

func (c *Context) Union(rdds []RDD) RDD {
	return newUnionRDD(c, rdds)
}

func (ctx *Context) runJob(done <-chan bool, rdd RDD, rn *JobFunc,
	additions []interface{},
	partitions []int, allowLocal bool) <-chan interface{} {
	var parts []int
	if partitions == nil {
		parts = make([]int, rdd.len())
		for i := 0; i < rdd.len(); i++ {
			parts[i] = i
		}
	} else {
		parts = partitions
	}
	if len(parts) == 0 {
		return nil
	}
	ctx.start()
	return ctx.scheduler.runJob(done, rdd, rn, additions, parts, allowLocal)
}

func (ctx *Context) Clear() {
	if !ctx.started {
		return
	}
	ctx.scheduler.clear()
}

func (ctx *Context) Stop() {
	if !ctx.started {
		return
	}
	_env.stop()
	ctx.scheduler.stop()
	ctx.started = false
	glog.Infof("context is stopped, duration = %s.", (time.Since(ctx.startTime)))
	glog.Flush()
}

/*
func (ctx *Context) Table(dpath string) RDD {
    absPathname, err := filepath.Abs(dpath)
    if err != nil {
        glog.Fatal(err)
        return nil
    }
    if fStat, err := os.Stat(absPathname); err != nil {
        glog.Fatal(err)
        return nil
    } else {
        if fStat.IsDir() {
            var fields []string
            pathNames := make([]string, 0)
            err = filepath.Walk(absPathname, func(p string, info os.FileInfo, err error) error {
                if !info.IsDir() && info.Name() == ".field_names" {
                    p = path.Join(absPathname, ".field_names")
                    if line, err := ioutil.ReadFile(p); err != nil {
                        fields = strings.Split(line, "\t")
                    } else {
                        glog.Fatal(err)
                    }
                }
                return nil
            })
            if err != nil {
            }
            return ctx.tableFile(absPathname).asTable(fields)
        }
    }
    glog.Fatalf("no .field_names found in %s", dpath)
    return nil
}
*/
