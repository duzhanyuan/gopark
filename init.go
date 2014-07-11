package gopark

import (
	"encoding/gob"
	"flag"
)

var _opts *Options
var _env *GoparkEnv = NewGoparkEnv()

func init() {
	_opts = &Options{}
	flag.StringVar(&_opts.master, "master", "local", "master of Mesos: local, host[:port], or mesos://")
	flag.IntVar(&_opts.parallel, "p", 0, "number of processes")
	flag.Float64Var(&_opts.cpus, "c", 1.0, "cpus used per task")
	flag.Float64Var(&_opts.mem, "M", 128.0, "memory used per task")
	flag.StringVar(&_opts.group, "g", "", "which group of machine")

	flag.Float64Var(&_opts.err, "err", 0.0, "acceptable ignored error record ratio (0.01%)")
	flag.StringVar(&_opts.snapshot_dir, "snapshot_dir", "", "shared dir to keep snapshot of RDDs")
	flag.StringVar(&_opts.conf, "conf", "", "path for configuration file")

	flag.StringVar(&_opts.process_child, "process_child", "", "not use by human.")
	flag.StringVar(&_opts.mesos_executor, "mesos_executor", "", "not use by human.")
}

func init() {
	gob.Register(make([]interface{}, 0))
}
