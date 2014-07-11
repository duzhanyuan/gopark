package gopark

import (
	"fmt"
	"github.com/golang/glog"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"
)

type GoparkEnv struct {
	isMaster bool
	isLocal  bool
	started  bool
	environ  map[string]interface{}
	workdir  []string
	parallel int

	conf *Configuration

	trackerClient *_TrackerClient
	trackerServer *TrackerServer

	cacheTracker _BaseCacheTracker

	shuffleFetcher   ShuffleFetcher
	mapOutputTracker BaseMapOutputTracker
}

func NewGoparkEnv() *GoparkEnv {
	return &GoparkEnv{
		started:  false,
		parallel: 2,
		environ:  make(map[string]interface{}),
	}
}

func (env *GoparkEnv) Register(name string, value interface{}) {
	env.environ[name] = value
}

func (env *GoparkEnv) Get(name string, defaultVal interface{}) interface{} {
	if val, ok := env.environ[name]; ok {
		return val
	}
	return defaultVal
}

func (env *GoparkEnv) start(isMaster bool, environ map[string]interface{}, isLocal bool) {
	if env.started {
		return
	}
	glog.Infof("start env in %d: %t %s", os.Getpid(), isMaster, environ)
	if environ != nil {
		for k, v := range environ {
			env.environ[k] = v
		}
	}
	env.isMaster = isMaster
	env.isLocal = isLocal
	var roots []string
	var addr string
	if isMaster {
		root := env.conf.GOPARK_WORK_DIR
		if strings.Index(root, ",") > -1 {
			roots = strings.Split(root, ",")
		} else {
			roots = []string{root}
		}

		if isLocal {
			rootpath := roots[0]
			if _, err := os.Stat(rootpath); os.IsNotExist(err) {
				os.MkdirAll(rootpath, 0777)
			}
		}
		hostname, _ := os.Hostname()
		name := fmt.Sprintf("%s-%s-%d", time.Now().Format("20060102-150405"),
			hostname, os.Getpid())
		var workdirs = make([]string, len(roots))
		for idx, dir := range roots {
			r := path.Join(dir, name)
			if _, err := os.Stat(r); os.IsNotExist(err) {
				os.MkdirAll(r, 0777)
			}
			workdirs[idx] = filepath.ToSlash(r)
		}
		env.environ["WORKDIR"] = workdirs
		env.trackerServer = NewTrackerServer()
		env.trackerServer.Start()
		addr = env.trackerServer.addr
		env.Register("TrackerAddr", addr)
	} else {
		addr = env.Get("TrackerAddr", "").(string)
	}
	env.trackerClient = newTrackerClient(addr)

	if isLocal {
		env.cacheTracker = newLocalCacheTracker()
	} else {
		env.cacheTracker = newCacheTracker()
	}

	LocalFileShuffle.initialize(isMaster)
	if isLocal {
		env.mapOutputTracker = NewLocalMapOutputTracker()
	} else {
		env.mapOutputTracker = NewMapOutputTracker()
	}
	env.shuffleFetcher = NewParallelShuffleFetcher(2)

	StartBroadcastManager(isMaster)

	env.started = true
	glog.Info("env started")
}

func (env *GoparkEnv) stop() {
	if !env.started {
		return
	}
	glog.Infof("stop env in %d", os.Getpid())
	env.shuffleFetcher.Stop()
	env.cacheTracker.clear()
	env.mapOutputTracker.Stop()
	if env.isMaster {
		env.trackerServer.Stop()
	}
	StopBroadcastManager()

	glog.Infof("cleaning workdir ...")
	if env.workdir != nil {
		for _, dir := range env.workdir {
			if err := os.RemoveAll(dir); err != nil {
				glog.Errorf("cleaning workdir %s got error: %s", dir, err)
			}
		}
	}
	glog.Infoln("done.")
	env.started = false
}
