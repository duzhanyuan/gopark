package gopark

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/golang/glog"
	"github.com/xiexiao/gopark/moosefs"
	"os"
	"path"
)

func newConf() *Configuration {
	conf := &Configuration{
		//GOPARK_WORK_DIR:      "/tmp/gopark", // workdir used in slaves for internal files
		MESOS_MASTER:         "localhost",
		MOOSEFS_MOUNT_POINTS: make(map[string]string),
		MOOSEFS_DIR_CACHE:    false,
	}
	conf.GOPARK_WORK_DIR = path.Join(os.TempDir(), "gopark")
	if _, ok := ExistPath("/dev/shm"); ok {
		conf.GOPARK_WORK_DIR = "/dev/shm,/tmp/gopark"
	}
	return conf
}

type Configuration struct {
	GOPARK_WORK_DIR      string
	MESOS_MASTER         string
	MOOSEFS_MOUNT_POINTS map[string]string
	MOOSEFS_DIR_CACHE    bool
}

//Load Configuration

func SetupConf(opts *Options) *Configuration {
	conf := newConf()
	if opts.conf != "" {
		loadConf(conf, opts.conf)
	} else if true {
	} else if true {
		loadConf(conf, "")
	}
	moosefs.MFS_PREFIX = conf.MOOSEFS_MOUNT_POINTS
	moosefs.ENABLE_DCACHE = conf.MOOSEFS_DIR_CACHE
	return conf
}

// load the conf file in json format
func loadConf(conf *Configuration, path string) error {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		msg := fmt.Sprintf("conf %s do not exists", path)
		glog.Error(msg)
		return errors.New(msg)
	}
	if file, err := os.Open(path); err == nil {
		decoder := json.NewDecoder(file)
		c := Configuration{}
		err := decoder.Decode(&c)
		if err != nil {
			glog.Error(err)
			return err
		}
		if c.GOPARK_WORK_DIR != "" {
			conf.GOPARK_WORK_DIR = c.GOPARK_WORK_DIR
		}
		if c.MESOS_MASTER != "" {
			conf.MESOS_MASTER = c.MESOS_MASTER
		}
		conf.MOOSEFS_MOUNT_POINTS = c.MOOSEFS_MOUNT_POINTS
		conf.MOOSEFS_DIR_CACHE = c.MOOSEFS_DIR_CACHE
	} else {
		return err
	}
	return nil
}

type Options struct {
	master       string
	parallel     int
	cpus         float64
	mem          float64
	group        string
	err          float64
	snapshot_dir string
	conf         string

	process_child  string
	mesos_executor string
}
