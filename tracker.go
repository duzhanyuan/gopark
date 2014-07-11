package gopark

import (
	"errors"
	"fmt"
	"github.com/golang/glog"
	"net"
	"net/rpc"
	"os"
	"strings"
	"time"
)

type TrackerServer struct {
	addr string
	locs map[string][]string
	_lis *net.TCPListener
}

type ActionItem struct {
	Action string //get, add, set, remove, stop
	Value  []string
	Key    string
}

type ReplyItem struct {
	Result string //OK | ERROR
	Value  []string
}

type RPC_Tracker struct {
	locs   map[string][]string
	server *TrackerServer
}

func NewRPC_Tracker(locs map[string][]string, server *TrackerServer) *RPC_Tracker {
	return &RPC_Tracker{
		locs:   locs,
		server: server,
	}
}

func (r *RPC_Tracker) Send(item *ActionItem, reply *ReplyItem) error {
	action := strings.ToLower(item.Action)
	var result = "OK"
	switch action {
	case "stop":
		r.server.Stop()
	case "get":
		if val, ok := r.locs[item.Key]; ok {
			reply.Value = val
		} else {
			reply.Value = nil
			result = "ERROR"
		}
	case "add":
		if len(item.Value) > 0 {
			vals := r.locs[item.Key]
			for _, v := range item.Value {
				vals = append(vals, v)
			}
			r.locs[item.Key] = vals
		}
	case "set":
		r.locs[item.Key] = item.Value
	case "remove":
		delete(r.locs, item.Key)
	case "remove_item":
		vals := r.locs[item.Key]
		results := make([]string, 0)
		for _, v := range item.Value {
			for _, val := range vals {
				if val != v {
					results = append(results, val)
				}
			}
		}
		r.locs[item.Key] = results
	default:
		//TODO:log
		result = "ERROR"
	}
	reply.Result = result
	return nil
}

func NewTrackerServer() *TrackerServer {
	return &TrackerServer{
		locs: make(map[string][]string),
	}
}

func (s *TrackerServer) Addr() string {
	return s.addr
}

func (s *TrackerServer) Start() {
	go s.run()
	for {
		if s.addr == "" {
			time.Sleep(10 * time.Millisecond)
		} else {
			break
		}
	}
}

func (s *TrackerServer) Stop() {
	if s._lis != nil {
		s._lis.Close()
	}
	glog.Infof("stop TrackerServer tcp://%s", s.addr)
}

func AcceptEx(lis net.Listener) {
	for {
		if conn, err := lis.Accept(); err != nil {
			//TODO:fix the error
		} else {
			go rpc.DefaultServer.ServeConn(conn)
		}
	}
}

func (s *TrackerServer) run() {
	r := NewRPC_Tracker(s.locs, s)
	rpc.Register(r)

	var (
		hostname string
		port     int
		err      error
	)
	if hostname, err = os.Hostname(); err == nil {
		if tcpAddr, err := net.ResolveTCPAddr("tcp", "0.0.0.0:0"); err == nil {
			if lis, err := net.ListenTCP("tcp", tcpAddr); err == nil {
				addr := lis.Addr()
				s._lis = lis
				if tcpAddr, ok := addr.(*net.TCPAddr); ok {
					port = tcpAddr.Port
					s.addr = fmt.Sprintf("%s:%d", hostname, port)
					glog.Infof("TrackerServer started at: tcp://%s", s.addr)
					AcceptEx(lis)
				}
			} else {
				glog.Fatal(err)
			}
		} else {
			glog.Fatal(err)
		}
	}
	glog.Fatal("not set a hostname: ", err)
}

type _TrackerClient struct {
	addr string
}

func newTrackerClient(addr string) *_TrackerClient {
	return &_TrackerClient{
		addr: addr,
	}
}

func (c *_TrackerClient) Get(key string) ([]string, error) {
	item := &ActionItem{
		Action: "get",
		Key:    key,
	}
	if reply, err := c.call(item); err == nil {
		if reply.Result == "OK" {
			return reply.Value, nil
		} else {
			return nil, errors.New(reply.Result)
		}
	} else {
		//TODO: log it
		return nil, err
	}

}

func (c *_TrackerClient) Add(key string, value string) (bool, error) {
	item := &ActionItem{
		Action: "add",
		Value:  []string{value},
		Key:    key,
	}
	if reply, err := c.call(item); err == nil {
		return reply.Result == "OK", nil
	} else {
		return false, err
	}
}

func (c *_TrackerClient) Remove(key string) (bool, error) {
	item := &ActionItem{
		Action: "remove",
		Key:    key,
	}
	if reply, err := c.call(item); err == nil {
		return reply.Result == "OK", nil
	} else {
		return false, err
	}
}

func (c *_TrackerClient) RemoveItem(key string, value string) (bool, error) {
	item := &ActionItem{
		Action: "remove_item",
		Value:  []string{value},
		Key:    key,
	}
	if reply, err := c.call(item); err == nil {
		return reply.Result == "OK", nil
	} else {
		return false, err
	}
}

func (c *_TrackerClient) Set(key string, values []string) (bool, error) {
	item := &ActionItem{
		Action: "set",
		Value:  values,
		Key:    key,
	}
	if reply, err := c.call(item); err == nil {
		return reply.Result == "OK", nil
	} else {
		//TODO: log it
		return false, err
	}
}

func (c *_TrackerClient) call(item *ActionItem) (*ReplyItem, error) {
	return SendRPC(c.addr, "RPC_Tracker.Send", item)
}
