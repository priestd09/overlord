package proxy

import (
	"bytes"
	errs "errors"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"overlord/lib/backoff"
	"overlord/lib/conv"
	"overlord/lib/hashkit"
	"overlord/lib/log"
	libnet "overlord/lib/net"
	"overlord/proto"
	"overlord/proto/memcache"
	mcbin "overlord/proto/memcache/binary"
	"overlord/proto/redis"

	"github.com/pkg/errors"
)

const (
	executorStateOpening = int32(0)
	executorStateClosed  = int32(1)
)

// errors
var (
	ErrConfigServerFormat = errs.New("servers config format error")
	ErrExecutorHashNoNode = errs.New("executor hash no hit node")
	ErrExecutorClosed     = errs.New("executor already closed")
)

var (
	defaultExecCacheTypes = map[string]struct{}{
		proto.CacheTypeMemcache:       struct{}{},
		proto.CacheTypeMemcacheBinary: struct{}{},
		proto.CacheTypeRedis:          struct{}{},
	}
)

// NewExecutor new a executor by cluster config.
func NewExecutor(cc *ClusterConfig) (c proto.Executor) {
	// new executor
	if _, ok := defaultExecCacheTypes[cc.CacheType]; ok {
		return newDefaultExecutor(cc)
	}
	panic("unsupported protocol")
}

// defaultExecutor implement the default hashring router and msgbatch.
type defaultExecutor struct {
	cc *ClusterConfig

	ring    *hashkit.HashRing
	hashTag []byte

	// recording alias to real node
	alias    bool
	aliasMap map[string]string
	nodeChan map[string]*batchChan
	nodePing map[string]*pinger

	state int32
}

// newDefaultExecutor must combine.
func newDefaultExecutor(cc *ClusterConfig) proto.Executor {
	e := &defaultExecutor{cc: cc}
	// parse servers config
	addrs, ws, ans, alias, err := parseServers(cc.Servers)
	if err != nil {
		panic(err)
	}
	e.alias = alias
	e.hashTag = []byte(cc.HashTag)
	e.ring = hashkit.NewRing(cc.HashDistribution, cc.HashMethod)
	e.aliasMap = make(map[string]string)
	if alias {
		for idx, aname := range ans {
			e.aliasMap[aname] = addrs[idx]
		}
		e.ring.Init(ans, ws)
	} else {
		e.ring.Init(addrs, ws)
	}
	// start nbc
	e.nodeChan = make(map[string]*batchChan)
	for _, addr := range addrs {
		e.nodeChan[addr] = e.process(cc, addr)
	}
	if cc.PingAutoEject {
		e.nodePing = make(map[string]*pinger)
		for idx, addr := range addrs {
			w := ws[idx]
			pc := newPingConn(cc, addr)
			p := &pinger{ping: pc, cc: cc, node: addr, weight: w}
			go e.processPing(p)
		}
	}
	return e
}

// Execute impl proto.Executor
func (e *defaultExecutor) Execute(mba *proto.MsgBatchAllocator, msgs []*proto.Message) error {
	if closed := atomic.LoadInt32(&e.state); closed == executorStateClosed {
		return ErrExecutorClosed
	}
	for _, m := range msgs {
		if m.IsBatch() {
			for _, subm := range m.Batch() {
				addr, ok := e.getAddr(subm.Request().Key())
				if !ok {
					m.WithError(ErrExecutorHashNoNode)
					return ErrExecutorHashNoNode
				}
				mba.AddMsg(addr, subm)
			}
		} else {
			addr, ok := e.getAddr(m.Request().Key())
			if !ok {
				m.WithError(ErrExecutorHashNoNode)
				return ErrExecutorHashNoNode
			}
			mba.AddMsg(addr, m)
		}
	}
	for addr, mb := range mba.MsgBatchs() {
		if mb.Count() > 0 {
			e.nodeChan[addr].push(mb)
		}
	}
	return nil
}

// Close close executor.
func (e *defaultExecutor) Close() error {
	if !atomic.CompareAndSwapInt32(&e.state, executorStateOpening, executorStateClosed) {
		return nil
	}
	return nil
}

// process will start the special backend connection.
func (e *defaultExecutor) process(cc *ClusterConfig, addr string) *batchChan {
	conns := cc.NodeConnections
	nbc := newBatchChan(conns)
	for i := int32(0); i < conns; i++ {
		ch := nbc.get(i)
		nc := newNodeConn(cc, addr)
		go e.processIO(cc.Name, addr, ch, nc)
	}
	return nbc
}

func (e *defaultExecutor) processIO(cluster, addr string, ch <-chan *proto.MsgBatch, nc proto.NodeConn) {
	for {
		mb := <-ch
		if err := nc.WriteBatch(mb); err != nil {
			err = errors.Wrap(err, "Cluster batch write")
			mb.BatchDoneWithError(cluster, addr, err)
			continue
		}
		if err := nc.ReadBatch(mb); err != nil {
			err = errors.Wrap(err, "Cluster batch read")
			mb.BatchDoneWithError(cluster, addr, err)
			continue
		}
		mb.BatchDone(cluster, addr)
	}
}

func (e *defaultExecutor) processPing(p *pinger) {
	del := false
	for {
		if err := p.ping.Ping(); err != nil {
			p.failure++
			p.retries = 0
			log.Warnf("node ping fail:%d times with err:%v", p.failure, err)
			if netE, ok := err.(net.Error); !ok || !netE.Temporary() {
				p.ping.Close()
				p.ping = newPingConn(p.cc, p.node)
			}
		} else {
			p.failure = 0
			if del {
				e.ring.AddNode(p.node, p.weight)
				del = false
			}
		}
		if e.cc.PingAutoEject && p.failure >= e.cc.PingFailLimit {
			e.ring.DelNode(p.node)
			del = true
		}
		select {
		case <-time.After(backoff.Backoff(p.retries)):
			p.retries++
		}
	}
}

func (e *defaultExecutor) getAddr(key []byte) (addr string, ok bool) {
	if addr, ok = e.ring.GetNode(e.trimHashTag(key)); !ok {
		return
	}
	if e.alias {
		addr, ok = e.aliasMap[addr]
	}
	return
}

func (e *defaultExecutor) trimHashTag(key []byte) []byte {
	if len(e.hashTag) != 2 {
		return key
	}
	bidx := bytes.IndexByte(key, e.hashTag[0])
	if bidx == -1 {
		return key
	}
	eidx := bytes.IndexByte(key[bidx+1:], e.hashTag[1])
	if eidx == -1 {
		return key
	}
	return key[bidx+1 : bidx+1+eidx]
}

type batchChan struct {
	idx int32
	cnt int32
	chs []chan *proto.MsgBatch
}

func newBatchChan(n int32) *batchChan {
	chs := make([]chan *proto.MsgBatch, n)
	for i := int32(0); i < n; i++ {
		chs[i] = make(chan *proto.MsgBatch, 1024)
	}
	return &batchChan{cnt: n, chs: chs}
}

func (c *batchChan) push(m *proto.MsgBatch) {
	i := atomic.AddInt32(&c.idx, 1)
	c.chs[i%c.cnt] <- m
}

func (c *batchChan) get(i int32) chan *proto.MsgBatch {
	return c.chs[i%c.cnt]
}

type pinger struct {
	cc     *ClusterConfig
	ping   proto.Pinger
	node   string
	weight int

	failure int
	retries int
}

func newNodeConn(cc *ClusterConfig, addr string) proto.NodeConn {
	dto := time.Duration(cc.DialTimeout) * time.Millisecond
	rto := time.Duration(cc.ReadTimeout) * time.Millisecond
	wto := time.Duration(cc.WriteTimeout) * time.Millisecond
	switch cc.CacheType {
	case proto.CacheTypeMemcache:
		return memcache.NewNodeConn(cc.Name, addr, dto, rto, wto)
	case proto.CacheTypeMemcacheBinary:
		return mcbin.NewNodeConn(cc.Name, addr, dto, rto, wto)
	case proto.CacheTypeRedis:
		return redis.NewNodeConn(cc.Name, addr, dto, rto, wto)
	default:
		panic(proto.ErrNoSupportCacheType)
	}
}

func newPingConn(cc *ClusterConfig, addr string) proto.Pinger {
	dto := time.Duration(cc.DialTimeout) * time.Millisecond
	rto := time.Duration(cc.ReadTimeout) * time.Millisecond
	wto := time.Duration(cc.WriteTimeout) * time.Millisecond

	conn := libnet.DialWithTimeout(addr, dto, rto, wto)
	switch cc.CacheType {
	case proto.CacheTypeMemcache:
		return memcache.NewPinger(conn)
	case proto.CacheTypeMemcacheBinary:
		return mcbin.NewPinger(conn)
	case proto.CacheTypeRedis:
		return redis.NewPinger(conn)
	default:
		panic(proto.ErrNoSupportCacheType)
	}
}

func parseServers(svrs []string) (addrs []string, ws []int, ans []string, alias bool, err error) {
	for _, svr := range svrs {
		if strings.Contains(svr, " ") {
			alias = true
		} else if alias {
			err = ErrConfigServerFormat
			return
		}
		var (
			ss    []string
			addrW string
		)
		if alias {
			ss = strings.Split(svr, " ")
			if len(ss) != 2 {
				err = ErrConfigServerFormat
				return
			}
			addrW = ss[0]
			ans = append(ans, ss[1])
		} else {
			addrW = svr
		}
		ss = strings.Split(addrW, ":")
		if len(ss) != 3 {
			err = ErrConfigServerFormat
			return
		}
		addrs = append(addrs, net.JoinHostPort(ss[0], ss[1]))
		w, we := conv.Btoi([]byte(ss[2]))
		if we != nil || w <= 0 {
			err = ErrConfigServerFormat
			return
		}
		ws = append(ws, int(w))
	}
	return
}