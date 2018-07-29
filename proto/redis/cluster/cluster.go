package cluster

import (
	"sync"
	"sync/atomic"
	"time"

	"overlord/lib/hashkit"
	"overlord/proto"
)

const (
	musk     = 0x3fff // 16383
	slotSize = musk + 1
)

var (
	defaultCluster = &cluster{}
	once           sync.Once
)

type cluster struct {
	addrm map[string]struct{}
	lock  sync.Mutex

	nodes atomic.Value // NOTE: store []string
	slotN atomic.Value // NOTE: store []int
}

// Router return default cluster.
func Router() proto.Router {
	return defaultCluster
}

// GetNode get hash node.
func (c *cluster) GetNode(key []byte) (node string, ok bool) {
	slot := hashkit.Crc16(key) & musk
	sn := c.slotN.Load().([]int)
	if int(slot) >= len(sn) {
		return
	}
	n := sn[slot]
	if n == -1 {
		return
	}
	nodes := c.nodes.Load().([]string)
	if n >= len(nodes) {
		return
	}
	node = nodes[n]
	ok = true
	return
}

func (c *cluster) add(addr string) {
	c.lock.Lock()
	c.addrm[addr] = struct{}{}
	c.lock.Unlock()

	once.Do(func() {
		go c.fetchNodes()
	})
}

func (c *cluster) fetchNodes() {
	var randAddr = func() string {
		for addr := range c.addrm {
			return addr
		}
		return ""
	}
	for {
		var (
			addr = randAddr()
			f    = newFetcher(addr)
		)
		nm, err := f.fetch()
		if err != nil {
			f.close()
			time.Sleep(time.Minute)
			continue
		}
		slotN := make([]int, slotSize)
		for i := range slotN {
			slotN[i] = -1
		}
		var m int
		nodes := make([]string, 0, len(nm))
		for _, n := range nm {
			if n.role == roleMaster {
				nodes = append(nodes, n.addr)
				for _, slot := range n.slots {
					slotN[slot] = m
				}
				m++
			}
		}
		f.close()
		c.nodes.Store(nodes)
		c.slotN.Store(slotN)
		time.Sleep(time.Minute)
	}
}
