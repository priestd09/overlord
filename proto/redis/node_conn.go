package redis

import (
	libnet "overlord/lib/net"
	"overlord/proto"
	"sync/atomic"
	"time"
)

const (
	closed = uint32(0)
	opened = uint32(1)
)

type nodeConn struct {
	cluster string
	addr    string
	conn    *libnet.Conn
	rc      *respConn
	p       *pinger
	state   uint32
}

// NewNodeConn create the node conn from proxy to redis
func NewNodeConn(cluster, addr string, dialTimeout, readTimeout, writeTimeout time.Duration) (nc proto.NodeConn) {
	conn := libnet.DialWithTimeout(addr, dialTimeout, readTimeout, writeTimeout)
	return newNodeConn(cluster, addr, conn)
}

func newNodeConn(cluster, addr string, conn *libnet.Conn) proto.NodeConn {
	return &nodeConn{
		cluster: cluster,
		addr:    addr,
		rc:      newRespConn(conn),
		conn:    conn,
		p:       newPinger(conn),
		state:   closed,
	}
}

func (nc *nodeConn) WriteBatch(mb *proto.MsgBatch) error {
	for _, m := range mb.Msgs() {
		err := nc.write(m)
		if err != nil {
			m.DoneWithError(err)
			return err
		}
		m.MarkWrite()
	}
	err := nc.rc.Flush()
	return err
}

func (nc *nodeConn) write(m *proto.Message) error {
	cmd, ok := m.Request().(*Command)
	if !ok {
		m.DoneWithError(ErrBadAssert)
		return ErrBadAssert
	}
	return nc.rc.encode(cmd.respObj)
}

func (nc *nodeConn) ReadBatch(mb *proto.MsgBatch) error {
	nc.rc.br.ResetBuffer(mb.Buffer())
	defer nc.rc.br.ResetBuffer(nil)

	count := mb.Count()
	resps, err := nc.rc.decodeCount(count)
	if err != nil {
		return err
	}
	for i, msg := range mb.Msgs() {
		cmd, ok := msg.Request().(*Command)
		if !ok {
			return ErrBadAssert
		}
		cmd.reply = resps[i]
		msg.MarkRead()
	}
	return nil
}

func (nc *nodeConn) Ping() error {
	return nc.p.ping()
}

func (nc *nodeConn) Close() error {
	if atomic.CompareAndSwapUint32(&nc.state, opened, closed) {
		return nc.conn.Close()
	}
	return nil
}

var (
	robjCluterNodes = newRespArray([]*resp{
		newRespBulk([]byte("7\r\nCLUSTER")),
		newRespBulk([]byte("5\r\nNODES")),
	})
)

func (nc *nodeConn) FetchSlots() (nodes []string, slots [][]int, err error) {
	err = nc.rc.encode(robjCluterNodes)
	if err != nil {
		return
	}
	err = nc.rc.Flush()
	if err != nil {
		return
	}
	rs, err := nc.rc.decodeCount(1)
	if err != nil {
		return
	}
	robj := rs[0]
	ns, err := ParseSlots(robj)
	if err != nil {
		return
	}

	cns := ns.GetNodes()
	nodes = make([]string, 0)
	slots = make([][]int, 0)
	for _, node := range cns {
		if node.Role() == roleMaster {
			nodes = append(nodes, node.Addr())
			slots = append(slots, node.Slots())
		}
	}

	return
}