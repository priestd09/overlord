package cluster

import (
	"bytes"
	"overlord/lib/conv"
	"time"

	"overlord/proto"
	"overlord/proto/redis"
)

const (
	respError = '-'
)

var (
	movedBytes = []byte("MOVED")
	askBytes   = []byte("ASK")
)

type nodeConn struct {
	cluster       string
	addr          string
	pnc           proto.NodeConn
	dto, rto, wto time.Duration
}

// NewNodeConn create the node conn from proxy to redis
func NewNodeConn(cluster string, addr string, dialTimeout, readTimeout, writeTimeout time.Duration) (nc proto.NodeConn) {
	nc = &nodeConn{
		cluster: cluster,
		addr:    addr,
		pnc:     redis.NewNodeConn(cluster, addr, dialTimeout, readTimeout, writeTimeout),
		dto:     dialTimeout,
		rto:     readTimeout,
		wto:     writeTimeout,
	}
	defaultCluster.add(addr)
	return
}

func (nc *nodeConn) WriteBatch(mb *proto.MsgBatch) (err error) {
	err = nc.pnc.WriteBatch(mb)
	return
}

func (nc *nodeConn) ReadBatch(mb *proto.MsgBatch) (err error) {
	if err = nc.pnc.ReadBatch(mb); err != nil {
		return
	}
	for _, m := range mb.Msgs() {
		req := m.Request().(*redis.Request)
		resp := req.Reply()
		data := resp.Data()
		if resp.Type() != respError || data == nil {
			continue
		}
		if !bytes.HasPrefix(data, movedBytes) && !bytes.HasPrefix(data, askBytes) {
			continue
		}
		fields := bytes.Fields(data)
		if len(fields) != 3 {
			continue
		}
		if _, err = conv.Btoi(fields[1]); err != nil { // NOTE: slot
			continue
		}
		redirect := proto.NewMsgBatch()
		if bytes.Equal(fields[0], askBytes) {
			askM := proto.GetMsg()
			askM.Type = proto.CacheTypeRedisCluster
			askR := redis.GetReq()
			// askR.resp.reset()
			// askR.resp.rTp = pc.resp.rTp
			// askR.resp.data = pc.resp.data
			askM.WithRequest(askR)
			redirect.AddMsg(askM)
		}
		redirect.AddMsg(m)
		nconn := NewNodeConn(nc.cluster, string(fields[2]), nc.dto, nc.rto, nc.wto)
		if err = nconn.WriteBatch(redirect); err != nil {
			return
		}
		if err = nconn.ReadBatch(redirect); err != nil {
			return
		}
	}
	return
}

func (nc *nodeConn) Ping() (err error) {
	return nc.pnc.Ping()
}

func (nc *nodeConn) Close() (err error) {
	return nc.pnc.Close()
}
