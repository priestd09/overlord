package cluster

import (
	"bytes"
	errs "errors"
	"strconv"
	"strings"
	"time"

	"overlord/lib/bufio"
	"overlord/lib/conv"
	libnet "overlord/lib/net"
)

const (
	nodesRespType = '$'

	roleMaster = "master"
	roleSlave  = "slave"
)

var (
	crlfBytes  = []byte("\r\n")
	nodesBytes = []byte("*2\r\n$7\r\nCLUSTER\r\n$5\r\nNODES\r\n")
)

var (
	errNodesRespType = errs.New("cluster nodes resp type error")
)

type fetcher struct {
	conn *libnet.Conn
	br   *bufio.Reader
}

func newFetcher(addr string) *fetcher {
	conn := libnet.DialWithTimeout(addr, time.Second, time.Second, time.Second)
	return &fetcher{
		conn: conn,
		br:   bufio.NewReader(conn, bufio.Get(1024)),
	}
}

// CLUSTER NODES response
// 6b22f87b78cdb181f7b9b1e0298da177606394f7 172.17.0.2:7003@17003 slave 8f02f3135c65482ac00f217df0edb6b9702691f8 0 1532770704000 4 connected
// dff2f7b0fbda82c72d426eeb9616d9d6455bb4ff 172.17.0.2:7004@17004 slave 828c400ea2b55c43e5af67af94bec4943b7b3d93 0 1532770704538 5 connected
// b1798ba2171a4bd765846ddb5d5bdc9f3ca6fdf3 172.17.0.2:7000@17000 master - 0 1532770705458 1 connected 0-5460
// db2dd7d6fbd2a03f16f6ab61d0576edc9c3b04e2 172.17.0.2:7005@17005 slave b1798ba2171a4bd765846ddb5d5bdc9f3ca6fdf3 0 1532770704437 6 connected
// 828c400ea2b55c43e5af67af94bec4943b7b3d93 172.17.0.2:7002@17002 master - 0 1532770704000 3 connected 10923-16383
// 8f02f3135c65482ac00f217df0edb6b9702691f8 172.17.0.2:7001@17001 myself,master - 0 1532770703000 2 connected 5461-10922
func (f *fetcher) fetch() (nm map[string]*node, err error) {
	if _, err = f.conn.Write(nodesBytes); err != nil {
		err = errNodesRespType
		return
	}
	_ = f.br.Read()
	line, err := f.br.ReadLine()
	if err != nil {
		err = errNodesRespType
		return
	}
	rTp := line[0]
	if rTp != nodesRespType {
		err = errNodesRespType
		return
	}
	ls := len(line)
	sBs := line[1 : ls-2]
	size, err := conv.Btoi(sBs)
	if err != nil {
		err = errNodesRespType
		return
	}
	if size == -1 {
		err = errNodesRespType
		return
	}
	all := int(size) + 2 // NOTE: contains \r\n
	data, err := f.br.ReadExact(all)
	if err != nil {
		err = errNodesRespType
		return
	}
	nm, err = parseSlots(data)
	return
}

func (f *fetcher) close() {
	if f.conn != nil {
		f.conn.Close()
	}
}

func parseSlots(data []byte) (nm map[string]*node, err error) {
	lines := bytes.Split(data, crlfBytes)
	nm = map[string]*node{}
	for _, line := range lines {
		var n *node
		if n, err = parseNode(line); err != nil {
			err = errNodesRespType
			return
		}
		nm[n.addr] = n
	}
	return
}

type node struct {
	ID   string
	addr string
	// optional port
	gossipAddr string
	// Role is the special flag
	role        string
	flags       []string
	slaveOf     string
	pingSent    int
	pongRecv    int
	configEpoch int
	linkState   string
	slots       []int
}

func parseNode(line []byte) (n *node, err error) {
	if len(bytes.TrimSpace(line)) == 0 {
		err = errNodesRespType
		return
	}
	fields := strings.Fields(string(line))
	if len(fields) < 8 {
		err = errNodesRespType
		return
	}
	n = &node{}
	i := 0
	n.setID(fields[i])
	i++
	n.setAddr(fields[i])
	i++
	n.setFlags(fields[i])
	i++
	n.setSlaveOf(fields[i])
	i++
	n.setPingSent(fields[i])
	i++
	n.setPongRecv(fields[i])
	i++
	n.setConfigEpoch(fields[i])
	i++
	n.setLinkState(fields[i])
	i++
	n.setSlots(fields[i:]...)
	// i++
	return n, nil
}

func (n *node) setID(val string) {
	n.ID = strings.TrimSpace(val)
}

func (n *node) setAddr(val string) {
	trimed := strings.TrimSpace(val)
	// adaptor with 4.x
	splited := strings.Split(trimed, "@")
	n.addr = splited[0]
	if len(splited) == 2 {
		asp := strings.Split(n.addr, ":")
		n.gossipAddr = asp[0] + splited[1]
	}
}

func (n *node) setFlags(val string) {
	flags := strings.Split(val, ",")
	n.flags = flags
	if strings.Contains(val, roleMaster) {
		n.role = roleMaster
	} else if strings.Contains(val, roleSlave) {
		n.role = roleSlave
	}
}

func (n *node) setSlaveOf(val string) {
	n.slaveOf = val
}

func (n *node) setPingSent(val string) {
	ival, err := strconv.Atoi(val)
	if err != nil {
		n.pingSent = 0
	}
	n.pingSent = ival
}

func (n *node) setPongRecv(val string) {
	ival, err := strconv.Atoi(val)
	if err != nil {
		n.pongRecv = 0
	}
	n.pongRecv = ival
}

func (n *node) setConfigEpoch(val string) {
	ival, err := strconv.Atoi(val)
	if err != nil {
		n.configEpoch = 0
	}
	n.configEpoch = ival
}

func (n *node) setLinkState(val string) {
	n.linkState = val
}

func (n *node) setSlots(vals ...string) {
	slots := []int{}
	for _, val := range vals {
		subslots, ok := parseSlotField(val)
		if ok {
			slots = append(slots, subslots...)
		}
	}
	n.slots = slots
}

func parseSlotField(val string) ([]int, bool) {
	if len(val) == 0 || val == "-" {
		return nil, false
	}
	vsp := strings.SplitN(val, "-", 2)
	begin, err := strconv.Atoi(vsp[0])
	if err != nil {
		return nil, false
	}
	if len(vsp) == 1 {
		return []int{begin}, true
	}
	end, err := strconv.Atoi(vsp[1])
	if err != nil {
		return nil, false
	}
	if end < begin {
		return nil, false
	}
	slots := make([]int, 0, end-begin+1)
	for i := begin; i <= end; i++ {
		slots = append(slots, i)
	}
	return slots, true
}
