package mqtt

import (
	"errors"
	"math/rand"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"github.com/eclipse/paho.mqtt.golang/packets"
)

type ComnsPacket struct {
	packet *PacketAndToken
	c      *client
}

type ComnsInbound struct {
	err error
	cp  packets.ControlPacket
	c   *client
}

type ComnsControlPacket struct {
	packet *packets.ControlPacket
	c      *client
}

type Count struct {
	CountChan     chan *CountMsg
	IncomingCount int64
	AckCount      int64
	OutboundCount int64
}

type CountMsg struct {
	t int
	n int
}

type ClientPool struct {
	addClientChan      chan *client
	clientsWheel       [][]*client
	comns              []*client
	router             *router
	initTime           int64
	cursor             int
	worker             int
	commsoIncoming     chan *ComnsInbound
	oboundFromIncoming chan *ComnsPacket
	commsobound        chan *ComnsPacket
	commsoboundP       chan *ComnsPacket
	Count              *Count
	ForceFlushPing     bool
}

type ClientPoolOption struct {
	Worker int
}

func NewClientPool(o *ClientPoolOption) *ClientPool {

	var worker int
	if o.Worker > 0 {
		worker = o.Worker
	} else {
		worker = runtime.NumCPU()
	}

	cp := &ClientPool{
		make(chan *client, 36000),
		make([][]*client, 30),
		make([]*client, 0, 36000),
		newRouter(),
		time.Now().Unix(),
		0,
		worker,
		make(chan *ComnsInbound, 100000),
		make(chan *ComnsPacket, 100000),
		make(chan *ComnsPacket, 100000),
		make(chan *ComnsPacket, 100000),
		&Count{make(chan *CountMsg, 1000000), 0, 0, 0},
		false,
	}

	go cp.Start()

	return cp
}

func (cp *ClientPool) startConnReading() {

	num := cp.worker * 2
	runtime.GOMAXPROCS(num)

	ioChan := make(chan *client, num*100)

	var newIoReader func()
	newIoReader = func() {
		for {
			c := <-ioChan
			if !c.IsConnected() || c.conn == nil {
				continue
			}
			p, err := packets.ReadPacketNoBlocking(c.reader)
			if err != nil {
				if !strings.Contains(err.Error(), "unsupported packet type") {
					cp.commsoIncoming <- &ComnsInbound{err: err, c: c}
				}
				continue
			}
			if p != nil {
				cp.commsoIncoming <- &ComnsInbound{cp: p, c: c}
			}
		}
	}

	// for i := 0; i < num; i++ {
	go newIoReader()
	// }

	for {
		size := len(cp.comns)
		if size == 0 {
			time.Sleep(time.Millisecond * 500)
			continue
		} else {
			time.Sleep(time.Microsecond * 100)
		}
		for _, idx := range rand.Perm(size) {
			if cp.comns[idx] != nil {
				ioChan <- cp.comns[idx]
			}
		}
	}
}

func (cp *ClientPool) startConnWritinging() {

	num := cp.worker

	ioChan := make(chan *client, num*100)

	var newIoWriter func()
	newIoWriter = func() {
		for {
			c := <-ioChan
			if !c.IsConnected() || c.conn == nil {
				continue
			}

			buffered := c.writer.Buffered()

			if buffered > 244 {
				c.RWMutex.Lock()
				err := c.writer.Flush()
				c.RWMutex.Unlock()
				if err != nil {
					ERROR.Println("io flush error :", err)
				}
			}
		}
	}

	// for i := 0; i < num; i++ {
	go newIoWriter()
	// }

	for {
		size := len(cp.comns)
		if size == 0 {
			time.Sleep(time.Millisecond * 500)
			continue
		} else {
			time.Sleep(time.Microsecond * 100)
		}
		for _, idx := range rand.Perm(size) {
			if cp.comns[idx] != nil {
				ioChan <- cp.comns[idx]
			}
		}
	}
}

func (cp *ClientPool) startKeepalive() {

	pingTicker := time.NewTicker(time.Second * 1)
	defer pingTicker.Stop()
	idx := 0

	for range pingTicker.C {
		idx = idx % 30

		for _, c := range cp.clientsWheel[idx] {
			if c == nil {
				continue
			}

			var pingSent time.Time

			lastSent := c.lastSent.Load()
			lastReceived := c.lastReceived.Load()
			if lastSent == nil || lastReceived == nil {
				continue
			}

			lastSentTime := lastSent.(time.Time)
			lastReceivedTime := lastReceived.(time.Time)

			if time.Since(lastSentTime) >= time.Duration(c.options.KeepAlive*int64(time.Second)) || time.Since(lastReceivedTime) >= time.Duration(c.options.KeepAlive*int64(time.Second)) {
				if atomic.LoadInt32(&c.pingOutstanding) == 0 {
					ping := packets.NewControlPacket(packets.Pingreq).(*packets.PingreqPacket)
					// We don't want to wait behind large messages being sent, the Write call
					// will block until it it able to send the packet.
					atomic.StoreInt32(&c.pingOutstanding, 1)
					if err := ping.Write(c.writer); err != nil {
						ERROR.Println(PNG, err)
					}
					if cp.ForceFlushPing {
						err := c.writer.Flush()
						if err != nil {
							ERROR.Println(PNG, err)
						}
					}
					c.lastSent.Store(time.Now())
					pingSent = time.Now()
				}
			}
			if atomic.LoadInt32(&c.pingOutstanding) > 0 && time.Since(pingSent) >= c.options.PingTimeout {
				CRITICAL.Println(PNG, "pingresp not received, disconnecting")
				c.internalConnLost(errors.New("pingresp not received, disconnecting")) // no harm in calling this if the connection is already down (or shutdown is in progress)
				// cp.clientsWheel[idx][i] = nil
			}
		}
		idx++
	}
}

func (cp *ClientPool) StartIncoming() {
	for i := 0; i < cp.worker; i++ {
		go func() {
			for ci := range cp.commsoIncoming {
				cp.Count.CountChan <- &CountMsg{t: 2, n: 1}

				if ci.err != nil {
					ERROR.Println(ci.err)
					continue
				}

				in := resolveIncomingComms(ci.cp, ci.c)
				if in != nil {
					if in.err != nil {
						ERROR.Println(in.err)
						ci.c.internalConnLost(in.err)
						continue
					}
					if in.outbound != nil {
						cp.oboundFromIncoming <- &ComnsPacket{in.outbound, ci.c}
						continue
					}
					if in.incomingPub != nil {
						cp.router.matchAndDispatch(in.incomingPub, ci.c.options.Order, ci.c, cp.commsoboundP)
					}
				}
			}
		}()
	}
}

func (cp *ClientPool) StartOutcoming() {

	for i := 0; i < cp.worker; i++ {
		go func() {
			for {
				pack, ok := <-cp.commsobound
				cp.Count.CountChan <- &CountMsg{t: 4, n: 1}
				if !ok || pack.c.conn == nil {
					continue
				}
				c := pack.c
				msg := pack.packet.p.(*packets.PublishPacket)

				writeTimeout := pack.c.getWriteTimeOut()
				if writeTimeout > 0 {
					if err := pack.c.conn.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
						ERROR.Println("SetWriteDeadline ", err)
					}
				}

				c.RWMutex.Lock()
				if err := msg.Write(pack.c.writer); err != nil {
					ERROR.Println("outgoing obound reporting error ", err)
					pack.packet.t.setError(err)
					// report error if it's not due to the connection being closed elsewhere
					if !strings.Contains(err.Error(), closedNetConnErrorText) {
						ERROR.Println(err)
					}
					continue
				}
				c.RWMutex.Unlock()

				if writeTimeout > 0 {
					// If we successfully wrote, we don't want the timeout to happen during an idle period
					// so we reset it to infinite.
					if err := pack.c.conn.SetWriteDeadline(time.Time{}); err != nil {
						ERROR.Println("SetWriteDeadline to 0 ", err)
					}
				}

				if msg.Qos == 0 {
					pack.packet.t.flowComplete()
				}
				c.UpdateLastSent()
			}
		}()
	}
	for i := 0; i < cp.worker; i++ {
		go func() {
			for {
				pack, ok := <-cp.oboundFromIncoming
				cp.Count.CountChan <- &CountMsg{t: 3, n: 1}
				if !ok {
					continue
				}
				c := pack.c
				c.RWMutex.Lock()
				if err := pack.packet.p.Write(pack.c.writer); err != nil {
					ERROR.Println("outgoing oboundFromIncoming reporting error", err)
					if pack.packet.t != nil {
						pack.packet.t.setError(err)
					}
					continue
				}
				c.RWMutex.Unlock()
				c.UpdateLastSent()
			}
		}()
	}

	// go func() {
	// 	for {
	// 		pack, ok := <-cp.commsoboundP
	// 		cp.Count.CountChan <- &CountMsg{t: 4, n: 1}

	// 		if !ok {
	// 			continue
	// 		}
	// 		c := pack.c
	// 		if err := pack.packet.p.Write(pack.c.writer); err != nil {
	// 			ERROR.Println("outgoing oboundp reporting error ", err)
	// 			if pack.packet.t != nil {
	// 				pack.packet.t.setError(err)
	// 			}
	// 			continue
	// 		}

	// 		if _, ok := pack.packet.p.(*packets.DisconnectPacket); ok {
	// 			pack.packet.t.(*DisconnectToken).flowComplete()
	// 			// As per the MQTT spec "After sending a DISCONNECT Packet the Client MUST close the Network Connection"
	// 			// Closing the connection will cause the goroutines to end in sequence (starting with incoming comms)
	// 			pack.c.conn.Close()
	// 		}
	// 		c.UpdateLastSent()
	// 	}
	// }()

}

func (cp *ClientPool) Start() {

	go func() {
		cp.startConnReading()
	}()
	go func() {
		cp.startConnWritinging()
	}()
	go func() {
		cp.StartIncoming()
	}()
	go func() {
		cp.StartOutcoming()
	}()

	go func() {
		cp.startKeepalive()
	}()

	go func() {
		for {
			c := <-cp.addClientChan
			idx := (time.Now().Unix() - cp.initTime) % 30
			if cp.clientsWheel[idx] == nil {
				cp.clientsWheel[idx] = make([]*client, 0, 300)
			}
			cp.comns = append(cp.comns, c)
			cp.clientsWheel[idx] = append(cp.clientsWheel[idx], c)
			cp.cursor = (cp.cursor + 1) % len(cp.comns)
		}
	}()

	go func() {
		for {
			msg := <-cp.Count.CountChan
			switch msg.t {
			case 2:
				cp.Count.IncomingCount++
			case 3:
				cp.Count.AckCount++
			case 4:
				cp.Count.OutboundCount++
			}
		}
	}()

}

func (cp *ClientPool) NewPoolClient(o *ClientOptions) Client {
	c := newComnsClient(o, cp.commsobound, cp.commsoboundP)
	cp.addClientChan <- c.GetRawClient()
	return c
}

type CountInfo struct {
	OutboundCount          int64
	AckCount               int64
	IncomingCount          int64
	CommsoboundSize        int
	OboundFromIncomingSize int
	CommsoIncomingSize     int
}

func (cp *ClientPool) Size() *CountInfo {

	ci := &CountInfo{
		cp.Count.OutboundCount / 1000,
		cp.Count.AckCount / 1000,
		cp.Count.IncomingCount / 1000,
		len(cp.commsobound) / 1000,
		len(cp.oboundFromIncoming) / 1000,
		len(cp.commsoIncoming) / 1000,
	}

	cp.Count.AckCount = 0
	cp.Count.OutboundCount = 0
	cp.Count.IncomingCount = 0

	return ci
}
