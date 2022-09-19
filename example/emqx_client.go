package main

import (
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"strings"

	// _ "net/http/pprof"
	"os"
	"runtime"
	"strconv"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

var (
	Logger *log.Logger
)

func init() {
	file, err := os.OpenFile("log.log",
		os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalln("打开日志文件失败:", err)
	}

	Logger = log.New(io.MultiWriter(file, os.Stdout),
		"[INFO]  ",
		log.Ldate|log.Ltime)
	mqtt.ERROR = log.New(io.MultiWriter(file, os.Stdout), "[ERROR] ", log.Ldate|log.Ltime|log.Lshortfile)
	mqtt.CRITICAL = log.New(io.MultiWriter(file, os.Stdout), "[CRIT]  ", log.Ldate|log.Ltime|log.Lshortfile)

	mqtt.INFO = log.New(io.MultiWriter(file, os.Stdout), "[DEBUG]  ", log.Ldate|log.Ltime)
}

type EmqxClientManager struct {
	broker       string
	port         int
	cp           *mqtt.ClientPool
	clients      []mqtt.Client
	publishChan  chan *mqtt.Client
	topic        string
	msg          []byte
	countChan    chan int
	sendInterval int
	sendCount    int64
}

func NewEmqxClientManager(cp *mqtt.ClientPool, broker string, port int, sendInterval int) *EmqxClientManager {

	ecm := &EmqxClientManager{
		broker,
		port,
		cp,
		make([]mqtt.Client, 0, 36000),
		make(chan *mqtt.Client, 36000),
		"test",
		nil,
		make(chan int, 1000),
		sendInterval,
		0,
	}

	return ecm
}

func (ecm *EmqxClientManager) publish(client mqtt.Client) mqtt.Token {
	return client.Publish(ecm.topic, 2, false, ecm.msg)
}

func (ecm *EmqxClientManager) newMqttClient(id string) mqtt.Client {
	opts := mqtt.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("tcp://%s:%d", ecm.broker, ecm.port))
	opts.SetClientID(id)
	opts.SetUsername("a")
	opts.SetPassword("1234")
	opts.SetCleanSession(true)
	opts.SetKeepAlive(60 * time.Second)
	return ecm.cp.NewPoolClient(opts)
}

func (ecm *EmqxClientManager) startSendMsg() {

	timeTickerChan := time.Tick(time.Millisecond * time.Duration(ecm.sendInterval))

	go func() {
		for i := 0; i < runtime.NumCPU(); i++ {
			go func() {
				for {
					c := <-ecm.publishChan
					if (*c).IsConnected() && (*c).GetWaitting() < 100 {
						ecm.publish(*c)
						// ecm.countChan <- 1
					}
				}
			}()
		}
	}()

	go func() {
		for {
			<-time.Tick(time.Second * 10)

			ci := ecm.cp.Size()

			max := 0
			sum := 0

			for _, c := range ecm.clients {
				n := c.GetWaitting()
				if n > max {
					max = n
				}
				sum += n
			}

			Logger.Println("最大待确认发送：", max, "平均待确认发送：", sum/len(ecm.clients), "发送通道读次数：", ci.OutboundCount, "K,确认通道读次数", ci.AckCount,
				"K,接收通道读次数", ci.IncomingCount, "K,发送通道大小：", ci.CommsoboundSize, "K,确认通道大小：",
				ci.OboundFromIncomingSize, "K,接收通道大小：", ci.CommsoIncomingSize, "K")

		}
	}()

	for {
		<-timeTickerChan
		if ecm.cp.ForceFlushPing {
			continue
		}

		ecm.msg = []byte(fmt.Sprintf("[{\"timestamp\": \"%d\"}]", time.Now().UnixNano()/int64(time.Millisecond)))
		size := len(ecm.clients)

		// go func() {
		for _, idx := range rand.Perm(size) {
			ecm.publishChan <- &ecm.clients[idx]
		}
		// }()
	}
}

func (ecm *EmqxClientManager) addMqttClient(connNum int, clientId string) {

	parallChan := make(chan int)
	connectChan := make(chan *mqtt.Client, 100)

	for i := 0; i < 100; i++ {
		go func() {
			for id := range parallChan {
				client := ecm.newMqttClient(fmt.Sprintf("%s_%d", clientId, id))
				tk := client.Connect()
				if !tk.Wait() {
					Logger.Panicln(fmt.Sprintf("%s_%d", clientId, id), "连接失败", tk.Error())
					parallChan <- id
				} else {
					connectChan <- &client
				}
				// client := ecm.newMqttClient(fmt.Sprintf("%s_%d", clientId, id))
				// connectChan <- &client
			}
		}()
	}

	go func() {
		for c := range connectChan {
			ecm.clients = append(ecm.clients, *c)
			if len(ecm.clients) == connNum {
				Logger.Println("客户端连接：", fmt.Sprintf("%.2f", float64(len(ecm.clients))/1000), "K")
				close(parallChan)
				close(connectChan)
				// return
			} else if len(ecm.clients)%777 == 0 {
				Logger.Println("客户端连接：", fmt.Sprintf("%.2f", float64(len(ecm.clients))/1000), "K")
			}
		}
	}()

	for i := 0; i < connNum; i++ {
		parallChan <- i
	}
}

func getMacAddress() (macAddrs string) {
	netInterfaces, err := net.Interfaces()
	if err != nil {
		Logger.Panic("取得本机Mac地址失败", err)
		return macAddrs
	}

	for _, netInterface := range netInterfaces {
		hardwareAddr := netInterface.HardwareAddr.String()
		if len(hardwareAddr) == 0 {
			continue
		} else {
			macAddrs = strings.ReplaceAll(hardwareAddr, ":", "")
			break
		}
	}
	return macAddrs
}

func main() {
	totalConnect := 36000
	sendInterval := 1000
	broker := "192.168.10.10"
	port := 22175
	for idx, args := range os.Args {
		if args == "-i" {
			n, err := strconv.Atoi(os.Args[idx+1])
			if err != nil {
				Logger.Println("解析参数-i异常：", err)
			} else {
				sendInterval = n
				Logger.Println("发送间隔：", n)
			}
		}
		if args == "-n" {
			n, err := strconv.Atoi(os.Args[idx+1])
			if err != nil {
				Logger.Println("解析参数-n异常：", err)
			} else {
				Logger.Println("总连接数：", n)
				totalConnect = n
			}
		}
		if args == "-h" {
			s := os.Args[idx+1]
			broker = s
		}
		if args == "-p" {
			n, err := strconv.Atoi(os.Args[idx+1])
			if err != nil {
				Logger.Println("解析参数-p异常：", err)
			} else {
				port = n
			}
		}
	}

	// go http.ListenAndServe("localhost:6060", nil)

	cp := mqtt.NewClientPool(&mqtt.ClientPoolOption{})

	ecm := NewEmqxClientManager(cp, broker, port, sendInterval)

	mac := getMacAddress()

	Logger.Println(mac)

	ecm.cp.ForceFlushPing = true
	ecm.addMqttClient(totalConnect, mac)
	ecm.cp.ForceFlushPing = false
	ecm.startSendMsg()

}
