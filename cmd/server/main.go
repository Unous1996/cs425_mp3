package main

import (
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
)

var (
	coordinatorAddresses = "10.195.210.190"
	coordinatorPort = "6000"
)

var (
	workingChan chan bool
	serverChan chan bool
)

var (
	balance map[string]string
	balanceMutex = sync.RWMutex{}
)

var (
	localIpAddress string
	localHost string
	portNum string
)

var (
	coordinatorConnection *net.TCPConn
)

func checkErr(err error) int {
	if err != nil {
		if err.Error() == "EOF" {
			//fmt.Println(err)
			return 0
		}
		return -1
	}
	return 1
}

func readMessage(conn *net.TCPConn){
	fmt.Println("Start reading message")
	buff := make([]byte, 256)
	for {
		j, err := conn.Read(buff)
		flag := checkErr(err)
		if flag == 0 {
			if(conn.RemoteAddr().String() == coordinatorAddresses + ":" + coordinatorPort) {
				fmt.Println("Coordinator has failed")
				workingChan <- true
			}
			fmt.Println("Received a EOF")
			break
		}

		line_split := strings.Split(string(buff[0:j]), " ")
		fmt.Println("line = ", string(buff[0:j]))

		if(line_split[0] == "SET"){
			object := strings.Split(line_split[1],".")[1]
			balanceMutex.Lock()
			balance[object] = line_split[2]
			balanceMutex.Unlock()
			continue
		}

		if(line_split[0] == "GET"){
			object := strings.Split(line_split[1],".")[1]
			balanceMutex.RLock()
			_, ok := balance[object]
			balanceMutex.RUnlock()
			var replyGet string
			if !ok {
				replyGet = "NO"
			} else {
				balanceMutex.RLock()
				replyGet = balance[object]
				balanceMutex.RUnlock()
			}
			conn.Write([]byte(replyGet))
		}
	}
}

func startServer() {
	tcpAddr, _ := net.ResolveTCPAddr("tcp", localHost)
	tcpListen, err := net.ListenTCP("tcp", tcpAddr)

	if err != nil {
		fmt.Println("#Failed to listen on " + portNum)
	}

	fmt.Println("#Start listening on " + portNum)
	// Accept Tcp connection from other VMs

	for {
		conn, _ := tcpListen.AcceptTCP()
		fmt.Println("Accepted TCP From", conn.RemoteAddr().String())
		defer conn.Close()
		go readMessage(conn)
	}
}

func chanInit(){
	workingChan = make(chan bool)
	serverChan = make(chan bool)
}

func mapInit(){
	balance = make(map[string]string)
}

func initialize(){
	chanInit()
	mapInit()
}

func main(){
	if len(os.Args) != 2 {
		fmt.Println("Incorrect number of parameters")
		os.Exit(1)
	}

	portNum = os.Args[1]

	addrs, err := net.InterfaceAddrs()

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}


	for _, address := range addrs {
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				localIpAddress = ipnet.IP.String()
				fmt.Println("#The local ip address is:", ipnet.IP.String())
			}
		}
	}

	localHost = localIpAddress + ":" + portNum
	initialize()

	go startServer()

	<-workingChan
	fmt.Println("Server Closed")
}