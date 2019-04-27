package main

import (
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
)

var (
	//serverName = []string{"A", "B", "C", "D", "E"}
	//serverPorts = []string{"7001", "7002", "7003", "7004", "7005"}
	serverName = []string{"A"}
	serverPorts = []string{"7001"}
	serverIpAddr = "10.195.210.190"
)

var (
	localIpAddress string
	localHost string
	portNum string
)

var (
	workingChan chan bool
	dialChan chan bool
)

var (

	serverConnMap map[string]net.Conn
	lockMap map[string]map[int]int

	serverConnMapMutex = sync.RWMutex{}
	lockMapMutex = sync.RWMutex{}
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

func startCoordinator() {
	tcpAddr, _ := net.ResolveTCPAddr("tcp", localHost)
	tcpListen, err := net.ListenTCP("tcp", tcpAddr)

	if err != nil {
		fmt.Println("#Failed to listen on " + portNum)
	}

	fmt.Println("#Start listening on " + portNum)

	//Dial to 5 servers: A, B, C, D, E
	for i, port := range serverPorts {

		serverAddr := serverIpAddr + ":" + port

		tcpAdd, _ := net.ResolveTCPAddr("tcp", serverAddr)
		conn, err := net.DialTCP("tcp", nil, tcpAdd)
		if err != nil {
			fmt.Printf("#Failed to connect to Server%v.", serverName[i])
		}
		serverConnMap[serverName[i]] = conn
		defer conn.Close()

	}

	// Accept Tcp connection from clients
	for {

		conn, _ := tcpListen.AcceptTCP()
		defer conn.Close()
		go handleTransaction(conn)

	}

}

func chanInit(){
	//workingChan = make(chan bool)
	dialChan = make(chan bool)
}

func mapInit()  {
	serverConnMap = make(map[string]net.Conn)
	lockMap = make(map[string]map[int]int)
}

func initialize(){
	chanInit()
	mapInit()
}

func handleTransaction(conn net.Conn)  {

	buff := make([]byte, 10000)
	endChan := make(chan bool)

	for {

		for {

			j, err := conn.Read(buff)
			if err != nil {
				fmt.Println("#Failed to connect to Client.")
				endChan <- true
			}

			msg := string(buff[0:j])

			if msg == "BEGIN" {
				fmt.Println("#Start a transaction.")
				break;
			}

		}

		updateMap := make(map[string]string)
		logMap := make(map[string]string)
		holdLockMap := make(map[string]int)

		for {

			j, err := conn.Read(buff)
			flag := checkErr(err)
			if flag == 0 {
				break
			}

			line := string(buff[0:j])
			line_split := strings.Split(line, " ")


			if line_split[0] == "ABORT" {

				for k, v := range holdLockMap {

					if v == 1 {

						lockMapMutex.Lock()
						lockMap[k][1] -= 1
						lockMapMutex.Unlock()

					}

					if v == 2 {
						lockMapMutex.Lock()
						lockMap[k][2] = 0
						lockMapMutex.Unlock()
					}

				}


				conn.Write([]byte("ABORTED"))
				break
			}

			if line_split[0] == "COMMIT" {

				for k, v := range logMap {

					serverConnMapMutex.RLock()
					serverConn := serverConnMap[k]
					serverConnMapMutex.RUnlock()
					serverConn.Write([]byte(v))

				}

				for k, v := range holdLockMap {

					if v == 1 {

						lockMapMutex.Lock()
						lockMap[k][1] -= 1
						lockMapMutex.Unlock()

					}

					if v == 2 {

						lockMapMutex.Lock()
						lockMap[k][2] = 0
						lockMapMutex.Unlock()

					}

				}

				conn.Write([]byte("COMMIT OK"))
				break
			}

			if line_split[0] == "SET" {

				server := strings.Split(line_split[1],".")[0]

				for {

					lockMapMutex.RLock()
					_, ok := lockMap[line_split[1]]
					lockMapMutex.RUnlock()

					if !ok {

						lockMapMutex.Lock()
						lockMap[line_split[1]] = map[int]int{}
						lockMapMutex.Unlock()

					}

					lockMapMutex.RLock()
					WLock := lockMap[line_split[1]][2]
					RLock := lockMap[line_split[1]][1]
					lockMapMutex.RUnlock()

					if WLock == 1 {

						if holdLockMap[line_split[1]] == 2 {

							updateMap[line_split[1]] = line_split[2]
							logMap[server] = line
							conn.Write([]byte("OK"))
							break

						}

					}else {

						if RLock == 0 || ( RLock == 1 && holdLockMap[line_split[1]] == 1) {

							lockMapMutex.Lock()
							lockMap[line_split[1]][2] = 1
							lockMapMutex.Unlock()

							holdLockMap[line_split[1]] = 2

							if RLock == 1 {

								lockMapMutex.Lock()
								lockMap[line_split[1]][1] -= 1
								lockMapMutex.Unlock()

							}

							updateMap[line_split[1]] = line_split[2]
							logMap[server] = line
							conn.Write([]byte("OK"))
							break

						}

					}

				}

			}

			if line_split[0] == "GET" {


				for {

					lockMapMutex.RLock()
					_, ok := lockMap[line_split[1]]
					lockMapMutex.RUnlock()

					if !ok {

						lockMapMutex.Lock()
						lockMap[line_split[1]] = map[int]int{}
						lockMapMutex.Unlock()

					}

					lockMapMutex.RLock()
					WLock := lockMap[line_split[1]][2]
					lockMapMutex.RUnlock()

					if WLock == 1 && holdLockMap[line_split[1]] != 2 {
						continue
					}

					if holdLockMap[line_split[1]] == 0 {

						holdLockMap[line_split[1]] = 1

						lockMapMutex.Lock()
						lockMap[line_split[1]][1] += 1
						lockMapMutex.Unlock()

					}

					break
				}
				
				server := strings.Split(line_split[1],".")[0]
				v, ok := updateMap[line_split[1]]

				if ok {

					msg := line_split[1] + " = " + v
					conn.Write([]byte(msg))

				}else {

					serverConnMapMutex.RLock()
					serverConn := serverConnMap[server]
					serverConnMapMutex.RUnlock()
					serverConn.Write([]byte(line))
					j, err := serverConn.Read(buff)

					if err != nil {
						fmt.Printf("#Failed to connect to Server%v.", server)
						panic("Server" + server + " disconnected.")
					}

					if string(buff[0:j]) == "NO" {
						// return NOT FOUND and abort the transaction.

						lockMapMutex.Lock()
						lockMap[line_split[1]][1] -= 1
						lockMapMutex.Unlock()

						conn.Write([]byte("NO FOUND"))
						conn.Write([]byte("This transaction is being aborted, please start a new one."))
						break

					}else {

						updateMap[line_split[1]] = string(buff[0:j])
						msg := line_split[1] + " = " + string(buff[0:j])
						conn.Write([]byte(msg))

					}
				}

			}

		}

	}

	<-endChan

}

func main(){

	if len(os.Args) != 2 {
		fmt.Println("#Incorrect number of parameters")
		os.Exit(1)
	}

	portNum = os.Args[1]

	fmt.Println("#This is the coordinator code")

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

	startCoordinator()

	//<-workingChan
}
