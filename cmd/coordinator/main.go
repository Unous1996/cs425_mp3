package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
)

type Node struct {
	neighbors []*Node
	txid string
}

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
	abortChans [10]chan bool
	beginChans [10]chan bool
	commitChans [10]chan bool
	setChans [10]chan string
	getChans [10]chan string
)

var (
	serverConnMap map[string]net.Conn
	lockMap map[string]map[int]int
	readLockHolderMNap map[string]map[string]bool
	writeLockHolderMNap map[string]string
	ip2ChannelIndexMap map[string]int

	serverConnMapMutex = sync.RWMutex{}
	lockMapMutex = sync.RWMutex{}
	readLockHolderMNapMutex = sync.RWMutex{}
	writeLockHolderMNapMutex = sync.RWMutex{}
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

func deadlockDetection (nodemap map[string]*Node) bool {
	//calculating the in degree of each graph
	indegree := make(map[string]int)
	for _, node := range nodemap {
		for _, neighbor := range node.neighbors {
			indegree[neighbor.txid] += 1
		}
	}

	var queue []*Node
	//Push all the nodes with zero indegree in the queue
	for _, node := range nodemap {
		if indegree[node.txid] == 0 {
			queue = append(queue, node)
		}
	}

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		for _, neighbor := range current.neighbors {
			indegree[neighbor.txid] -= 1
			if indegree[neighbor.txid] == 0 {
				queue = append(queue, neighbor)
			}
		}
	}

	for _, value := range indegree {
		if value > 0 {
			return true
		}
	}

	return false
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
		go readMessage(conn)
	}

}

func chanInit(){
	//workingChan = make(chan bool)
	dialChan = make(chan bool)
}

func mapInit(){
	serverConnMap = make(map[string]net.Conn)
	lockMap = make(map[string]map[int]int)
	readLockHolderMNap = make(map[string]map[string]bool)
	writeLockHolderMNap = make(map[string]string)
}

func initialize(){
	chanInit()
	mapInit()
}

func readMessage(conn net.Conn) {

	buff := make([]byte, 10000)
	endChan := make(chan bool)
	remoteIpIndex := ip2ChannelIndexMap[strings.Split(conn.RemoteAddr().String(),":")[0]]
	transactionPrefix := conn.RemoteAddr().String()
	fmt.Println("remoteIpIndex = ", remoteIpIndex)
	go handleTransaction(conn, remoteIpIndex, transactionPrefix)

	for {
		j, err := conn.Read(buff)
		if err != nil {
			fmt.Println("#Failed to connect to Client.")
			endChan <- true
		}

		msg := string(buff[0:j])

		if msg == "BEGIN" {
			fmt.Println("recevide a BEGIN")
			beginChans[remoteIpIndex] <- true
			continue
		}

		if msg == "COMMIT" {
			commitChans[remoteIpIndex] <- true
			continue
		}

		if msg == "ABORT" {
			abortChans[remoteIpIndex] <- true
			continue
		}

		messageSplit := strings.Split(msg, " ")
		messageType := messageSplit[0]


		if messageType == "SET" {
			fmt.Println("received a SET")
			setChans[remoteIpIndex] <- msg
		}

		if messageType == "GET" {
			getChans[remoteIpIndex] <- msg
		}

	}

	<-endChan
}

func handleTransaction(conn net.Conn, remoteIpindex int, transactionPrefix string)  {

	count := 0
	serverBuff := make([]byte, 10000)
	//remoteIpIndex := ip2ChannelIndexMap[strings.Split(conn.RemoteAddr().String(),":")[1]]

	for {
		fmt.Println("Waiting for BEGIN")
		<-beginChans[remoteIpindex]
		count += 1
		transactionID := transactionPrefix + "_" + strconv.Itoa(count)
		fmt.Println("#Start a transaction whose ID is", transactionID)
		conn.Write([]byte("OK"))

		updateMap := make(map[string]string)
		logMap := make(map[string][]string)
		holdLockMap := make(map[string]int)
		breakLoop :=  false

		for {
			fmt.Printf("RemoteIdx = %d, breakLoop = %d\n", remoteIpindex, breakLoop)
			if breakLoop {
				break
			}

			select {
				case <- commitChans[remoteIpindex]:
					for k, v := range logMap {

						serverConnMapMutex.RLock()
						serverConn := serverConnMap[k]
						serverConnMapMutex.RUnlock()

						for _, line := range v {
							serverConn.Write([]byte(line+"\n"))
						}
					}

					for k, v := range holdLockMap {

						if v == 1 {

							lockMapMutex.Lock()
							lockMap[k][1] -= 1
							lockMapMutex.Unlock()
							readLockHolderMNapMutex.Lock()
							delete(readLockHolderMNap[k], transactionID)
							readLockHolderMNapMutex.Unlock()
						}

						if v == 2 {

							lockMapMutex.Lock()
							lockMap[k][2] = 0
							lockMapMutex.Unlock()
							writeLockHolderMNapMutex.Lock()
							writeLockHolderMNap[k] = ""
							writeLockHolderMNapMutex.Unlock()
						}

					}

					conn.Write([]byte("COMMIT OK"))
					breakLoop = true

				case <- abortChans[remoteIpindex]:

					for k, v := range holdLockMap {

						if v == 1 {

							lockMapMutex.Lock()
							lockMap[k][1] -= 1
							lockMapMutex.Unlock()
							readLockHolderMNapMutex.Lock()
							delete(readLockHolderMNap[k], transactionID)
							readLockHolderMNapMutex.Unlock()
						}

						if v == 2 {
							lockMapMutex.Lock()
							lockMap[k][2] = 0
							lockMapMutex.Unlock()
							writeLockHolderMNapMutex.Lock()
							writeLockHolderMNap[k] = ""
							writeLockHolderMNapMutex.Unlock()
						}

					}
					fmt.Println("Abort type 1")
					conn.Write([]byte("ABORTED"))
					breakLoop = true

				case line := <-setChans[remoteIpindex]:
					lineSplit := strings.Split(line, " ")
					server := strings.Split(lineSplit[1],".")[0]
					abortWait := false

					for {

						lockMapMutex.RLock()
						_, ok := lockMap[lineSplit[1]]
						lockMapMutex.RUnlock()

						if !ok {

							lockMapMutex.Lock()
							lockMap[lineSplit[1]] = map[int]int{}
							lockMapMutex.Unlock()

						}

						lockMapMutex.RLock()
						WLock := lockMap[lineSplit[1]][2]
						RLock := lockMap[lineSplit[1]][1]
						lockMapMutex.RUnlock()

						if WLock >= 1 {

							if holdLockMap[lineSplit[1]] == 2 {
								fmt.Println("abortWait = ", abortWait)
								if abortWait {
									for k, v := range holdLockMap {

										if v == 1 {

											lockMapMutex.Lock()
											lockMap[k][1] -= 1
											lockMapMutex.Unlock()
											readLockHolderMNapMutex.Lock()
											delete(readLockHolderMNap[k], transactionID)
											readLockHolderMNapMutex.Unlock()
										}

										if v == 2 {
											lockMapMutex.Lock()
											lockMap[k][2] = 0
											lockMapMutex.Unlock()
											writeLockHolderMNapMutex.Lock()
											writeLockHolderMNap[k] = ""
											writeLockHolderMNapMutex.Unlock()
										}

									}
									fmt.Println("Abort Type 2")
									conn.Write([]byte("ABORTED"))
									breakLoop = true
									break
								}
 								updateMap[lineSplit[1]] = lineSplit[2]
								logMap[server] = append(logMap[server], line)
								conn.Write([]byte("OK"))
								break
							} else {
								select {
									case <-abortChans[remoteIpindex]:
										abortWait = true
									default:
								}
							}

						} else {

							if RLock == 0 || ( RLock == 1 && holdLockMap[lineSplit[1]] == 1) {
								if abortWait {
									for k, v := range holdLockMap {

										if v == 1 {

											lockMapMutex.Lock()
											lockMap[k][1] -= 1
											lockMapMutex.Unlock()
											readLockHolderMNapMutex.Lock()
											delete(readLockHolderMNap[k], transactionID)
											readLockHolderMNapMutex.Unlock()
										}

										if v == 2 {
											lockMapMutex.Lock()
											lockMap[k][2] = 0
											lockMapMutex.Unlock()
											writeLockHolderMNapMutex.Lock()
											writeLockHolderMNap[k] = ""
											writeLockHolderMNapMutex.Unlock()
										}

									}
									fmt.Println("Abort type 3")
									conn.Write([]byte("ABORTED"))
									breakLoop = true
									break
								}

								lockMapMutex.Lock()
								lockMap[lineSplit[1]][2] = 1
								lockMapMutex.Unlock()

								writeLockHolderMNapMutex.Lock()
								writeLockHolderMNap[lineSplit[1]] = transactionID
								writeLockHolderMNapMutex.Unlock()

								holdLockMap[lineSplit[1]] = 2

								if RLock == 1 {

									lockMapMutex.Lock()
									lockMap[lineSplit[1]][1] -= 1
									readLockHolderMNapMutex.Lock()
									delete(readLockHolderMNap, lineSplit[1])
									readLockHolderMNapMutex.Unlock()
									lockMapMutex.Unlock()

								}

								updateMap[lineSplit[1]] = lineSplit[2]
								logMap[server] = append(logMap[server], line)
								conn.Write([]byte("OK"))
								break
							} else {
								select {
									case <-abortChans[remoteIpindex]:
										abortWait = true
									default:
								}
							}
						}
					}

				case line := <-getChans[remoteIpindex]:
					abortWait := false
					lineSplit := strings.Split(line, " ")

					for {

						lockMapMutex.RLock()
						_, ok := lockMap[lineSplit[1]]
						lockMapMutex.RUnlock()

						if !ok {

							lockMapMutex.Lock()
							lockMap[lineSplit[1]] = map[int]int{}
							lockMapMutex.Unlock()

						}

						lockMapMutex.RLock()
						WLock := lockMap[lineSplit[1]][2]
						lockMapMutex.RUnlock()

						if WLock == 1 && holdLockMap[lineSplit[1]] != 2 {
							select {
								case <-abortChans[remoteIpindex]:
									abortWait = true
								default:
							}
							continue
						}

						if abortWait {
							for k, v := range holdLockMap {

								if v == 1 {
									lockMapMutex.Lock()
									lockMap[k][1] -= 1
									lockMapMutex.Unlock()
									readLockHolderMNapMutex.Lock()
									delete(readLockHolderMNap[k], transactionID)
									readLockHolderMNapMutex.Unlock()
								}

								if v == 2 {
									lockMapMutex.Lock()
									lockMap[k][2] = 0
									lockMapMutex.Unlock()
									writeLockHolderMNapMutex.Lock()
									writeLockHolderMNap[k] = ""
									writeLockHolderMNapMutex.Unlock()
								}

							}

							fmt.Println("Abort type 4")
							conn.Write([]byte("ABORTED"))
							breakLoop = true
							break
						}

						if holdLockMap[lineSplit[1]] == 0 {

							holdLockMap[lineSplit[1]] = 1
							lockMapMutex.Lock()
							lockMap[lineSplit[1]][1] += 1

							readLockHolderMNapMutex.RLock()
							_, ok := readLockHolderMNap[lineSplit[1]]
							readLockHolderMNapMutex.RUnlock()
							readLockHolderMNapMutex.Lock()
							if ! ok {
								readLockHolderMNap[lineSplit[1]] = map[string]bool{}
							}
							readLockHolderMNap[lineSplit[1]][transactionID] = true
							readLockHolderMNapMutex.Unlock()
							lockMapMutex.Unlock()
						}

						break
					}

					if(breakLoop) {
						break
					}

					fmt.Println("Reached here 2")
					server := strings.Split(lineSplit[1],".")[0]
					v, ok := updateMap[lineSplit[1]]
					fmt.Println("Reached here 3")

					if ok {

						msg := lineSplit[1] + " = " + v
						conn.Write([]byte(msg))

					} else {

						serverConnMapMutex.RLock()
						serverConn := serverConnMap[server]
						serverConnMapMutex.RUnlock()
						serverConn.Write([]byte(line+"\n"))
						fmt.Println("Send some bytes to the server")
						j, err := serverConn.Read(serverBuff)

						if err != nil {
							fmt.Printf("#Failed to connect to Server%v.", server)
							panic("Server" + server + " disconnected.")
						}

						if string(serverBuff[0:j]) == "NO" {
							// return NOT FOUND and abort the transaction.
							for k, v := range holdLockMap {
								if v == 1 {

									lockMapMutex.Lock()
									lockMap[k][1] -= 1
									lockMapMutex.Unlock()
									readLockHolderMNapMutex.Lock()
									delete(readLockHolderMNap[k], transactionID)
									readLockHolderMNapMutex.Unlock()
								}

								if v == 2 {
									lockMapMutex.Lock()
									lockMap[k][2] = 0
									lockMapMutex.Unlock()
									writeLockHolderMNapMutex.Lock()
									writeLockHolderMNap[k] = ""
									writeLockHolderMNapMutex.Unlock()
								}
							}

							conn.Write([]byte("NO FOUND"))
							conn.Write([]byte("This transaction is being aborted, please start a new one."))
							breakLoop = true
							break

						} else {
							fmt.Println("Found in the server")
							updateMap[lineSplit[1]] = string(serverBuff[0:j])
							msg := lineSplit[1] + " = " + string(serverBuff[0:j])
							conn.Write([]byte(msg))

						}
					}

			}
		}

	}

}

func main(){

	if len(os.Args) != 2 {
		fmt.Println("#Incorrect number of parameters")
		os.Exit(1)
	}

	for i := range abortChans {
		abortChans[i] = make(chan bool)
		beginChans[i] = make(chan bool)
		commitChans[i] = make(chan bool)
		setChans[i] = make(chan string)
		getChans[i] = make(chan string)
	}

	ip2ChannelIndexMap = make(map[string]int)
	ip2ChannelIndexMap = map[string]int{
		"172.22.156.52": 0,
		"172.22.158.52": 1,
		"172.22.94.61":  2,
		"172.22.156.53": 3,
		"172.22.158.53": 4,
		"172.22.94.62":  5,
		"172.22.156.54": 6,
		"172.22.158.54": 7,
		"172.22.94.63":  8,
		"172.22.156.55": 9,
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

	<-workingChan
}
