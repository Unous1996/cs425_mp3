package main

import (
	"fmt"
	"net"
	"os"
	"strings"
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
	lockMap map[string]int
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
	lockMap = make(map[string]int)
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
				fmt.Println("#Begin transaction.")
				break;
			}

		}

		updateMap := make(map[string]string)
		logMap := make(map[string]string)

		for {

			j, err := conn.Read(buff)
			flag := checkErr(err)
			if flag == 0 {
				break
			}

			line := string(buff[0:j])
			line_split := strings.Split(line, " ")


			if line_split[0] == "ABORT" {

				for k := range lockMap {
					lockMap[k] = 0
				}

				conn.Write([]byte("ABORTED"))
				break
			}

			if line_split[0] == "COMMIT" {

				for k, v := range logMap {
					serverConn := serverConnMap[k]
					serverConn.Write([]byte(v))
				}

				for k := range lockMap {
					lockMap[k] = 0
				}

				conn.Write([]byte("COMMIT OK"))
				break
			}

			if line_split[0] == "SET" {

				server := strings.Split(line_split[1],".")[0]


				for {
					lock := lockMap[line_split[1]]
					if lock != 0 {
						continue
					}

					lockMap[line_split[1]] = 2
					updateMap[line_split[1]] = line_split[2]
					logMap[server] = line
					conn.Write([]byte("OK"))
					break
				}

			}

			if line_split[0] == "GET" {

				server := strings.Split(line_split[1],".")[0]
				v, ok := updateMap[line_split[1]]

				if ok {

					for {
						lock := lockMap[line_split[1]]
						if lock == 2 {
							continue
						}
						lockMap[line_split[1]] = 1
						msg := line_split[1] + " = " + v
						conn.Write([]byte(msg))
						break
					}

				}else {

					serverConn := serverConnMap[server]
					serverConn.Write([]byte(line))
					j, err := serverConn.Read(buff)

					if err != nil {
						fmt.Printf("#Failed to connect to Server%v.", server)
						panic("Server" + server + " disconnected.")
					}

					if string(buff[0:j]) == "NO" {
						// return NOT FOUND and abort the transaction.
						conn.Write([]byte("NO FOUND"))
						conn.Write([]byte("This transaction is being aborted, please start a new one."))
						break
					}else {

						for {
							lock := lockMap[line_split[1]]
							if lock == 2 {
								continue
							}
							lockMap[line_split[1]] = 1
							updateMap[line_split[1]] = string(buff[0:j])
							msg := line_split[1] + " = " + string(buff[0:j])
							conn.Write([]byte(msg))
							break
						}

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
