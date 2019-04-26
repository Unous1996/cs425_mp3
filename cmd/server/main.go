package main

import (
	"fmt"
	"net"
	"os"
	"strings"
)

var (
	coordinatorAddresses = "10.105.210.190"
	coordinatorPort = "6000"
)

var (
	workingChan chan bool
)

var (
	balance map[string]string
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
	buff := make([]byte, 10000)
	for {
		j, err := conn.Read(buff)
		flag := checkErr(err)
		if flag == 0 {
			break
		}

		receviedStringSpilt := strings.Split(string(buff[0:j]), "\n");
		for _, line := range receviedStringSpilt {
			line_split := strings.Split(line, " ")
			if(line_split[0] == "SET"){
				object := strings.Split(line_split[1],".")[1]
				balance[object] = line_split[2]
				continue
			}

			if(line_split[0] == "GET") {
				object := strings.Split(line_split[1],".")[1]
				_, ok := balance[object]
				var replyGet string
				if !ok {
					replyGet = "NO"
				} else {
					replyGet = balance[object]
				}
				coordinatorConnection.Write([]byte(replyGet))
			}
		}
	}
}

func chanInit(){
	workingChan = make(chan bool)
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

	coordinatorHost := coordinatorAddresses + coordinatorPort
	initialize()
	for{
		tcpAdd, _ := net.ResolveTCPAddr("tcp", coordinatorHost)
		var err error
		coordinatorConnection, err = net.DialTCP("tcp", nil, tcpAdd)
		if err != nil {
			fmt.Println("#Failed to connect to the coordinator")
			continue
		}

		defer coordinatorConnection.Close()
		go readMessage(coordinatorConnection)
	}

	fmt.Println("Server Closed")
	<-workingChan
}