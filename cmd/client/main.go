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
			workingChan <- true
			break
		}

		receviedStringSpilt := strings.Split(string(buff[0:j]), "\n");
		for line := range receviedStringSpilt {
			fmt.Println(line)
		}
	}
}

func chanInit(){
	workingChan = make(chan bool)
}

func initialize(){
	chanInit()
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
		coordinatorConnection, err := net.DialTCP("tcp", nil, tcpAdd)
		if err != nil {
			fmt.Println("#Failed to connect to the coordinator")
			continue
		}

		defer coordinatorConnection.Close()
		go readMessage(coordinatorConnection)
		break;
	}

	<-workingChan
	fmt.Println("Client Closed")
}