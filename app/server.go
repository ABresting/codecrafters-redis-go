package main

import (
	"fmt"
	"net"
	"io"
	"strings"
	"os"
	"strconv"
)

var store map[string]string

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")
	// store where redis stores the dict items
	store = make(map[string]string)

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		fmt.Println(err)
		os.Exit(1)
	}


	// we use the event loop to handle concurrent connections
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		defer conn.Close()
		// this event will handle each connection
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn){
	buf := make([]byte, 1024)
	for {
		read_input_len,err := conn.Read(buf)
		if err != nil {
			if err == io.EOF {
				break	
			} else {
				fmt.Println("error reading from client: ", err.Error())
				os.Exit(1)
			}
		}

		// 5th character reads the length of 1st input
		input_length, _ := strconv.Atoi(strings.Split(string(buf),"")[5])
		// As per the RESP specification from 8th character reads input
		input_command := string(buf)[8:8+input_length]

		if strings.ToLower(input_command) == "echo" {
			input_len := len(strings.Split(string(buf),"")[0:read_input_len])
			if input_len < 15 {
				processEchoCommand("", conn)	
			} else {
				to_echo := "\""+string(buf)[14+input_length:read_input_len-2]+"\""
				processEchoCommand(to_echo, conn)
			}
			continue
		} else if strings.ToLower(input_command) == "set" {
			key_len, _ := strconv.Atoi(strings.Split(string(buf),"")[14])
			key := string(buf)[17:17+key_len]

			value := string(buf)[23+key_len: read_input_len-2]
			// fmt.Println(string(buf)[0:read_input_len])
			processSetCommand(key,value,conn)
			continue
		} else if strings.ToLower(input_command) == "get" {
			key := string(buf)[17:read_input_len-2]
			
			processGetCommand(key,conn)
			continue
		}else {
			to_write := "+PONG\r\n"
			writeConnectionSuccess(to_write, conn)
		}
	}
}

func processEchoCommand(input string, conn net.Conn){
	to_write := "+"+input+"\r\n"
	writeConnectionSuccess(to_write, conn)
}

func processSetCommand(key string, value string, conn net.Conn){
	// Update the key-value pair in the global dict
	store[key] = value
	// fmt.Println(value)
	// write OK message to connection
	to_write := "+\""+"OK"+"\"\r\n"
	writeConnectionSuccess(to_write, conn)
}

func processGetCommand(key string, conn net.Conn){
	value := store[key]
	to_write := "+\""+value+"\"\r\n"
	writeConnectionSuccess(to_write, conn)
}

func writeConnectionSuccess(reply string, conn net.Conn){
	_, err := conn.Write([]byte(reply))
		if err != nil {
			fmt.Println("error writing to connection: ", err.Error())
			os.Exit(1)	
	}
}