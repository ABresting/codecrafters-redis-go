package main

import (
	"fmt"
	"net"
	"io"
	"strings"
	"os"
	"strconv"
	"time"
)

type val_data struct{
	val string
	expireTime int64
	setTime time.Time
	expiryEnabled bool
} 

var store map[string]val_data

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")
	// store where redis stores the dict items
	store = make(map[string]val_data)

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
		// As per the RESP specification from 8th character reads input

		input_command := strings.Split(string(buf[0:read_input_len]), "\r\n")
		command := input_command[2]
		fmt.Println(int64(time.Millisecond))
		if strings.ToLower(command) == "echo" {
			if len(input_command) < 4 {
				processEchoCommand("", conn)	
			} else {
				to_echo := input_command[4]
				processEchoCommand(to_echo, conn)
			}
			continue
		} else if strings.ToLower(command) == "set" {
			expire_option_given := false
			expiry_value := 0
			if len(input_command) < 7 {
				to_write := "-ERR unknown command \r\n"
				writeConnection(to_write, conn)
				continue
			}

			key := input_command[4]
			value := input_command[6]
			// PX (expiry) option is provided or not
			if len(input_command) > 8 {
				if input_command[8] == "PX" {
					expire_option_given = true
					expiry_value, _ = strconv.Atoi(input_command[10])
				} else {
					to_write := "-ERR unknown command \r\n"
					writeConnection(to_write, conn)
					continue
				}
			}

			processSetCommand(key,value,expire_option_given, int64(expiry_value),conn)
			continue
		} else if strings.ToLower(command) == "get" {
			key := input_command[4]
			
			processGetCommand(key,conn)
			continue
		}else if strings.ToLower(command) == "ping"{
			to_write := "+PONG\r\n"
			writeConnection(to_write, conn)
			continue
		} else {
			to_write := "-ERR unknown command \r\n"
			writeConnection(to_write, conn)
			continue
		}
	}
}

func processEchoCommand(input string, conn net.Conn){
	to_write := "+"+input+"\r\n"
	writeConnection(to_write, conn)
}

func processSetCommand(key string, value string, expiry_enabled bool, expiry_value int64, conn net.Conn){
	// Update the key-value pair in the global dict
	store[key] = val_data{
		val: value,
		expireTime: expiry_value,
		setTime: time.Now(),
		expiryEnabled: expiry_enabled,
	}
	// write OK message to connection
	to_write := "+OK"+"\r\n"
	writeConnection(to_write, conn)
}

func processGetCommand(key string, conn net.Conn){
	value := store[key].val
	to_write := "+"+value+"\r\n"

	if store[key].expiryEnabled {
		start_time := (store[key].setTime).UnixNano() / int64(time.Millisecond)
		end_time := (time.Now()).UnixNano() / int64(time.Millisecond)
		if end_time - start_time > store[key].expireTime {
			to_write = "$-1\r\n"
		}
	}

	writeConnection(to_write, conn)
}

func writeConnection(reply string, conn net.Conn){
	_, err := conn.Write([]byte(reply))
		if err != nil {
			fmt.Println("error writing to connection: ", err.Error())
			os.Exit(1)	
	}
}
