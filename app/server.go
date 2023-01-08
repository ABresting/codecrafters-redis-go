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
		input_length, _ := strconv.Atoi(strings.Split(string(buf),"")[5])
		// As per the RESP specification from 8th character reads input
		input_command := string(buf)[8:8+input_length]

		if strings.ToLower(input_command) == "echo" {
			input_len := len(strings.Split(string(buf),"")[0:read_input_len])
			if input_len < 15 {
				processEchoCommand("", conn)	
			} else {
				to_echo := string(buf)[14+input_length:read_input_len-2]
				processEchoCommand(to_echo, conn)
			}
			continue
		} else if strings.ToLower(input_command) == "set" {
			expiry_enabled := false
			expiry_value := 0
			// example "*3\r\n$3\r\nset\r\n$5\r\nhello\r\n$5\r\nworld\r\n"
			key_len_string := readValueFromRESP(string((buf)[14:read_input_len]))
			key_len, _ := strconv.Atoi(readValueFromRESP(string((buf)[14:read_input_len])))
			key := readValueFromRESP(string((buf)[16+len(key_len_string):read_input_len]))

			value_len_string := readValueFromRESP(string((buf)[(19+len(key_len_string)+key_len):read_input_len]))
			value_len,_ := strconv.Atoi(value_len_string)
			fmt.Println(value_len)
			value := string(buf)[23+key_len: 23+key_len+value_len]
			
			num_options, _ := strconv.Atoi(strings.Split(string(buf),"")[1])
			// PX (expiry) option is provided or not
			if num_options > 3{
				expiry_enabled = true
				expiry_value, _ = strconv.Atoi(string(buf)[37+key_len+value_len: read_input_len -2])
			}

			processSetCommand(key,value,expiry_enabled, int64(expiry_value),conn)
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

func processSetCommand(key string, value string, expiry_enabled bool, expiry_value int64, conn net.Conn){
	// Update the key-value pair in the global dict
	store[key] = val_data{
		val: value,
		expireTime: expiry_value,
		setTime: time.Now(),
		expiryEnabled: expiry_enabled,
	}
	// write OK message to connection
	to_write := "+\""+"OK"+"\"\r\n"
	writeConnectionSuccess(to_write, conn)
}

func processGetCommand(key string, conn net.Conn){
	value := store[key].val
	to_write := "+\""+value+"\"\r\n"

	if store[key].expiryEnabled {
		start_time := (store[key].setTime).UnixNano() / int64(time.Millisecond)
		end_time := (time.Now()).UnixNano() / int64(time.Millisecond)
		if end_time - start_time > store[key].expireTime {
			to_write = "$-1\r\n"
		}
	}

	writeConnectionSuccess(to_write, conn)
}

func writeConnectionSuccess(reply string, conn net.Conn){
	_, err := conn.Write([]byte(reply))
		if err != nil {
			fmt.Println("error writing to connection: ", err.Error())
			os.Exit(1)	
	}
}

func readValueFromRESP(str string) string {
	value := ""
	for i:=0; i<len(str);i++ {
		if string(str[i]) != "\r" {
			value = value + string(str[i])
		} else {
			break
		}
	}

	return value
}