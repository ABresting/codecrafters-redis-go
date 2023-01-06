package main

import (
	"fmt"
	// "io"
	"net"
	"os"
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// Uncomment this block to pass the first stage
	
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		fmt.Println(err)
		os.Exit(1)
	}
	conn, err := l.Accept()
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
		os.Exit(1)
	}

	defer conn.Close()

	buf := make([]byte, 1024)
	for {

		_,err := conn.Read(buf)
		if err != nil {			
			fmt.Println("error reading from client: ", err.Error())
			os.Exit(1)
		}


		// we have hardcoded the reply for now
		_, err = conn.Write([]byte("+PONG\r\n"))
		if err != nil {
			fmt.Println("error writing to connection: ", err.Error())
			os.Exit(1)	
		}
	}

}
