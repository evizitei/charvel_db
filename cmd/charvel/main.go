package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/charvel_db/pkg/sql"
)

func readCommand(cmdBuf *bufio.Reader) string {
	fmt.Print("charvelDB > ")
	command, err := cmdBuf.ReadString('\n')
	if err != nil {
		fmt.Println("Command Read Failure: ", err)
	}
	command = strings.Trim(command, "\n")
	return command
}

func processMetaCommand(command string) bool {
	if command == "$exit" || command == "$quit" {
		return true
	}
	fmt.Println("UNRECOGNIZED COMMAND: ", command)
	return false
}

func processCommand(command string) bool {
	if command[0] == '$' {
		return processMetaCommand(command)
	}
	statement, err := sql.Prepare(command)
	if err != nil {
		fmt.Println("Error in statement construction: ", err)
		return false
	}
	sql.Execute(statement)
	return false
}

func main() {
	fmt.Println("DB Terminal")
	flag.Parse()
	commandBuffer := bufio.NewReader(os.Stdin)
	for {
		command := readCommand(commandBuffer)
		quit := processCommand(command)
		if quit {
			break
		}
	}
	return
}
