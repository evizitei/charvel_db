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

func processMetaCommand(command string, engine *sql.Engine) bool {
	if command == "$exit" || command == "$quit" {
		return true
	} else if command == "$print" {
		fmt.Println(engine.TableStateString())
		return false
	}
	fmt.Println("UNRECOGNIZED COMMAND: ", command)
	return false
}

func processCommand(command string, engine *sql.Engine) bool {
	if command[0] == '$' {
		return processMetaCommand(command, engine)
	}
	statement, err := engine.Prepare(command)
	if err != nil {
		fmt.Println("Error in statement construction: ", err)
		return false
	}
	engine.Execute(statement)
	return false
}

func main() {
	fmt.Println("DB Terminal")
	flag.Parse()
	sqlEngine := sql.NewEngine()
	commandBuffer := bufio.NewReader(os.Stdin)
	for {
		command := readCommand(commandBuffer)
		quit := processCommand(command, sqlEngine)
		if quit {
			break
		}
	}
	return
}
