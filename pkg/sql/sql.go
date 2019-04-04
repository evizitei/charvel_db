package sql

import (
	"fmt"
	"strings"
)

/*
Statement is a wrapper for
preparaing SQL commands and paasasing them to the executor */
type Statement struct {
	raw string
}

/*
ToString is a convenience for printing the state
of the statement*/
func (s *Statement) ToString() string {
	return s.raw
}

func (s *Statement) isSelect() bool {
	return s.raw[0:6] == "select"
}

func (s *Statement) isInsert() bool {
	return s.raw[0:6] == "insert"
}

func (s *Statement) isUpdate() bool {
	return s.raw[0:6] == "update"
}

func (s *Statement) isDelete() bool {
	return s.raw[0:6] == "delete"
}

/*
Prepare is a kind of constructor for a
SQL statement */
func Prepare(command string) *Statement {
	return &Statement{
		raw: strings.ToLower(command),
	}
}

/*
Execute takes a statement and tries to apply
it to the dataset*/
func Execute(statement *Statement) {
	if statement.isSelect() {
		fmt.Println("Executing select...")
	} else if statement.isInsert() {
		fmt.Println("Executing insert...")
	} else if statement.isUpdate() {
		fmt.Println("Executing update...")
	} else if statement.isDelete() {
		fmt.Println("Executing delete")
	} else {
		fmt.Println("Unrecognized keyword at beginning of statement: ", statement.ToString())
	}
}
