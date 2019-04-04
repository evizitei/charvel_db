package sql

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

const idSize = 4
const usernameSize = 32
const emailSize = 255
const idOffset = 0
const usernameOffset = idOffset + idSize
const emailOffset = usernameOffset + usernameSize
const rowSize = idSize + usernameSize + emailSize

/*
Row is a wrapper for an instance of a relation in this database*/
type Row struct {
	ID       int32
	Username [usernameSize]byte
	Email    [emailSize]byte
}

/*
ToString produces a representation of a row
mostly for debugging purposes*/
func (r *Row) ToString() string {
	idString := strconv.Itoa(int(r.ID))
	nameBuilder := strings.Builder{}
	nameBuilder.Write(r.Username[0:usernameSize])
	emailBuilder := strings.Builder{}
	emailBuilder.Write(r.Email[0:emailSize])
	stringComponents := []string{"Row", idString, nameBuilder.String(), emailBuilder.String()}
	return strings.Join(stringComponents, " : ")
}

/*Serialize emits a byte stream that represents
the canonical way to compactly store a row*/
func (r *Row) Serialize() *bytes.Buffer {
	rowBytes := []byte{}
	rowBuffer := bytes.NewBuffer(rowBytes)
	binary.Write(rowBuffer, binary.BigEndian, r.ID)
	rowBuffer.Write(r.Username[0:usernameSize])
	rowBuffer.Write(r.Email[0:emailSize])
	return rowBuffer
}

/*
NewRow is a constructor that deals with the string
to fixed byte array issue*/
func NewRow(id int32, name string, email string) *Row {
	row := &Row{ID: id}
	for i, char := range name {
		if i >= usernameSize {
			break
		}
		row.Username[i] = byte(char)
	}
	for i, char := range email {
		if i >= emailSize {
			break
		}
		row.Email[i] = byte(char)
	}
	return row
}

/*DeserializeRow will re-hydrate
the byte array from the table store
to an actual GoLang row object*/
func DeserializeRow(rowBytes *[rowSize]byte) *Row {
	row := &Row{}
	var rowID int32
	rowBuffer := bytes.NewBuffer(rowBytes[0:rowSize])
	binary.Read(rowBuffer, binary.BigEndian, &rowID)
	row.ID = rowID
	usernameBytes := rowBuffer.Next(usernameSize)
	for i, byteVal := range usernameBytes {
		if i >= usernameSize {
			break
		}
		row.Username[i] = byteVal
	}
	emailBytes := rowBuffer.Next(emailSize)
	for i, byteVal := range emailBytes {
		if i >= emailSize {
			break
		}
		row.Email[i] = byteVal
	}
	return row
}

// Table Constants for in-memory representation
const pageSize = 4096
const tableMaxPages = 100
const rowsPerPage = pageSize / rowSize
const tableMaxRows = rowsPerPage * tableMaxPages

/*
TableAddress is a simple way to pass around
a specific memory address (offset really)
of a row*/
type TableAddress struct {
	PageNum    int
	ByteOffset int
}

/*
Table is the storage engine, managing how
records are serialized and deserialized
into bytes in memory*/
type Table struct {
	pages   [tableMaxPages][pageSize]byte
	numRows int
}

/*FetchAddress performs the conversion
from row index to an actual address in the
data pages with page offset and byte offset*/
func (t *Table) FetchAddress(rowNum int) TableAddress {
	pageNum := rowNum / rowsPerPage
	rowsIntoPage := rowNum % rowsPerPage
	byteOffset := rowsIntoPage * rowSize
	return TableAddress{
		PageNum:    pageNum,
		ByteOffset: byteOffset,
	}
}

/*
NextRowAddress computes where exactly to write the next
row to in memory.  Return values are page_int, */
func (t *Table) NextRowAddress() TableAddress {
	return t.FetchAddress(t.numRows)
}

/*Append provides a means to persist the new row as an entry
in the current table*/
func (t *Table) Append(row *Row) error {
	address := t.NextRowAddress()
	rowBytes := row.Serialize().Bytes()
	for i, byteVal := range rowBytes {
		if i >= rowSize {
			break
		}
		t.pages[address.PageNum][address.ByteOffset+i] = byteVal
	}
	t.numRows = t.numRows + 1
	return nil
}

/*FetchRow knows how to find an address
in the record pages and rehydrate
the row object that lives there*/
func (t *Table) FetchRow(address TableAddress) *Row {
	rowBytes := [rowSize]byte{}
	recByteOffset := 0
	for {
		if recByteOffset >= rowSize {
			break
		}
		rowBytes[recByteOffset] = t.pages[address.PageNum][address.ByteOffset+recByteOffset]
		recByteOffset++
	}
	return DeserializeRow(&rowBytes)
}

/*ToString is mostly for debugging
by dumping the current state of the table
to the output*/
func (t *Table) ToString() string {
	rowNum := 0
	builder := strings.Builder{}
	builder.WriteString("Row Count: ")
	builder.WriteString(strconv.Itoa(t.numRows))
	builder.WriteString("\n")
	for {
		if rowNum >= t.numRows {
			break
		}
		address := t.FetchAddress(rowNum)
		row := t.FetchRow(address)
		builder.WriteString(row.ToString())
		builder.WriteString("\n")
		rowNum++
	}
	return builder.String()
}

/*
Statement is a wrapper for
preparaing SQL commands and paasasing them to the executor */
type Statement struct {
	raw         string
	rowToInsert *Row
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

/*Engine keeps track of the memory state
so that the executing functions
can have contextual access to the
relevant  data structures*/
type Engine struct {
	usersTable *Table
}

/*NewEngine is a standard constructor.
It will take care of creating tablestate
for now*/
func NewEngine() *Engine {
	return &Engine{
		usersTable: &Table{},
	}
}

/*
Prepare is a kind of constructor for a
SQL statement */
func (e *Engine) Prepare(command string) (*Statement, error) {
	lowerCommand := strings.ToLower(command)
	statement := &Statement{
		raw: lowerCommand,
	}
	if len(lowerCommand) < 6 {
		return statement, errors.New("Unrecognized keyword at start of command: " + lowerCommand)
	}
	if statement.isInsert() {
		components := strings.Split(statement.raw, " ")
		rowID, err := strconv.Atoi(components[1])
		if err != nil {
			return statement, err
		}
		statement.rowToInsert = NewRow(int32(rowID), components[2], components[3])
	}
	return statement, nil
}

/*
Execute takes a statement and tries to apply
it to the dataset*/
func (e *Engine) Execute(statement *Statement) {
	if statement.isSelect() {
		fmt.Println("Executing select...")
	} else if statement.isInsert() {
		fmt.Println("Inserting this row!")
		fmt.Println(statement.rowToInsert.ToString())
		e.usersTable.Append(statement.rowToInsert)
	} else if statement.isUpdate() {
		fmt.Println("Executing update...")
	} else if statement.isDelete() {
		fmt.Println("Executing delete")
	} else {
		fmt.Println("Unrecognized keyword at beginning of statement: ", statement.ToString())
	}
}

/*TableStateString will return a stringified
version of the whole table, useful for
debugging current state*/
func (e *Engine) TableStateString() string {
	return e.usersTable.ToString()
}
