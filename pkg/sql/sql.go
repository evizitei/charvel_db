package sql

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
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
const pageConstraintSize = 4096
const tableMaxPages = 100
const rowsPerPage = pageConstraintSize / rowSize
const actualPageSize = rowsPerPage * rowSize
const tableMaxRows = rowsPerPage * tableMaxPages
const dbFile = "./charvel.db"

/*Pager is an abstraction layer for the table.
It keeps pages cached in memory, and also knows
how to read and write them from disk. This
way the table can ask for a given page, and the
Pager will take care of figuring out whether
it's already in memory or not. */
type Pager struct {
	pageCache    *[tableMaxPages][actualPageSize]byte
	cacheIndex   *map[int]bool
	file         *os.File
	fileReadSize int64
}

/*NewPager is the constructor for a pager
to take care of allocating system resources for a file
and memory space for a page cache*/
func NewPager(dbFileName string) *Pager {
	if dbFileName == "default" {
		dbFileName = dbFile
	}
	fd, err := os.OpenFile(dbFileName, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		log.Fatal("Could not open db file: ", err)
	}
	fInfo, err := fd.Stat()
	if err != nil {
		log.Fatal("Could not get db file info: ", err)
	}
	cIndex := make(map[int]bool)
	return &Pager{
		pageCache:    &[tableMaxPages][actualPageSize]byte{},
		cacheIndex:   &cIndex,
		file:         fd,
		fileReadSize: fInfo.Size(),
	}
}

/*Flush one page to disk*/
func (p *Pager) Flush(pageIdx int, writeSize int) {
	pageOffset := int64(pageIdx * actualPageSize)
	p.file.Seek(pageOffset, 0)
	pageBytes := p.pageCache[pageIdx][0:writeSize]
	p.file.Write(pageBytes)
}

/*Close makes the underlying file close
cleanly so we don't end up leaving
the file un-flushed*/
func (p *Pager) Close() {
	p.file.Close()
}

/*Write sends the bytes for a record to a specific address,
first making sure the page for the target address is loaded into memory*/
func (p *Pager) Write(address TableAddress, rowBytes []byte) {
	p.checkPage(address.PageNum)
	for i, byteVal := range rowBytes {
		if i >= rowSize {
			break
		}
		p.pageCache[address.PageNum][address.ByteOffset+i] = byteVal
	}
}

func (p *Pager) cachePage(pageNum int) {
	p.file.Seek(int64(pageNum*actualPageSize), 0)
	pageBuffer := make([]byte, actualPageSize)
	_, err := p.file.Read(pageBuffer)
	if err != nil && err != io.EOF {
		log.Fatal("Failed to cache page ", err)
	}
	for i, byteVal := range pageBuffer {
		p.pageCache[pageNum][i] = byteVal
	}

}

func (p *Pager) checkPage(pageNum int) {
	loaded, ok := (*p.cacheIndex)[pageNum]
	if !ok || !loaded {
		p.cachePage(pageNum)
		(*p.cacheIndex)[pageNum] = true
	}
}

func (p *Pager) Read(address TableAddress) [rowSize]byte {
	p.checkPage(address.PageNum)
	rowBytes := [rowSize]byte{}
	recByteOffset := 0
	for {
		if recByteOffset >= rowSize {
			break
		}
		rowBytes[recByteOffset] = p.pageCache[address.PageNum][address.ByteOffset+recByteOffset]
		recByteOffset++
	}
	return rowBytes
}

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
	pager   *Pager
	numRows int
}

/*NewTable is a constructor for the table object.
While booting, it will evaluate the table size
from the file size on disk.*/
func NewTable(dbFileName string) *Table {
	pager := NewPager(dbFileName)
	rowCount := pager.fileReadSize / rowSize
	/*fmt.Println("TABLE LOAD: read size", pager.fileReadSize)
	fmt.Println("TABLE LOAD: row size", rowSize)
	fmt.Println("TABLE LOAD: row count", rowCount)*/
	table := &Table{pager: pager, numRows: int(rowCount)}
	return table
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
	t.pager.Write(address, rowBytes)
	t.numRows = t.numRows + 1
	return nil
}

/*FetchRow knows how to find an address
in the record pages and rehydrate
the row object that lives there*/
func (t *Table) FetchRow(address TableAddress) *Row {
	rowBytes := t.pager.Read(address)
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
	//cur := NewCursor(t, "iterator")

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

/*Close flushes the whole table to disk
and closes the db file*/
func (t *Table) Close() {
	pageCount := t.numRows / rowsPerPage
	for i := 0; i < pageCount; i++ {
		t.pager.Flush(i, actualPageSize)
	}
	extraRows := t.numRows % rowsPerPage
	t.pager.Flush(pageCount, extraRows*rowSize)
	t.pager.Close()
}

/*Cursor is a way to hold an offset in a table
so you can scan forward or backward*/
type Cursor struct {
	Table    *Table
	rowIndex int
}

/*GetAddress returns the address to read/write
from on the underlying table.*/
func (c *Cursor) GetAddress() TableAddress {
	return c.Table.FetchAddress(c.rowIndex)
}

/*Advance just moves the cursor forward through
the table, returning true if we're still
within the table*/
func (c *Cursor) Advance() bool {
	c.rowIndex++
	return !c.BeyondTable()
}

/*BeyondTable is true if the offset is outside
the range of rows for which we have real data*/
func (c *Cursor) BeyondTable() bool {
	return c.rowIndex >= c.Table.numRows || c.rowIndex < 0
}

/*NewCursor sets up the offset at the beginning
or end of the table. The iterator mode
offsets to -1 because it expects a for loop
to call Advance before it's accessed anything*/
func NewCursor(t *Table, mode string) *Cursor {
	cursor := &Cursor{Table: t}
	if mode == "start" {
		cursor.rowIndex = 0
	} else if mode == "iterator" {
		cursor.rowIndex = -1
	} else if mode == "end" {
		cursor.rowIndex = t.numRows - 1
	}
	return cursor
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
	return &Engine{usersTable: NewTable("default")}
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
		fmt.Println(e.usersTable.ToString())
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
