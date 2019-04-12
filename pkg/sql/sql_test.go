package sql

import (
	"fmt"
	"os"
	"strings"
	"testing"
)

const TestFileName = "./test_file.db"

func ClearTestFile() {
	_, err := os.Stat(TestFileName)
	if os.IsNotExist(err) {
		return
	}
	os.Remove(TestFileName)
}

func WriteRecords(count int, table *Table) error {
	for i := 1; i <= count; i++ {
		username := fmt.Sprintf("User %d", i)
		email := fmt.Sprintf("user.%d@test.com", i)
		row := NewRow(int32(i), username, email)
		err := table.Append(row)
		if err != nil {
			return err
		}
	}
	return nil
}

func TestPagerFileStore(t *testing.T) {
	ClearTestFile()
	pager := NewPager(TestFileName)
	row := NewRow(42, "testUser", "test@test.com")
	rowBytes := row.Serialize()
	address := TableAddress{PageNum: 0, ByteOffset: 0}
	pager.Write(address, rowBytes.Bytes())
	pager.Flush(0, rowSize)
	pager.Close()
	pager = NewPager(TestFileName)
	readBytes := pager.Read(address)
	readRow := DeserializeRow(&readBytes)
	if readRow.ID != row.ID {
		t.Error("pager should persist to disk")
	}
	ClearTestFile()
}

func TestReadingRowcount(t *testing.T) {
	ClearTestFile()
	table := NewTable(TestFileName)
	if table.numRows != 0 {
		t.Error("Blank file should count as 0 rows")
	}
	rowCount := 200
	err := WriteRecords(rowCount, table)
	if err != nil {
		t.Error("Failed to append records", err)
	}
	if table.numRows != rowCount {
		t.Error("Did not persist correct row count: ", table.numRows)
	}
	table.Close()
	table = NewTable(TestFileName)
	if table.numRows != rowCount {
		t.Error("Did not recover rowcount from file: ", table.numRows)
	}
	ClearTestFile()
}

func TestAddressFetching(t *testing.T) {
	ClearTestFile()
	table := NewTable(TestFileName)
	row1Address := table.FetchAddress(0)
	if row1Address.PageNum != 0 || row1Address.ByteOffset != 0 {
		t.Error("Row Offset for addressing is off: ", row1Address)
	}
}

func TestTableString(t *testing.T) {
	ClearTestFile()
	table := NewTable(TestFileName)
	err := WriteRecords(5, table)
	if err != nil {
		t.Error("Failed to append records", err)
	}
	tableState := table.ToString()
	if !strings.Contains(tableState, "User 2") {
		t.Error("table should have 5 records: ", tableState)
	}
	if strings.Contains(tableState, "User 6") {
		t.Error("Table should have cut off at 5", tableState)
	}
}

func TestReadingFromOtherPages(t *testing.T) {
	ClearTestFile()
	table := NewTable(TestFileName)
	err := WriteRecords(30, table)
	if err != nil {
		t.Error("Failed to append records", err)
	}
	table.Close()
	table = NewTable(TestFileName)
	row1 := table.FetchRow(TableAddress{PageNum: 0, ByteOffset: 0})
	if row1.ID != 1 {
		t.Error("loaded wrong row", row1)
	}
	row16Address := table.FetchAddress(15)
	if row16Address.PageNum != 1 {
		t.Error("Address matching should have hit next page")
	}
	row16 := table.FetchRow(row16Address)
	for i := 0; i <= 6; i++ {
		if row16.Username[i] != []byte("User 16")[i] {
			t.Error("Loaded wrong row for 16: ", string(row16.Username[0:32]))
		}
	}
}
