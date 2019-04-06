package sql

import "testing"

func TestPagerFileStore(t *testing.T) {
	testFileName := "./test_file.db"
	pager := NewPager(testFileName)
	row := NewRow(42, "testUser", "test@test.com")
	rowBytes := row.Serialize()
	address := TableAddress{PageNum: 0, ByteOffset: 0}
	pager.Write(address, rowBytes.Bytes())
	pager.Flush(0, rowSize)
	pager.Close()
	pager = NewPager(testFileName)
	readBytes := pager.Read(address)
	readRow := DeserializeRow(&readBytes)
	if readRow.ID != row.ID {
		t.Error("pager should persist to disk")
	}
}
