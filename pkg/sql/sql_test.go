package sql

import "testing"

func TestAssert(t *testing.T) {
	if 1 != 1 {
		t.Error("Equality is obviously right")
	}
}
