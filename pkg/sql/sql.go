package sql

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

/*
Prepare is a kind of constructor for a
SQL statement */
func Prepare(command string) *Statement {
	return &Statement{
		raw: command,
	}
}
