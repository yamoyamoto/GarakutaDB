package parser

type Parser interface {
	Parse(SqlString string) (Stmt, error)
}

// TODO: implement
type Stmt interface {
}
