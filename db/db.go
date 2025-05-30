package db

type DBType int

const (
	SQLITE = iota
)

type DB interface {
	Type() DBType
}
