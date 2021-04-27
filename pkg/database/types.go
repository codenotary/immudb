package database

// DatabaseList DatabaseList interface
type DatabaseList interface {
	Append(database DB)
	GetByIndex(index int64) DB
	GetByName(string) DB
	GetId(dbname string) int64
	Length() int
}
