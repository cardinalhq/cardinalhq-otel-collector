package boxer

type BufferRecord struct {
	Scope    string
	Interval int64
	Contents []byte
}

type ForEachFunc func(record *BufferRecord) (keepGoing bool, err error)

type Buffer interface {
	Write(data *BufferRecord) error
	GetScopes(interval int64) (scopes []string, err error)
	GetIntervals() (intervals []int64, err error)
	ForEach(interval int64, scope string, f ForEachFunc) error
	CloseInterval(interval int64, scope string) error
	Shutdown() error
}
