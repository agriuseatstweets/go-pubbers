package pubbers

type WriteResults struct {
	Sent int
	Written int
}

type QueueWriter interface {
	Publish (chan QueuedMessage, chan error) WriteResults
}

type QueuedMessage struct {
	Key []byte
	Value []byte
}
