package engine

// ChannelInbox is a built-in Inbox backed by a bounded buffered channel.
// A handler drains C at its own pace; when the buffer is full
// TryDeliver returns false rather than blocking, isolating slow
// handlers from the rest of the system.
type ChannelInbox struct {
	C chan Delivery
}

// NewChannelInbox creates an inbox with the given buffer size.
func NewChannelInbox(buffer int) *ChannelInbox {
	if buffer <= 0 {
		buffer = 1
	}
	return &ChannelInbox{C: make(chan Delivery, buffer)}
}

// TryDeliver implements Inbox.
func (c *ChannelInbox) TryDeliver(d Delivery) bool {
	select {
	case c.C <- d:
		return true
	default:
		return false
	}
}

// Recv blocks until one delivery is available or done is closed.
func (c *ChannelInbox) Recv(done <-chan struct{}) (Delivery, bool) {
	select {
	case d := <-c.C:
		return d, true
	case <-done:
		return Delivery{}, false
	}
}
