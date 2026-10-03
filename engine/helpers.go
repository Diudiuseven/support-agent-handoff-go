package engine

import (
	"time"

	"github.com/Diudiuseven/support-agent-handoff-go/domain"
)

func timeZero() time.Time { return time.Time{} }

// ackedMsgID extracts the acknowledged message id from an EvAcked event.
// Ack events carry the id inside Msg (the other Message fields are
// unused on ack).
func ackedMsgID(ev domain.Event) string {
	if ev.Msg != nil {
		return ev.Msg.ID
	}
	return ""
}
