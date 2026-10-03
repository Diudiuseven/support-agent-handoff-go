package engine

import (
	"time"

	"github.com/Diudiuseven/support-agent-handoff-go/domain"
	"github.com/Diudiuseven/support-agent-handoff-go/queue"
)

func queueEntry(id domain.SessionID, qn domain.QueueName, since, deadline time.Time) queue.Entry {
	return queue.Entry{SessionID: id, Queue: qn, EnqueuedAt: since, Deadline: deadline}
}
