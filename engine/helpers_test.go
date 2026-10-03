package engine

import (
	"fmt"

	"github.com/Diudiuseven/support-agent-handoff-go/agent"
	"github.com/Diudiuseven/support-agent-handoff-go/domain"
)

func agentID(i int) domain.AgentID { return domain.AgentID(fmt.Sprintf("ag%02d", i)) }

func domainSess(i int) domain.SessionID { return domain.SessionID(fmt.Sprintf("sess-%06d", i)) }

func regProfile(id domain.AgentID, cap int) agent.Profile {
	return agent.Profile{ID: id, Name: string(id), Queues: []domain.QueueName{"support"}, Capacity: cap}
}
