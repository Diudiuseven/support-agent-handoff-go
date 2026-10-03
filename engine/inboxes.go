package engine

// NewDefaultInboxes builds the standard inbox map: one bot inbox plus
// one bounded ChannelInbox per given agent id, each with buffer
// capacity opts.DeliveryBuffer.
func NewDefaultInboxes(buffer int, agentIDs ...string) (map[string]Inbox, *ChannelInbox, map[string]*ChannelInbox) {
	if buffer <= 0 {
		buffer = 64
	}
	inboxes := make(map[string]Inbox, len(agentIDs)+1)
	bot := NewChannelInbox(buffer)
	inboxes[InboxBot] = bot
	agents := make(map[string]*ChannelInbox, len(agentIDs))
	for _, id := range agentIDs {
		ib := NewChannelInbox(buffer)
		agents[id] = ib
		inboxes[InboxAgentPrefix+id] = ib
	}
	return inboxes, bot, agents
}
