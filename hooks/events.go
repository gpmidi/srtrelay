package hooks

type Event interface {
	// Type returns the event type
	Type() WebHookType
}

type EventImpl struct {
	t WebHookType
}

func NewEvent(hookType WebHookType) Event {
	return EventImpl{
		t: hookType,
	}
}

func (e EventImpl) Type() WebHookType {
	return e.t
}
