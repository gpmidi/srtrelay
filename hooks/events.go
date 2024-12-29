package hooks

type Event interface {
	// Type returns the event type
	Type() WebHookType
}

type EventImpl struct {
	t WebHookType
}

func NewEvent() Event {

}

func (e EventImpl) Type() WebHookType {
	return e.t
}
