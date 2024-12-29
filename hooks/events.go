package hooks

type ValueKey int

// Shared keys for various Values()
const (
	ValKeyAuth ValueKey = iota // aka password
	ValKeyName
	ValKeyUsername
	ValKeyAddr
	ValKeyStreamKey
	ValKeyMode

	CallbackKeyCall     = "call"
	CallbackKeyApp      = "app"
	CallbackKeyName     = "name"
	CallbackKeyUsername = "username"
	//CallbackKey         = ""
	//CallbackKey         = ""
	//CallbackKey         = ""
	//CallbackKey         = ""
)

type Event interface {
	// Type returns the event type
	Type() WebHookType
	Values() map[ValueKey]string
}

type EventImpl struct {
	t    WebHookType
	data map[ValueKey]string
}

func NewEvent(hookType WebHookType, data map[ValueKey]string) Event {
	return EventImpl{
		t:    hookType,
		data: data,
	}
}

func (e EventImpl) Type() WebHookType {
	return e.t
}

func (e EventImpl) Values() map[ValueKey]string {
	return e.data
}
