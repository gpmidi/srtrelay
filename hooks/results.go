package hooks

const (
	DefaultOk      = true
	DefaultCode    = 200
	DefaultMessage = ""
)

type Result interface {
	Block() bool
	Ok() bool
	ResultMessage() string
	Code() int
}

type ResultImpl struct {
	ok      bool
	message string
	code    int
}

func (r ResultImpl) Ok() bool {
	return r.ok
}

func (r ResultImpl) ResultMessage() string {
	return r.message
}

func (r ResultImpl) Code() int {
	return r.code
}

func (r ResultImpl) Block() bool {
	return !r.ok
}

func NewResult(ok bool, message string, code int) Result {
	return ResultImpl{
		ok:      ok,
		message: message,
		code:    code,
	}
}

func NewDefaultResult() Result {
	return NewResult(DefaultOk, DefaultMessage, DefaultCode)
}
