package hooks

import "net/url"

const (
	DefaultOk      = true
	DefaultCode    = 200
	DefaultMessage = ""
)

type Result interface {
	Block() bool
	Ok() bool
	ResultMessage() string
	RedirectURL() *url.URL
	HasRedirectURL() bool
	Code() int
}

type ResultImpl struct {
	ok          bool
	message     string
	code        int
	redirectURL *url.URL
}

func (r ResultImpl) RedirectURL() *url.URL {
	return r.redirectURL
}

func (r ResultImpl) HasRedirectURL() bool {
	// TODO: More tests here for validity
	return r.redirectURL != nil
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

func NewResult(ok bool, message string, code int, redirectURL *url.URL) Result {
	return ResultImpl{
		ok:          ok,
		message:     message,
		code:        code,
		redirectURL: redirectURL,
	}
}

func NewDefaultResult() Result {
	return NewResult(DefaultOk, DefaultMessage, DefaultCode, nil)
}
