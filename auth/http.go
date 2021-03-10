package auth

import (
	"errors"
	"log"
	"net/http"
	"net/url"
	"time"

	"github.com/voc/srtrelay/stream"
)

var ErrRedirect = errors.New("redirect")

type httpAuth struct {
	config HTTPAuthConfig
	client *http.Client
}

type HTTPAuthConfig struct {
	URL           string
	Application   string
	Timeout       time.Duration // Timeout for Auth request
	PasswordParam string        // POST Parameter containing stream passphrase
}

// NewHttpAuth creates an Authenticator with a HTTP backend
func NewHTTPAuth(config HTTPAuthConfig) *httpAuth {
	return &httpAuth{
		config: config,
		client: &http.Client{
			Timeout: config.Timeout,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return ErrRedirect
			},
		},
	}
}

// Implement Authenticator

// Authenticate sends form-data in a POST-request to the configured url.
// If the response code is 2xx the publish/play is allowed, otherwise it is denied.
// This should be compatible with nginx-rtmps on_play/on_publish directives.
// https://github.com/arut/nginx-rtmp-module/wiki/Directives#on_play
func (h *httpAuth) Authenticate(streamid stream.StreamID) (bool, stream.StreamID) {
	response, err := h.client.PostForm(h.config.URL, url.Values{
		"call":                 {streamid.Mode().String()},
		"app":                  {h.config.Application},
		"name":                 {streamid.Name()},
		h.config.PasswordParam: {streamid.Password()},
	})
	if err.Error() == ErrRedirect.Error() {

	} else if err != nil {
		log.Println("http-auth:", err)
		return false, streamid
	}
	defer response.Body.Close()

	// Redirects
	if response.StatusCode == http.StatusPermanentRedirect || response.StatusCode == http.StatusTemporaryRedirect {
		loc, err := response.Location()
		if err != nil {
			return false, streamid
		}
		return true, stream.NewStreamID(streamid.Mode(), loc.Path, streamid.Password())
	}

	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return false, streamid
	}

	return true, streamid
}
