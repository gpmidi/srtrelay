package hooks

import (
	"context"
	"net/url"
)

const (
	OnWebhookName   = "OnConnect"
	OnWebhookCallID = "connect" // https://github.com/arut/nginx-rtmp-module/wiki/Directives#on_connect
)

type OnConnectWebhook struct {
	BaseHook
}

func (w *OnConnectWebhook) OnEvent(ctx context.Context, event Event) (result Result, err error) {
	vals := event.Values()

	return w.doCallback(ctx, url.Values{
		CallbackKeyCall:               {OnWebhookCallID},
		CallbackKeyApp:                {w.Config().Application},
		CallbackKeyName:               {vals[ValKeyName]},
		CallbackKeyUsername:           {vals[ValKeyUsername]},
		w.Config().GetPasswordParam(): {vals[ValKeyAuth]},
	})
}

func init() {
	// Register in list of hooks
	if err := RegisterWebhook(&OnConnectWebhook{
		BaseHook: newBaseHook(OnWebhookName, WebhookTypeConnect),
	}); err != nil {
		panic(err)
	}
}
