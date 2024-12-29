package hooks

import "context"

type OnConnectWebhook struct {
	BaseHook
}

func (w *OnConnectWebhook) OnEvent(ctx context.Context, event Event) (result Result, err error) {
	// FIXME: Implement this
	return NewDefaultResult(), nil
}

func init() {
	// Register in list of hooks
	if err := RegisterWebhook(&OnConnectWebhook{
		BaseHook: newBaseHook("OnConnect", WebhookTypeConnect),
	}); err != nil {
		panic(err)
	}
}
