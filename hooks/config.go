package hooks

import "time"

type ConfigName string

type WebHookType int

const (
	WebhookTypeInvalid WebHookType = iota
	WebhookTypeConnect
	WebhookTypePlay
	WebhookTypePublish
	WebhookTypeDone
	WebhookTypePlayDone
	WebhookTypePublishDone
	WebhookTypeRecordDone
	WebhookTypeUpdate
)

type Webhooks struct {
	Hooks map[ConfigName]WebhookConfig
}

type WebhookConfig struct {
	Disabled    bool          // Is this hook turned on/off
	URL         string        // URL to call
	Method      string        // GET/POST/etc to use with URL
	Application string        // App to pass in
	Timeout     time.Duration // Timeout for webhook request
}

func (w Webhooks) UpdateConfigs() error {
	for name, cfg := range w.Hooks {
		h, err := GetHookByConfigName(name)
		if err != nil {
			return err
		}
		h.UpdateConfig(cfg)
	}
	return nil
}
