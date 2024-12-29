package hooks

import "time"

type ConfigName string

type WebHookType int64

const (
	WebhookTypeInvalid WebHookType = 1 << iota
	WebhookTypePlay
	WebhookTypePublish
	WebhookTypePlayDone
	WebhookTypePublishDone
	WebhookTypeRecordDone
	WebhookTypeUpdate
	WebhookTypeConnect = WebhookTypePlay | WebhookTypePublish
	WebhookTypeDone    = WebhookTypePlayDone | WebhookTypePublishDone | WebhookTypeRecordDone
)

// Webhooks are all configured hooks from the config file
type Webhooks struct {
	Hooks map[ConfigName]WebhookConfig
}

type WebhookConfig struct {
	Disabled      bool          // Is this Hook turned on/off
	URL           string        // URL to call
	Method        string        // GET/POST/etc to use with URL
	Application   string        // App to pass in
	Timeout       time.Duration // Timeout for webhook request
	PasswordParam string        // POST Parameter containing stream passphrase
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

func (w WebhookConfig) GetPasswordParam() string {
	if w.PasswordParam == "" {
		return "auth"
	}
	return w.PasswordParam
}

func NewWebhookConfig() Webhooks {
	return Webhooks{
		Hooks: make(map[ConfigName]WebhookConfig),
	}
}
