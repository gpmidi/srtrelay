package hooks

import (
	"context"
	"fmt"
	"sync"
)

type Webhook interface {
	// HandleType returns the webhook handler type
	HandleType() WebHookType

	// ConfigName returns the Hook name for the configuration file
	ConfigName() ConfigName
	// Config returns the raw configuration info
	Config() WebhookConfig
	// UpdateConfig updates the configuration file
	UpdateConfig(config WebhookConfig)
	// ConfigIsValid returns true if it's a usable configuration
	ConfigIsValid() bool

	// OnEvent is called when the webhook type event happens.
	// Go/No-go is returned via bool and err if there was a handling problem. Must return go unless told not to by Hook result.
	OnEvent(ctx context.Context, event Event) (result Result, err error)

	// IsEnabled returns true if we're not disabled and have a valid configuration
	IsEnabled() bool
}

var (
	registeredHooks     = make(map[WebHookType]Webhook)
	registeredHooksLock sync.Mutex
)

type BaseHook struct {
	configName ConfigName
	config     *WebhookConfig
	handleType WebHookType
}

func newBaseHook(configName ConfigName, handleType WebHookType) BaseHook {
	return BaseHook{
		configName: configName,
		handleType: handleType,
	}
}

// ProcessEvent should be called when there is an event to pass on via hooks.
// The result is always an "all good" unless a webhook works and says not to. Optionally includes redirect info via msg.
func ProcessEvent(ctx context.Context, event Event) (res []Result, err error) {
	t, err := NewThreadedHookProcessor(ctx, event)
	if err != nil {
		return nil, err
	}

	t.Start()

	// Return the first error if there are any
	for _, h := range t.Results() {
		if h.Err != nil {
			return res, h.Err
		}
	}

	// No errors
	return res, nil
}

// ProcessEventQuick is a shortcut for ProcessEvent and NewEvent in one.
func ProcessEventQuick(ctx context.Context, hookType WebHookType) (result []Result, err error) {
	return ProcessEvent(
		ctx,
		NewEvent(
			hookType,
		),
	)
}

func (w *BaseHook) HandleType() WebHookType {
	return w.handleType
}
func (w *BaseHook) ConfigName() ConfigName {
	return w.configName
}

func (w *BaseHook) UpdateConfig(config WebhookConfig) {
	w.config = &config
}

func (w *BaseHook) Config() WebhookConfig {
	if w.config == nil {
		return WebhookConfig{
			Disabled: true,
		}
	}
	return *w.config
}

func (w *BaseHook) IsEnabled() bool {
	return !w.Config().Disabled && w.ConfigIsValid()
}

func (w *BaseHook) ConfigIsValid() bool {
	// If no config set, assume invalid
	if w.config == nil {
		return false
	}
	cfg := w.Config()
	if cfg.URL == "" {
		return false
	}
	if cfg.Method == "" {
		return false
	}
	if cfg.Application == "" {
		return false
	}
	return true
}

func RegisterWebhook(hook Webhook) error {
	hookType := hook.HandleType()
	registeredHooksLock.Lock()
	defer registeredHooksLock.Unlock()

	if _, ok := registeredHooks[hookType]; ok {
		return fmt.Errorf("Hook already registered: %v", hookType)
	}
	registeredHooks[hookType] = hook
	return nil
}

func GetHookByType(hookType WebHookType) (ret []Webhook, err error) {
	registeredHooksLock.Lock()
	defer registeredHooksLock.Unlock()

	ret = make([]Webhook, 0)

	for hType, hook := range registeredHooks {
		if hookType&hType > 0 {
			ret = append(ret, hook)
		}
	}

	return
}

func GetHookByConfigName(configName ConfigName) (Webhook, error) {
	registeredHooksLock.Lock()
	defer registeredHooksLock.Unlock()
	for _, hook := range registeredHooks {
		if hook.ConfigName() == configName {
			return hook, nil
		}
	}
	return nil, fmt.Errorf("Hook not registered: %v", configName)
}
