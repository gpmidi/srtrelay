package hooks

import (
	"context"
	"sync"
)

type HookInstanceID int

type HookInstance struct {
	id     HookInstanceID
	ctx    context.Context
	cancel context.CancelFunc
	Hook   Webhook
	Event  Event
	Result Result
	Err    error
}

type ThreadedHookProcessor struct {
	insts     map[HookInstanceID]*HookInstance
	instsLock sync.Mutex
	wg        sync.WaitGroup
}

func NewThreadedHookProcessor(ctx context.Context, event Event) (*ThreadedHookProcessor, error) {
	thp := ThreadedHookProcessor{
		insts:     make(map[HookInstanceID]*HookInstance),
		instsLock: sync.Mutex{},
		wg:        sync.WaitGroup{},
	}

	hooks, err := GetHookByType(event.Type())
	if err != nil {
		return nil, err
	}

	thp.instsLock.Lock()
	defer thp.instsLock.Unlock()

	for i, hook := range hooks {
		idx := HookInstanceID(i)
		c, cancel := context.WithTimeout(ctx, hook.Config().Timeout)
		thp.insts[idx] = &HookInstance{
			id:     idx,
			ctx:    c,
			cancel: cancel,
			Event:  event,
			Hook:   hook,
			Result: nil,
			Err:    nil,
		}
	}

	return &thp, nil
}

func (t *ThreadedHookProcessor) Start() {
	t.instsLock.Lock()
	defer t.instsLock.Unlock()

	for _, hook := range t.insts {
		go func() {
			t.wg.Add(1)
			defer t.wg.Done()
			defer hook.cancel()

			r, err := t.Run(hook.ctx, hook.Event, hook.Hook)

			// Will block until loop is done but that's fine
			t.instsLock.Lock()
			defer t.instsLock.Unlock()
			hook.Result, hook.Err = r, err
		}()
	}
}

func (t *ThreadedHookProcessor) Run(ctx context.Context, event Event, hook Webhook) (Result, error) {
	if hook.IsEnabled() {
		// Is enabled
		return hook.OnEvent(ctx, event)
	} else {
		// If Hook isn't good to go we can just assume all is well
		return NewDefaultResult(), nil
	}
}

func (t *ThreadedHookProcessor) Results() []*HookInstance {
	t.wg.Wait()

	t.instsLock.Lock()
	defer t.instsLock.Unlock()

	res := make([]*HookInstance, len(t.insts))

	for _, hook := range t.insts {
		res[hook.id] = hook
	}

	return res
}
