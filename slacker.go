package slacker

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/patrickmn/go-cache"
	"github.com/slack-go/slack"
	"github.com/slack-go/slack/socketmode"
)

type contextKey string

func (c contextKey) String() string {
	return string(c)
}

const (
	slackerEventIDKey contextKey = "slacker_event_id"
	slackerPayloadKey contextKey = "slacker_payload"
)

// Slacker is the main struct for managing Slack pipelines and event handling.
// Parameters:
// - cache: An in-memory cache for storing step payloads.
// - socketmodeHandler: The Socket Mode handler for managing Slack events.
// - logger: A structured logger for logging events and errors.
type Slacker struct {
	cache    *cache.Cache
	shandler *socketmode.SocketmodeHandler
	logger   *slog.Logger
}

// New creates a new instance of Slacker with the provided cache, socketmode handler, and logger.
// Parameters:
// - cache: An in-memory cache for storing step payloads.
// - socketmodeHandler: The Socket Mode handler for managing Slack events.
// - logger: A structured logger for logging events and errors.
// Returns:
// - *Slacker: A new instance of the Slacker struct.
func New(cache *cache.Cache, socketmodeHandler *socketmode.SocketmodeHandler, logger *slog.Logger) *Slacker {
	contextHandler := &ContextHandler{logger.Handler()}

	// Wrap the handler to existing logger
	logger = slog.New(contextHandler)

	return &Slacker{
		cache:    cache,
		shandler: socketmodeHandler,
		logger:   logger,
	}
}

// SlackerHandler is a function type for handling Slack events.
// Extending the socketmode.SocketmodeHandlerFunc to propagate the slacker functionality
type SlackerHandler func(ctx context.Context, evt *socketmode.Event, c *socketmode.Client) error

// SlackerSlashCommandHandler is a function type for handling Slack slash command events.
// Extending the socketmode.SocketmodeHandlerFunc to propagate the slacker functionality
type SlackerSlashCommandHandler func(ctx context.Context, payload slack.SlashCommand, c *socketmode.Client) error

// SlackerStep represents a single step in a Slack pipeline, including its name, event type, and handler function.
type SlackerStep struct {
	StepName  string
	EventType socketmode.EventType
	Handler   SlackerHandler

	// If EventType is SlashCommand
	slashCommand        string
	slashCommandHandler SlackerSlashCommandHandler

	interactionType slack.InteractionType
}

// Handle creates a SlackerStep with the specified step name, event type, and handler function.
// Parameters:
// - stepName: The name of the step.
// - eventType: The type of Slack event to handle.
// - handler: The function to handle the event.
// Returns:
// - SlackerStep: A new instance of SlackerStep with the provided parameters.
func Handle(stepName string, eventType socketmode.EventType, handler SlackerHandler) SlackerStep {
	return SlackerStep{
		StepName:  stepName,
		EventType: eventType,
		Handler:   handler,
	}
}

// HandleSlashCommand creates a SlackerStep for handling slash command events.
// Parameters:
// - stepName: The name of the step.
// - slashCommand: The specific slash command to handle (e.g., "/invite-user").
// - handler: The function to handle the slash command event.
// Returns:
// - SlackerStep: A new instance of SlackerStep configured for the specified slash command.
func HandleSlashCommand(stepName string, slashCommand string, handler SlackerHandler) SlackerStep {
	return SlackerStep{
		StepName:     stepName,
		EventType:    socketmode.EventTypeSlashCommand,
		slashCommand: slashCommand,
		Handler:      handler,
	}
}

// HandleInteraction creates a SlackerStep for handling interaction events.
// Parameters:
// - stepName: The name of the step.
// - interactionType: The specific interaction type to handle (e.g., slack.InteractionTypeViewSubmission).
// - handler: The function to handle the interaction event.
// Returns:
// - SlackerStep: A new instance of SlackerStep configured for the specified interaction type.
func HandleInteraction(stepName string, interactionType slack.InteractionType, handler SlackerHandler) SlackerStep {
	return SlackerStep{
		StepName:        stepName,
		EventType:       socketmode.EventTypeInteractive,
		Handler:         handler,
		interactionType: interactionType,
	}
}

// SlackerContextPayload represents the payload stored in the Slacker context for a specific step.
// If you have multiple steps in a pipeline, each step's payload will be stored with its step name as the key.
// To get the payload for a specific step, use slacker.GetStepEvent(ctx, "step_name").
type SlackerContextPayload struct {
	StepOrder int
	StepName  string
	Payload   *socketmode.Event
}

type OnStart interface {
	Valid() error
	Register(pipelineName string, sl *Slacker) error
}

// type MessagePrefixOnStart struct {
// 	StepName string
// 	Prefix   string
// 	Handler  SlackerHandler
// }
//
// func (s MessagePrefixOnStart) Valid() error {
// 	if s.Prefix == "" {
// 		return ErrInvalidOnStart
// 	}
//
// 	if strings.HasPrefix(s.Prefix, "!") {
// 		return ErrInvalidPrefix
// 	}
//
// 	if s.Handler == nil {
// 		return ErrInvalidOnStart
// 	}
//
// 	return nil
// }
//
// func (s MessagePrefixOnStart) Register(pipelineName string, sl *Slacker) error {
// 	sl.shandler.HandleEvents(slackevents.Message, func(e *socketmode.Event, c *socketmode.Client) {
//
// 	})
//
// 	return nil
// }

type ShortcutOnStart struct {
	StepName   string
	CallbackID string
	Handler    SlackerHandler
}

func (s ShortcutOnStart) Valid() error {
	if s.CallbackID == "" {
		return ErrInvalidOnStart
	}
	if s.Handler == nil {
		return ErrInvalidOnStart
	}

	return nil
}

func (s ShortcutOnStart) Register(pipelineName string, sl *Slacker) error {
	sl.shandler.HandleInteraction(slack.InteractionTypeShortcut, func(e *socketmode.Event, c *socketmode.Client) {

		ctx, _ := sl.slackerContextFromSocketEvent(pipelineName, e)

		// Check for idempotence
		if sl.isIdempotenceEvent(ctx, pipelineName, s.CallbackID) {
			sl.logger.DebugContext(ctx, "onstart.idempotence_skip", slog.String("pipeline", pipelineName), slog.String("callback_id", s.CallbackID))
			return
		}

		// Shortcut is of InteractionCallback type
		payload, ok := e.Data.(slack.InteractionCallback)
		if !ok {
			sl.logger.ErrorContext(ctx, "onstart.invalid_event_data", slog.String("pipeline", pipelineName), slog.String("callback_id", s.CallbackID))
			return
		}

		// Verify callback ID
		if payload.CallbackID != s.CallbackID {
			sl.logger.DebugContext(ctx, "onstart.callback_id_mismatch", slog.String("pipeline", pipelineName), slog.String("expected_callback_id", s.CallbackID), slog.String("received_callback_id", payload.CallbackID))
			return
		}

		err := s.Handler(ctx, e, c)
		if err != nil {
			sl.logger.ErrorContext(ctx, "onstart.error", slog.String("pipeline", pipelineName), slog.Any("error", err))
		} else {
			sl.logger.DebugContext(ctx, "onstart.finish", slog.String("pipeline", pipelineName))
		}

		slackerContextPayload := SlackerContextPayload{
			StepOrder: 1,
			Payload:   e,
			StepName:  s.CallbackID,
		}
		sl.storeStepPayload(ctx, pipelineName, s.StepName, slackerContextPayload)
		sl.setIdempotenceEvent(ctx, pipelineName, s.StepName)
	})

	return nil
}

func OnStartShortcut(stepName, callbackID string, handler SlackerHandler) ShortcutOnStart {
	return ShortcutOnStart{
		StepName:   stepName,
		CallbackID: callbackID,
		Handler:    handler,
	}
}

type SlashCommandOnStart struct {
	StepName string
	Command  string
	Handler  SlackerHandler
}

func (s SlashCommandOnStart) Valid() error {
	if s.Command == "" {
		return ErrInvalidOnStart
	}
	if s.Handler == nil {
		return ErrInvalidOnStart
	}

	return nil
}

func (s SlashCommandOnStart) Register(pipelineName string, sl *Slacker) error {
	sl.shandler.HandleSlashCommand(s.Command, func(e *socketmode.Event, c *socketmode.Client) {

		ctx, _ := sl.slackerContextFromSocketEvent(pipelineName, e)
		if sl.isIdempotenceEvent(ctx, pipelineName, s.Command) {
			return
		}

		err := s.Handler(ctx, e, c)
		if err != nil {
			sl.logger.ErrorContext(ctx, "onstart.error", slog.String("pipeline", pipelineName), slog.Any("error", err))
		} else {
			sl.logger.DebugContext(ctx, "onstart.finish", slog.String("pipeline", pipelineName))
		}

		slackerContextPayload := SlackerContextPayload{
			StepOrder: 1,
			Payload:   e,
			StepName:  s.Command,
		}
		sl.storeStepPayload(ctx, pipelineName, s.StepName, slackerContextPayload)
		sl.setIdempotenceEvent(ctx, pipelineName, s.StepName)
	})

	return nil

}

func OnStartSlashCommand(stepName, command string, handler SlackerHandler) SlashCommandOnStart {
	return SlashCommandOnStart{
		StepName: stepName,
		Command:  command,
		Handler:  handler,
	}
}

type AddPipelineOption struct {
	// OnStart is a handler that will be called when the pipeline starts.
	// Only handle SlashCommand or Shortcut events.
	OnStart        OnStart
	Steps          []SlackerStep
	OnEvict        func(pipelineName string, slackerEventID string)
	ExpirationTime time.Duration
}

func (sl *Slacker) AddPipeline(pipelineName string, options AddPipelineOption) error {
	// Register OnStart handler
	err := options.OnStart.Valid()
	if err != nil {
		sl.logger.Error("invalid onstart handler", slog.String("pipeline", pipelineName), slog.Any("error", err))

		return err
	}

	err = options.OnStart.Register(pipelineName, sl)
	if err != nil {
		sl.logger.Error("failed to register onstart handler", slog.String("pipeline", pipelineName), slog.Any("error", err))
		return err
	}

	err = sl.registerSlackerSteps(pipelineName, options.Steps)
	if err != nil {
		sl.logger.Error("failed to register slacker steps", slog.String("pipeline", pipelineName), slog.Any("error", err))
		return err
	}

	return nil
}

func (sl *Slacker) registerSlackerSteps(pipelineName string, steps []SlackerStep) error {
	for index, step := range steps {
		// Step order starts from 2, as 1 is reserved for OnStart
		order := index + 2

		switch step.EventType {
		case socketmode.EventTypeInteractive:
			if step.interactionType == "" {
				return ErrInvalidSlackerStep
			}
			sl.shandler.HandleInteraction(step.interactionType, func(e *socketmode.Event, c *socketmode.Client) {
				ctx, isEventExist := sl.slackerContextFromSocketEvent(pipelineName, e)

				if order != 1 && sl.isIdempotenceEvent(ctx, pipelineName, step.StepName) {
					sl.logger.DebugContext(ctx, "step.idempotence_skip", slog.String("pipeline", pipelineName), slog.String("step", step.StepName))
					return
				}

				if !isEventExist {
					sl.logger.DebugContext(ctx, "step.missing_previous_event.skipping", slog.String("pipeline", pipelineName), slog.String("step", step.StepName))
					return
				}

				// Only handle slacker events

				sl.logger.DebugContext(ctx, "step.start", slog.String("pipeline", pipelineName), slog.String("step", step.StepName))

				err := step.Handler(ctx, e, c)
				if err != nil {
					sl.logger.ErrorContext(ctx, "step.error", slog.String("pipeline", pipelineName), slog.String("step", step.StepName), slog.Any("error", err))
				} else {
					sl.logger.DebugContext(ctx, "step.finish", slog.String("pipeline", pipelineName), slog.String("step", step.StepName))
				}

				slackerContextPayload := SlackerContextPayload{
					StepOrder: order,
					Payload:   e,
					StepName:  step.StepName,
				}

				sl.storeStepPayload(ctx, pipelineName, step.StepName, slackerContextPayload)
				sl.setIdempotenceEvent(ctx, pipelineName, step.StepName)
			})

		}
	}

	return nil
}

// GetStepEvent retrieves the event payload for a specific step from the context.
// Parameters:
// - ctx: The context containing the cached step payloads.
// - stepName: The name of the step whose event payload is to be retrieved.
// Returns:
// - *SlackerContextPayload: The payload associated with the specified step name.
// - error: An error indicating whether the step event was found or not.
func GetStepEvent(ctx context.Context, stepName string) (*SlackerContextPayload, error) {
	valueAny := ctx.Value(slackerPayloadKey)
	if valueAny == nil {
		return nil, ErrStepEventNotFound
	}

	value, ok := valueAny.(StorePayload)
	if !ok {
		return nil, ErrStepEventNotFound
	}

	stepPayload, found := value[stepName]
	if !found {
		return nil, ErrStepEventNotFound

	}

	slog.Debug("get_step_event.found", slog.String("step", stepName))

	return &stepPayload, nil
}

// OpenView opens a modal view with the slacker event ID injected into the view's private metadata.
// This allows tracking of views related to specific slacker events.
// Parameters:
// - ctx: The context containing the slacker event ID.
// - c: The socketmode client used to open the view.
// - triggerID: The trigger ID for opening the view.
// - view: The modal view request to be opened.
// Returns:
// - *slack.ViewResponse: The response from the Slack API after opening the view.
// - error: An error indicating whether the operation was successful or not.
func OpenView(ctx context.Context, c *socketmode.Client, triggerID string, callbackId string, view *slack.ModalViewRequest) (*slack.ViewResponse, error) {

	slackerEventID, ok := ctx.Value(slackerEventIDKey).(string)
	if !ok {
		slackerEventID = ""
	}
	if slackerEventID != "" {
		view.PrivateMetadata = slackerEventID
	}

	view.CallbackID = callbackId

	return c.OpenView(triggerID, *view)
}

// OpenViewContext is like OpenView but uses client.OpenViewContext.
// Parameters:
// - ctx: The context containing the slacker event ID.
// - c: The socketmode client used to open the view.
// - triggerID: The trigger ID for opening the view.
// - view: The modal view request to be opened.
// Returns:
// - *slack.ViewResponse: The response from the Slack API after opening the view.
// - error: An error indicating whether the operation was successful or not.
func OpenViewContext(ctx context.Context, c *socketmode.Client, triggerID string, view slack.ModalViewRequest) (*slack.ViewResponse, error) {

	slackerEventID, ok := ctx.Value(slackerEventIDKey).(string)
	if !ok {
		slackerEventID = ""
	}
	if slackerEventID != "" {
		view.PrivateMetadata = slackerEventID
	}

	return c.OpenViewContext(ctx, triggerID, view)
}

// PostMessage sends a message to a channel with the slacker event ID injected into the message metadata.
// This allows tracking of messages related to specific slacker events.
// Returns:
// - string: The timestamp of the sent message.
// - string: The channel ID where the message was sent.
// - error: An error indicating whether the operation was successful or not.
func PostMessage(ctx context.Context, client *socketmode.Client, channelID string, options ...slack.MsgOption) (string, string, error) {

	slackerEventID, ok := ctx.Value(slackerEventIDKey).(string)
	if !ok {
		slackerEventID = ""
	}

	slackerEventMsgOption := slack.MsgOptionMetadata(slack.SlackMetadata{
		EventType: slackerEventIDKey.String(),
		EventPayload: map[string]any{
			slackerEventIDKey.String(): slackerEventID,
		},
	})

	options = append(options, slackerEventMsgOption)

	return client.PostMessage(channelID, options...)
}

func (sl *Slacker) RunEventLoop() error {
	return sl.shandler.RunEventLoop()
}

func (sl *Slacker) slackerContextFromSocketEvent(pipelineName string, e *socketmode.Event) (context.Context, bool) {
	ctx := extractSlackerEventIDToContext(e)
	ctx = LoggerWithContext(ctx, sl.logger)
	ctx, isEventExist := sl.populateContextWithEvents(ctx, pipelineName)

	return ctx, isEventExist
}

// populateContextWithEvents fills the context with cached events for the given pipeline name.
// It retrieves the cached events from the Slacker cache and adds them to the context.
func (sl *Slacker) populateContextWithEvents(ctx context.Context, pipelineName string) (context.Context, bool) {

	payload := sl.loadPipelineStore(pipelineName, ctx)
	if payload == nil {
		return ctx, false
	}

	contextPayload := *payload

	return context.WithValue(ctx, slackerPayloadKey, contextPayload), true
}

// extractSlackerEventIDToContext extracts the slacker event ID from the socketmode.Event
// and returns a new context with the slacker event ID added.
// this function will run at the beginning of each event handler
// InteractionTypeView will be extracted from View.PrivateMetadata -> Injected with slacker.OpenViewContext()
// InteractionTypeBlockActions will be extracted from message metadata -> Injected with slacker.PostMessage()
func extractSlackerEventIDToContext(e *socketmode.Event) context.Context {

	slackerEventId := uuid.NewString()

	ctx := context.Background()

	switch e.Type {

	case socketmode.EventTypeInteractive:
		payload, ok := e.Data.(slack.InteractionCallback)
		if !ok {
			return ctx
		}

		switch payload.Type {
		case slack.InteractionTypeViewSubmission, slack.InteractionTypeViewClosed:
			slackerEventId = payload.View.PrivateMetadata
		case slack.InteractionTypeBlockActions:
			if eventId, found := eventIDFromMessageMetadata(payload.Message.Metadata); found {
				slackerEventId = eventId
			}
			// TODO: handles all interaction types
		}

	// case socketmode.EventTypeEventsAPI:
	// 	// TODO: handles all event types
	// 	switch event := e.Data.(type) {
	// 	case slackevents.EventsAPIEvent:
	// 		switch ev := event.InnerEvent.Data.(type) {
	// 		case *slackevents.MessageEvent:
	// 		}
	// 	}

	default:
	}

	ctx = context.WithValue(ctx, slackerEventIDKey, slackerEventId)

	return ctx
}

// eventIDFromBlockActions removed: logic moved to message metadata extraction

// extract slacker event ID from message metadata
func eventIDFromMessageMetadata(metadata slack.SlackMetadata) (string, bool) {
	if metadata.EventType == slackerEventIDKey.String() {
		if eventID, found := metadata.EventPayload[slackerEventIDKey.String()]; found {
			if eventIDStr, ok := eventID.(string); ok {
				return eventIDStr, true
			}
		}
	}
	return "", false
}
