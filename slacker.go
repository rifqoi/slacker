package slacker

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	"github.com/patrickmn/go-cache"
	"github.com/slack-go/slack"
	"github.com/slack-go/slack/socketmode"
)

const SlackerEventIDKey string = "slacker_event_id"
const SlackerPayloadKey string = "slacker_payload"

// We mentioned that there were a few different types of interaction payloads your app might receive.
// They'll be sent to your specified Request URL in an HTTP POST request in the form application/x-www-form-urlencoded.
// For more information, refer to Using the Slack Web API: Basics.
// The body of the request will contain a payload parameter; your app should parse this payload parameter as JSON.
// The resulting object can have different structures depending on the source. All those structures will have a type field that indicates the source of the interaction. Our reference docs have a more detailed look at the payload structures for the different type sources:
// - block_actions payloads are received when a user clicks a Block Kit interactive component.
// - shortcut and message_actions payloads are received when global and message shortcuts are used.
// - view_submission payloads are received when a modal is submitted.
// - view_closed payloads are received when a modal is canceled.
//
// Interaction ->
// 	InteractionTypeDialogCancellation = InteractionType("dialog_cancellation")
// InteractionTypeDialogSubmission   = InteractionType("dialog_submission")
// InteractionTypeDialogSuggestion   = InteractionType("dialog_suggestion")
// InteractionTypeInteractionMessage = InteractionType("interactive_message")
// InteractionTypeMessageAction      = InteractionType("message_action")
// InteractionTypeBlockActions       = InteractionType("block_actions")
// InteractionTypeBlockSuggestion    = InteractionType("block_suggestion")
// InteractionTypeViewSubmission     = InteractionType("view_submission")
// InteractionTypeViewClosed         = InteractionType("view_closed")
// InteractionTypeShortcut           = InteractionType("shortcut")
// InteractionTypeWorkflowStepEdit   = InteractionType("workflow_step_edit")
//
// Action Type ->
// AttachmentAction
// BlockAction

// 1. Pipe Function to Inject Custom ID into the socketmodehandler.Event
// 2. GetState(either ctx or event),
// 3. ChatMessageWithContext(), encapsulate the message blocks with custom id that we store in the cache
// 4.
type Slacker struct {
	cache    *cache.Cache
	shandler *socketmode.SocketmodeHandler
	logger   *slog.Logger
}

func New(cache *cache.Cache, socketmodeHandler *socketmode.SocketmodeHandler, logger *slog.Logger) *Slacker {
	return &Slacker{
		cache:    cache,
		shandler: socketmodeHandler,
		logger:   logger,
	}
}

type SlackerHandler func(ctx context.Context, evt *socketmode.Event, c *socketmode.Client) error
type SlackerSlashCommandHandler func(ctx context.Context, payload slack.SlashCommand, c *socketmode.Client) error

type SlackerStep struct {
	StepName  string
	EventType socketmode.EventType
	Handler   SlackerHandler

	// If EventType is SlashCommand
	slashCommand        string
	slashCommandHandler SlackerSlashCommandHandler

	interactionType slack.InteractionType
}

func Handle(stepName string, eventType socketmode.EventType, handler SlackerHandler) SlackerStep {
	return SlackerStep{
		StepName:  stepName,
		EventType: eventType,
		Handler:   handler,
	}
}

func HandleSlashCommand(stepName string, slashCommand string, handler SlackerSlashCommandHandler) SlackerStep {
	return SlackerStep{
		StepName:            stepName,
		EventType:           socketmode.EventTypeSlashCommand,
		slashCommand:        slashCommand,
		slashCommandHandler: handler,
	}
}

func HandleInteraction(stepName string, interactionType slack.InteractionType, handler SlackerHandler) SlackerStep {
	return SlackerStep{
		StepName:        stepName,
		EventType:       socketmode.EventTypeInteractive,
		Handler:         handler,
		interactionType: interactionType,
	}
}

type SlackerContextPayload struct {
	StepOrder int
	StepName  string
	Payload   *socketmode.Event
}

func (sl *Slacker) AddPipeline(pipelineName string, pipelines ...SlackerStep) {

	for index, step := range pipelines {

		order := index + 1

		switch step.EventType {
		case socketmode.EventTypeSlashCommand:
			sl.logger.Info("registering slash command", slog.Any("command", step))
			sl.shandler.HandleSlashCommand(step.slashCommand, func(e *socketmode.Event, c *socketmode.Client) {

				ctx := extractSlackerEventIDToContext(e)
				ctx = populateContextWithEvents(sl.cache, ctx, pipelineName)

				// Populate the context first
				// Fill context with new payload
				// Payload =>  pipelineName.[].stepName = event / {"event": "event", "order", 1}
				// User just call slacker.GetStepEvent("step_name") -> socketmode.Event

				payload, ok := e.Data.(slack.SlashCommand)
				if !ok {
					return
				}

				c.Ack(*e.Request)
				err := step.slashCommandHandler(ctx, payload, c)
				if err != nil {
					sl.logger.ErrorContext(ctx, "error-found", slog.Any("error", err))
				}

				slackerContextPayload := SlackerContextPayload{
					StepOrder: order,
					Payload:   e,
					StepName:  step.StepName,
				}

				buildSlackerCache(sl.cache, ctx, pipelineName, step.StepName, slackerContextPayload)

			})

		case socketmode.EventTypeInteractive:

			if step.interactionType != "" {
				sl.shandler.HandleInteraction(step.interactionType, func(e *socketmode.Event, c *socketmode.Client) {
					ctx := extractSlackerEventIDToContext(e)
					ctx = populateContextWithEvents(sl.cache, ctx, pipelineName)

					err := step.Handler(ctx, e, c)
					if err != nil {
						sl.logger.ErrorContext(ctx, "error-found", slog.Any("error", err))
					}

					slackerContextPayload := SlackerContextPayload{
						StepOrder: order,
						Payload:   e,
						StepName:  step.StepName,
					}

					buildSlackerCache(sl.cache, ctx, pipelineName, step.StepName, slackerContextPayload)

				})
			}

		}
	}
}

// InteractionCallback.View -> View.PrivateMetadata -> Injected with slacker.OpenViewContext()
// InteractionCallback.PostMessage (Blocks) -> Blocks.Action.ActionID = "slacker_event_id" -> Injected with slacker.ChatMessageWithContext()
func GetStepEvent(ctx context.Context, stepName string) (*SlackerContextPayload, error) {
	value, ok := ctx.Value(SlackerPayloadKey).(slackerCache)

	slog.Info("slackercontext", "value", value)
	if !ok {
		return nil, ErrStepEventNotFound
	}

	stepPayload, found := value[stepName]
	if !found {
		return nil, ErrStepEventNotFound

	}

	return &stepPayload, nil
}

func OpenView(ctx context.Context, c *socketmode.Client, triggerID string, view *slack.ModalViewRequest) (*slack.ViewResponse, error) {

	slackerEventID := ctx.Value(SlackerEventIDKey).(string)
	fmt.Printf("slackerEventId: %s", slackerEventID)
	if slackerEventID != "" {
		view.PrivateMetadata = slackerEventID
	}

	return c.OpenView(triggerID, *view)
}

func OpenViewContext(ctx context.Context, c *socketmode.Client, triggerID string, view slack.ModalViewRequest) (*slack.ViewResponse, error) {

	slackerEventID := ctx.Value(SlackerEventIDKey).(string)
	if slackerEventID != "" {
		view.PrivateMetadata = slackerEventID
	}

	return c.OpenViewContext(ctx, triggerID, view)
}

func PostMessage(ctx context.Context, client *socketmode.Client, channelID string, options ...slack.MsgOption) (string, string, error) {

	slackerEventID := ctx.Value(SlackerEventIDKey).(string)

	slackerEventMsgOption := slack.MsgOptionMetadata(slack.SlackMetadata{
		EventType: SlackerEventIDKey,
		EventPayload: map[string]any{
			SlackerEventIDKey: slackerEventID,
		},
	})

	options = append(options, slackerEventMsgOption)

	return client.PostMessage(channelID, options...)
}

func populateContextWithEvents(c *cache.Cache, ctx context.Context, pipelineName string) context.Context {

	key := buildCacheKey(ctx, pipelineName)

	payload, ok := c.Get(key)
	slog.Info("populateContextWithEvents", "payload", payload, "ok", ok)
	if !ok {
		return ctx
	}

	contextPayload := payload.(slackerCache)

	return context.WithValue(ctx, SlackerPayloadKey, contextPayload)
}

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
			if eventId, found := extractFromBlockActions(payload.ActionCallback.BlockActions); found {
				slackerEventId = eventId
			}
		}

	case socketmode.EventTypeEventsAPI:
	default:
	}

	ctx = context.WithValue(ctx, SlackerEventIDKey, slackerEventId)

	return ctx
}

func extractFromBlockActions(blockActions []*slack.BlockAction) (string, bool) {
	for _, action := range blockActions {
		// Return when action_id already have slacker_event_id
		if action.ActionID == SlackerEventIDKey {
			return action.Value, true
		}
	}

	return "", false
}
