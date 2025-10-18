package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"time"

	"github.com/patrickmn/go-cache"
	"github.com/rifqoi/slacker"
	"github.com/slack-go/slack"
	"github.com/slack-go/slack/socketmode"
)

func main() {
	cache := cache.New(1*time.Minute, 1*time.Minute)

	botToken := os.Getenv("SLACK_BOT_TOKEN")
	appToken := os.Getenv("SLACK_APP_TOKEN")

	api := slack.New(
		botToken,
		slack.OptionAppLevelToken(appToken),
	)

	client := socketmode.New(
		api,
	)

	socketmodeHandler := socketmode.NewSocketmodeHandler(client)

	slackerHandler := slacker.New(cache, socketmodeHandler, slog.New(slog.Default().Handler()))

	slackerHandler.AddPipeline("invite_user",
		slacker.HandleSlashCommand("open_modal", "/invite-user", OpenModal),
		slacker.HandleInteraction("open_modal_submit_callback", slack.InteractionTypeViewSubmission, OpenModalSubmitCallback),
	)

	socketmodeHandler.RunEventLoop()

}

func OpenModal(ctx context.Context, payload slack.SlashCommand, c *socketmode.Client) error {

	view, err := generateModalView()
	if err != nil {
		return err
	}

	_, err = slacker.OpenView(ctx, c, payload.TriggerID, view)
	if err != nil {
		return err
	}

	return nil
}
func OpenModalSubmitCallback(ctx context.Context, evt *socketmode.Event, c *socketmode.Client) error {
	callback, ok := evt.Data.(slack.InteractionCallback)
	if !ok {
		return errors.New("not InteractionCallback event")
	}

	if callback.Type != slack.InteractionTypeViewSubmission {
		slog.Warn("Not InviteBrandCommandViewCallbackId", slog.String("callback_id", callback.CallbackID))
		return errors.New("Not InviteBrandCommandViewCallbackId")
	}

	slog.Info("Incoming submission approval modal", "event", evt)

	view := callback.View.State.Values
	slog.Info("callback values", slog.Any("view", view))

	stepEvents, err := slacker.GetStepEvent(ctx, "open_modal")
	if err != nil {
		return err
	}

	data := stepEvents.Payload.Data.(slack.SlashCommand)

	slog.Info("username", "name", data.UserName, "ts", data.ChannelID)

	slog.Info("step_events", slog.Any("events", stepEvents))

	c.Ack(*evt.Request)

	slacker.PostMessage(ctx, c, data.ChannelID, slack.MsgOptionText("Sending approval!", false))

	return nil
}

func generateModalView() (*slack.ModalViewRequest, error) {
	jsn := `
{
	"type": "modal",
	"title": {
		"type": "plain_text",
		"text": "My App",
		"emoji": true
	},
	"submit": {
		"type": "plain_text",
		"text": "Submit",
		"emoji": true
	},
	"close": {
		"type": "plain_text",
		"text": "Cancel",
		"emoji": true
	},
	"blocks": [
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": ":airplane:  Invite User"
			}
		},
		{
			"type": "divider"
		},
		{
			"type": "input",
			"block_id": "email_block",
			"element": {
				"type": "plain_text_input",
				"action_id": "email"
			},
			"label": {
				"type": "plain_text",
				"text": "Email",
				"emoji": true
			}
		}
	]
}`

	var modal *slack.ModalViewRequest
	if err := json.Unmarshal([]byte(jsn), &modal); err != nil {
		return nil, err
	}

	return modal, nil

}
