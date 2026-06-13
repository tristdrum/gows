package gows

import (
	"fmt"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"strings"
)

func getCreationMessage(message *waE2E.Message) *waE2E.PollCreationMessage {
	creationMessage := message.PollCreationMessage
	if creationMessage == nil {
		creationMessage = message.PollCreationMessageV2
	}
	if creationMessage == nil {
		creationMessage = message.PollCreationMessageV3
	}
	return creationMessage
}

func CheckVotesInOptions(message *waE2E.Message, votes []string) error {
	creationMessage := getCreationMessage(message)
	if creationMessage == nil {
		return nil
	}
	names := make([]string, 0, len(creationMessage.Options))
	for _, option := range creationMessage.Options {
		names = append(names, *option.OptionName)
	}
	for _, option := range votes {
		if !contains(names, option) {
			return fmt.Errorf(
				"option %q not found in poll. Available options: %s",
				option,
				strings.Join(names, ", "),
			)
		}
	}
	return nil
}

func contains(names []string, option string) bool {
	for _, name := range names {
		if name == option {
			return true
		}
	}
	return false
}
