package sqlstorage

import (
	"strings"

	"go.mau.fi/whatsmeow/types"
)

// This interface has no Status grant. Its source domain is ordinary chats only.
func maintenanceChat(jid string) bool {
	if strings.Count(jid, "@") != 1 {
		return false
	}
	parsed, err := types.ParseJID(jid)
	if err != nil || parsed.User == "" {
		return false
	}
	switch parsed.Server {
	case types.DefaultUserServer, types.LegacyUserServer, types.HiddenUserServer, types.GroupServer:
		return true
	}
	return false
}

// column is a constant owned by this package, never request input.
func maintenanceChatSQL(column string) string {
	return "(" + column + " LIKE '_%@s.whatsapp.net' OR " + column + " LIKE '_%@c.us' OR " + column + " LIKE '_%@lid' OR " + column + " LIKE '_%@g.us')"
}

func maintenanceContentSQL(dialect, column string) string {
	if dialect == "postgres" {
		return "COALESCE((" + column + "::jsonb->'Info'->>'IsNewsletterStatus')::boolean,FALSE)=FALSE"
	}
	return "COALESCE(json_extract(" + column + ",'$.Info.IsNewsletterStatus'),0)=0"
}
