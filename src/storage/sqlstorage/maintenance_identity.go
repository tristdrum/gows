package sqlstorage

import (
	"context"
	"database/sql"
	"errors"
	"strings"

	"github.com/jmoiron/sqlx"
	"go.mau.fi/whatsmeow/types"
)

func maintenanceMappedIdentity(lid string, pn sql.NullString) (string, bool) {
	if !pn.Valid || pn.String == "" {
		return lid, false
	}
	candidate := pn.String + "@" + types.DefaultUserServer
	parsed, err := types.ParseJID(candidate)
	if err != nil || parsed.User != pn.String || parsed.Device != 0 || !maintenanceChat(candidate) {
		return lid, false
	}
	return candidate, true
}

func maintenanceCurrentIdentity(ctx context.Context, tx *sqlx.Tx, lid string) (string, bool, error) {
	var pn sql.NullString
	err := tx.GetContext(ctx, &pn, `SELECT pn FROM whatsmeow_lid_map WHERE lid=$1`, strings.TrimSuffix(lid, "@lid"))
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return "", false, err
	}
	canonical, known := maintenanceMappedIdentity(lid, pn)
	return canonical, known, nil
}

// Provider mapping writes also occur in connection/message/notification paths,
// outside GOWS Contact and history callbacks. Reconcile the whole requested time
// window before filtering by PN: a changed LID may move into the selected chat.
func reconcileMaintenanceIdentities(ctx context.Context, tx *sqlx.Tx, from, to int64) error {
	var rows []struct {
		ID          string         `db:"id"`
		ProviderJID string         `db:"provider_jid"`
		JID         string         `db:"jid"`
		Known       bool           `db:"identity_known"`
		PN          sql.NullString `db:"pn"`
	}
	if err := tx.SelectContext(ctx, &rows, `SELECT a.id,a.provider_jid,a.jid,a.identity_known,l.pn
  FROM gows_maintenance_activity a LEFT JOIN whatsmeow_lid_map l ON l.lid=replace(a.provider_jid,'@lid','')
  WHERE a.timestamp>=$1 AND a.timestamp<=$2 AND a.provider_jid LIKE '_%@lid'`, from, to); err != nil {
		return err
	}
	changed := false
	for _, row := range rows {
		canonical, known := maintenanceMappedIdentity(row.ProviderJID, row.PN)
		if canonical == row.JID && known == row.Known {
			continue
		}
		if !changed {
			if _, err := tx.ExecContext(ctx, `UPDATE gows_maintenance_state SET revision=revision+1 WHERE id=1`); err != nil {
				return err
			}
			changed = true
		}
		if _, err := tx.ExecContext(ctx, `UPDATE gows_maintenance_activity SET jid=$1,identity_known=$2,revision=(SELECT revision FROM gows_maintenance_state WHERE id=1) WHERE id=$3`, canonical, known, row.ID); err != nil {
			return err
		}
	}
	return nil
}
