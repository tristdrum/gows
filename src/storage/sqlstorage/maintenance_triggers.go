package sqlstorage

import (
	"context"
	"fmt"
)

func maintenanceRegisteredSQL(row string) string {
	return fmt.Sprintf(`EXISTS(SELECT 1 FROM gows_maintenance_writes w WHERE w.id=%s.id AND w.jid=%s.jid AND w.timestamp=%s.timestamp AND w.depth>0)`, row, row, row)
}

// Every dependency here is standard SQL, available to the old image too. The
// trigger records unknown writers without preventing their normal SQL writes.
func (c *GContainer) installMaintenanceTriggers(ctx context.Context) error {
	if c.dialect == "sqlite3" || c.dialect == "sqlite" {
		tx, err := c.db.BeginTxx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		for _, op := range []string{"INSERT", "UPDATE", "DELETE"} {
			row := "NEW"
			deleted := "deleted"
			meaningful := maintenanceChatSQL("NEW.jid")
			registered := maintenanceRegisteredSQL("NEW")
			if op == "DELETE" {
				row = "OLD"
				deleted = "TRUE"
				meaningful = maintenanceChatSQL("OLD.jid")
				registered = maintenanceRegisteredSQL("OLD")
			}
			if op == "UPDATE" {
				meaningful = "(" + maintenanceChatSQL("OLD.jid") + " OR " + maintenanceChatSQL("NEW.jid") + ") AND (OLD.id<>NEW.id OR OLD.jid<>NEW.jid OR OLD.timestamp<>NEW.timestamp OR OLD.from_me<>NEW.from_me OR OLD.is_real<>NEW.is_real OR json_extract(OLD.data,'$.Message') IS NOT json_extract(NEW.data,'$.Message') OR json_extract(OLD.data,'$.Info') IS NOT json_extract(NEW.data,'$.Info'))"
				registered = "(" + maintenanceRegisteredSQL("OLD") + " AND " + maintenanceRegisteredSQL("NEW") + ")"
			}
			name := "gows_maintenance_revision_" + op
			if _, err = tx.ExecContext(ctx, "DROP TRIGGER IF EXISTS "+name); err != nil {
				return err
			}
			q := fmt.Sprintf(`CREATE TRIGGER %s AFTER %s ON gows_messages BEGIN
    UPDATE gows_maintenance_state SET revision=revision+1,untracked=untracked OR ((%s) AND NOT (%s)) WHERE id=1;
    UPDATE gows_maintenance_activity SET revision=(SELECT revision FROM gows_maintenance_state WHERE id=1),deleted=%s WHERE id=%s.id AND (%s);
   END`, name, op, meaningful, registered, deleted, row, meaningful)
			if _, err = tx.ExecContext(ctx, q); err != nil {
				return err
			}
		}
		return tx.Commit()
	}
	ordinaryNew := maintenanceChatSQL("NEW.jid")
	ordinaryOld := maintenanceChatSQL("OLD.jid")
	q := fmt.Sprintf(`CREATE OR REPLACE FUNCTION gows_maintenance_revision() RETURNS TRIGGER AS $$
 DECLARE meaningful BOOLEAN; registered BOOLEAN;
 BEGIN
  IF TG_OP='INSERT' THEN meaningful:=%s; registered:=%s;
  ELSIF TG_OP='DELETE' THEN meaningful:=%s; registered:=%s;
  ELSE meaningful:=(%s OR %s) AND (OLD.id IS DISTINCT FROM NEW.id OR OLD.jid IS DISTINCT FROM NEW.jid OR OLD.timestamp IS DISTINCT FROM NEW.timestamp OR OLD.from_me IS DISTINCT FROM NEW.from_me OR OLD.is_real IS DISTINCT FROM NEW.is_real OR OLD.data::jsonb-'Status' IS DISTINCT FROM NEW.data::jsonb-'Status'); registered:=%s AND %s;
  END IF;
  UPDATE gows_maintenance_state SET revision=revision+1,untracked=untracked OR (meaningful AND NOT registered) WHERE id=1;
  IF meaningful THEN
   IF TG_OP='DELETE' THEN UPDATE gows_maintenance_activity SET deleted=TRUE,revision=(SELECT revision FROM gows_maintenance_state WHERE id=1) WHERE id=OLD.id;
   ELSE UPDATE gows_maintenance_activity SET revision=(SELECT revision FROM gows_maintenance_state WHERE id=1) WHERE id=NEW.id;
   END IF;
  END IF;
  RETURN NULL;
 END; $$ LANGUAGE plpgsql`, ordinaryNew, maintenanceRegisteredSQL("NEW"), ordinaryOld, maintenanceRegisteredSQL("OLD"), ordinaryOld, ordinaryNew, maintenanceRegisteredSQL("OLD"), maintenanceRegisteredSQL("NEW"))
	tx, err := c.db.BeginTxx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, q); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `DROP TRIGGER IF EXISTS gows_maintenance_revision ON gows_messages; CREATE TRIGGER gows_maintenance_revision AFTER INSERT OR UPDATE OR DELETE ON gows_messages FOR EACH ROW EXECUTE FUNCTION gows_maintenance_revision()`); err != nil {
		return err
	}
	return tx.Commit()
}
