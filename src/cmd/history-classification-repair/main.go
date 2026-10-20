package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/devlikeapro/gows/storage/historyrepair"
)

func run(args []string) error {
	if len(args) < 1 {
		return errors.New("usage: history-classification-repair audit|apply|rollback [flags]")
	}
	mode := args[0]
	flags := flag.NewFlagSet(mode, flag.ContinueOnError)
	database := flags.String("db", "", "exact existing SQLite database path")
	manifestPath := flags.String("manifest", "", "private manifest file (created exclusively by audit)")
	group := flags.String("group", "", "exact group JID for audit")
	since := flags.Int64("since", 0, "inclusive audit epoch second")
	until := flags.Int64("until", 0, "exclusive audit epoch second")
	cap := flags.Int("max-candidates", 100, "audit candidate cap, at most 100")
	ids := flags.String("ids", "", "optional comma-separated exact provider IDs")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if *database == "" || *manifestPath == "" || flags.NArg() != 0 {
		return errors.New("require --db and --manifest, with no positional arguments")
	}
	path, err := filepath.EvalSymlinks(*database)
	if err != nil {
		return err
	}
	path, err = filepath.Abs(path)
	if err != nil {
		return err
	}
	var report historyrepair.Report
	switch mode {
	case "audit":
		scope := historyrepair.Scope{Database: path, Group: *group, Since: *since, Until: *until, MaxCandidates: *cap}
		if *ids != "" {
			scope.IDs = strings.Split(*ids, ",")
		}
		var manifest historyrepair.Manifest
		manifest, report, err = historyrepair.Audit(scope)
		if err == nil {
			err = historyrepair.WriteManifest(*manifestPath, manifest)
		}
	case "apply", "rollback":
		auditFlags := false
		flags.Visit(func(value *flag.Flag) {
			auditFlags = auditFlags || value.Name != "db" && value.Name != "manifest"
		})
		if auditFlags {
			return errors.New("apply/rollback accept only --db and --manifest; scope comes from the audited manifest")
		}
		var manifest historyrepair.Manifest
		manifest, err = historyrepair.ReadManifest(*manifestPath)
		if err == nil && manifest.Scope.Database != path {
			return errors.New("database path does not match the manifest")
		}
		if err == nil {
			report, err = historyrepair.Apply(manifest, mode == "rollback")
		}
	default:
		return errors.New("mode must be audit, apply or rollback")
	}
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(report)
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
