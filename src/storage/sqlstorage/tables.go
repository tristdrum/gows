package sqlstorage

type Table struct {
	Name             string
	Columns          []string
	DataField        string
	OnConflict       []string
	UpdateOnConflict []string
}

var MessageTable = Table{
	Name: "gows_messages",
	Columns: []string{
		"jid",
		"id",
		"timestamp",
		"from_me",
		"is_real",
		"data",
	},
	DataField: "data",
	OnConflict: []string{
		"id",
	},
	UpdateOnConflict: []string{
		"timestamp",
		"data",
	},
}

var GroupTable = Table{
	Name: "gows_groups",
	Columns: []string{
		"id",
		"name",
		"data",
	},
	DataField: "data",
	OnConflict: []string{
		"id",
	},
	UpdateOnConflict: []string{
		"name",
		"data",
	},
}

var ChatEphemeralSettingsTable = Table{
	Name: "gows_chat_ephemeral_setting",
	Columns: []string{
		"id",
		"data",
	},
	DataField: "data",
	OnConflict: []string{
		"id",
	},
	UpdateOnConflict: []string{
		"data",
	},
}

var LabelsTable = Table{
	Name: "gows_labels",
	Columns: []string{
		"id",
		"data",
	},
	DataField: "data",
	OnConflict: []string{
		"id",
	},
	UpdateOnConflict: []string{
		"data",
	},
}

var LabelAssociationsTable = Table{
	Name: "gows_label_associations",
	Columns: []string{
		"jid",
		"label_id",
		"data",
	},
	DataField: "data",
	OnConflict: []string{
		"jid",
		"label_id",
	},
	UpdateOnConflict: []string{
		"data",
	},
}
