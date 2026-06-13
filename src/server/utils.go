package server

import (
	"encoding/json"
	"github.com/devlikeapro/gows/proto"
	"time"
)

func parseTimeS(s uint64) *time.Time {
	seconds := int64(s)
	value := time.Unix(seconds, 0)
	return &value
}

func toJson(data interface{}) (*__.Json, error) {
	d, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	return &__.Json{Data: string(d)}, nil
}

func toJsonList[T any](data []T) (*__.JsonList, error) {
	list := make([]*__.Json, 0, len(data))
	for _, d := range data {
		j, err := toJson(d)
		if err != nil {
			return nil, err
		}
		list = append(list, j)
	}
	return &__.JsonList{Elements: list}, nil
}
