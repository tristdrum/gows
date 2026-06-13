package main

import (
	"fmt"
	"reflect"
	"strconv"
)

// maybeParserFunc parses a raw string into a value of type T (returned as any).
type maybeParserFunc func(s string) (any, error)

// maybeRegistry maps a reflect.Type to its parser. Register types here.
var maybeRegistry = map[reflect.Type]maybeParserFunc{
	reflect.TypeOf((*bool)(nil)): func(s string) (any, error) {
		b, err := strconv.ParseBool(s)
		if err != nil {
			return nil, err
		}
		return &b, nil
	},
	reflect.TypeOf((*uint32)(nil)): func(s string) (any, error) {
		n, err := strconv.ParseUint(s, 10, 32)
		if err != nil {
			return nil, err
		}
		u := uint32(n)
		return &u, nil
	},
}

// Maybe wraps an optional env value with three states:
//
//	Set=false          → env var absent or empty string, no change should be applied
//	Set=true, Value=nil → env var was "null", proto field should be set to nil
//	Set=true, Value=v   → env var was parsed successfully, apply v
//
// Implements encoding.TextUnmarshaler so caarlos0/env picks it up automatically.
type Maybe[T any] struct {
	Set   bool
	Value T
}

func (m *Maybe[T]) UnmarshalText(text []byte) error {
	m.Set = true
	s := string(text)
	if s == "null" {
		// Value stays at zero (nil for pointer types)
		return nil
	}
	var zero T
	t := reflect.TypeOf(zero)
	parser, ok := maybeRegistry[t]
	if !ok {
		return fmt.Errorf("no Maybe parser registered for type %v", t)
	}
	val, err := parser(s)
	if err != nil {
		return err
	}
	m.Value = val.(T)
	return nil
}
