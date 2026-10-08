// Package library exposes the same read-only evidence tools to Ask and MCP.
package library

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/google/jsonschema-go/jsonschema"
	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/integrations/inference"
)

var ErrArguments = errors.New("invalid tool arguments")

type Registry struct {
	Pool  database.Store
	tools map[string]registered
}
type registered struct {
	Definition inference.Tool
	Schema     *jsonschema.Resolved
	Handler    func(context.Context, json.RawMessage) (any, error)
}

func New(pool database.Store) (*Registry, error) {
	r := &Registry{Pool: pool, tools: map[string]registered{}}
	for _, spec := range specifications() {
		raw, err := json.Marshal(spec.Schema)
		if err != nil {
			return nil, err
		}
		var schema jsonschema.Schema
		if err = json.Unmarshal(raw, &schema); err != nil {
			return nil, err
		}
		resolved, err := schema.Resolve(nil)
		if err != nil {
			return nil, fmt.Errorf("tool %s schema: %w", spec.Name, err)
		}
		definition := inference.Tool{Type: "function"}
		definition.Function.Name = spec.Name
		definition.Function.Description = spec.Description
		definition.Function.Parameters = spec.Schema
		handler := r.handler(spec.Name)
		if handler == nil {
			return nil, fmt.Errorf("tool %s has no handler", spec.Name)
		}
		r.tools[spec.Name] = registered{definition, resolved, handler}
	}
	return r, nil
}

func (r *Registry) Definitions() []inference.Tool {
	result := []inference.Tool{}
	for _, tool := range r.tools {
		result = append(result, tool.Definition)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Function.Name < result[j].Function.Name })
	return result
}

func (r *Registry) Call(ctx context.Context, name string, args json.RawMessage) (any, error) {
	tool, ok := r.tools[name]
	if !ok {
		return nil, errors.New("unknown read-only library tool")
	}
	if len(args) > 65536 || !json.Valid(args) {
		return nil, ErrArguments
	}
	var value map[string]any
	decoder := json.NewDecoder(bytes.NewReader(args))
	decoder.UseNumber()
	if decoder.Decode(&value) != nil || value == nil {
		return nil, ErrArguments
	}
	normalized, err := schemaNumbers(value)
	if err != nil {
		return nil, ErrArguments
	}
	if err := tool.Schema.Validate(normalized); err != nil {
		return nil, fmt.Errorf("%w for %s", ErrArguments, name)
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	return tool.Handler(ctx, args)
}

func handlerFor[T any](
	initial T,
	call func(context.Context, T) (any, error),
) func(context.Context, json.RawMessage) (any, error) {
	return func(ctx context.Context, args json.RawMessage) (any, error) {
		in := initial
		decoder := json.NewDecoder(bytes.NewReader(args))
		decoder.UseNumber()
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&in); err != nil {
			return nil, ErrArguments
		}
		return call(ctx, in)
	}
}

// All numeric arguments are bounded integer identities or counts. Convert the
// validator's type view without rounding IDs through float64; decode handlers
// separately from the original bytes.
func schemaNumbers(value any) (any, error) {
	switch v := value.(type) {
	case json.Number:
		return v.Int64()
	case map[string]any:
		for key, item := range v {
			n, err := schemaNumbers(item)
			if err != nil {
				return nil, err
			}
			v[key] = n
		}
		return v, nil
	case []any:
		for i, item := range v {
			n, err := schemaNumbers(item)
			if err != nil {
				return nil, err
			}
			v[i] = n
		}
		return v, nil
	default:
		return value, nil
	}
}
