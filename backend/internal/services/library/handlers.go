package library

import (
	"context"
	"encoding/json"
	"time"

	"tree-eclass/internal/domain/knowledge"
	"tree-eclass/internal/domain/messages"
	"tree-eclass/internal/domain/navigation"
	"tree-eclass/internal/domain/study"
)

type courseInput struct {
	ID int64 `json:"course_id"`
}
type documentInput struct {
	ID string `json:"document_id"`
}
type pageInput struct {
	ID   string `json:"document_id"`
	Page int64  `json:"page_number"`
}
type coursesInput struct {
	IDs []int64 `json:"course_ids"`
}
type prioritiesInput struct {
	IDs   []int64 `json:"course_ids"`
	Limit int     `json:"limit"`
}
type recentInput struct {
	IDs   []int64 `json:"course_ids"`
	Since string  `json:"since"`
	Limit int     `json:"limit"`
}
type readMessagesInput struct {
	ID     string `json:"conversation_id"`
	Before int    `json:"context_before"`
	After  int    `json:"context_after"`
}
type federationInput struct {
	Query string  `json:"query"`
	IDs   []int64 `json:"course_ids"`
	Limit int     `json:"limit_per_source"`
	Mode  string  `json:"retrieval_mode"`
}

func (r *Registry) handler(name string) func(context.Context, json.RawMessage) (any, error) {
	if h := r.materialHandler(name); h != nil {
		return h
	}
	if h := r.messageHandler(name); h != nil {
		return h
	}
	return r.courseHandler(name)
}
func (r *Registry) materialHandler(name string) func(context.Context, json.RawMessage) (any, error) {
	k := knowledge.Reader{Pool: r.Pool}
	switch name {
	case "list_materials":
		return handlerFor(
			knowledge.ListRequest{Limit: 50},
			func(ctx context.Context, in knowledge.ListRequest) (any, error) { return k.Materials(ctx, in) },
		)
	case "search_materials":
		return handlerFor(
			knowledge.SearchRequest{Limit: 8, Mode: "hybrid"},
			func(ctx context.Context, in knowledge.SearchRequest) (any, error) { return k.Search(ctx, in) },
		)
	case "read_material":
		return handlerFor(
			knowledge.ReadRequest{IncludeNeighbors: true, MaxCharacters: 30000},
			func(ctx context.Context, in knowledge.ReadRequest) (any, error) { return k.Read(ctx, in) },
		)
	case "get_material_insight":
		return handlerFor(
			documentInput{},
			func(ctx context.Context, in documentInput) (any, error) { return k.MaterialInsight(ctx, in.ID) },
		)
	case "get_page_insight":
		return handlerFor(
			pageInput{},
			func(ctx context.Context, in pageInput) (any, error) { return k.PageInsight(ctx, in.ID, in.Page) },
		)
	case "get_recent_changes":
		return handlerFor(recentInput{Limit: 100}, func(ctx context.Context, in recentInput) (any, error) {
			return k.Recent(ctx, in.IDs, in.Since, in.Limit)
		})
	case "get_index_status":
		return handlerFor(
			coursesInput{},
			func(ctx context.Context, in coursesInput) (any, error) { return k.StatusFor(ctx, in.IDs) },
		)
	}
	return nil
}
func (r *Registry) messageHandler(name string) func(context.Context, json.RawMessage) (any, error) {
	m := messages.Reader{Pool: r.Pool}
	switch name {
	case "search_course_messages":
		return handlerFor(
			messages.SearchRequest{Limit: 8, Mode: "hybrid"},
			func(ctx context.Context, in messages.SearchRequest) (any, error) {
				return m.Search(ctx, in, time.Now())
			},
		)
	case "read_course_messages":
		return handlerFor(
			readMessagesInput{Before: 1, After: 1},
			func(ctx context.Context, in readMessagesInput) (any, error) {
				return m.Read(ctx, in.ID, in.Before, in.After)
			},
		)
	case "get_message_index_status":
		return handlerFor(
			coursesInput{},
			func(ctx context.Context, in coursesInput) (any, error) { return m.Status(ctx, in.IDs) },
		)
	}
	return nil
}
func (r *Registry) courseHandler(name string) func(context.Context, json.RawMessage) (any, error) {
	switch name {
	case "list_courses":
		return handlerFor(struct{}{}, func(ctx context.Context, _ struct{}) (any, error) { return r.courses(ctx) })
	case "get_course_study_blueprint":
		return handlerFor(courseInput{}, func(ctx context.Context, in courseInput) (any, error) {
			view, err := (navigation.Service{Pool: r.Pool}).Read(
				ctx,
				navigation.Request{CourseID: in.ID, Roadmap: true, IncludeActions: true},
			)
			return view.Blueprint, err
		})
	case "get_study_priorities":
		return handlerFor(prioritiesInput{Limit: 8}, func(ctx context.Context, in prioritiesInput) (any, error) {
			return (study.Service{Pool: r.Pool}).Priorities(ctx, in.IDs, in.Limit, time.Now())
		})
	case "search_course_knowledge":
		return handlerFor(federationInput{Limit: 6, Mode: "hybrid"}, r.federate)
	}
	return nil
}
