package settings

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
)

//go:embed ai_defaults.json
var aiDefaults []byte
var Providers = []string{"synthetic", "ollama", "huggingface", "alibaba", "zai", "opencode-go"}
var ChatProviders = []string{"synthetic", "ollama", "huggingface", "alibaba", "opencode-go"}
var AnalysisProviders = []string{"synthetic", "ollama", "huggingface", "alibaba", "zai"}

type AI struct {
	Disabled          []string `json:"disabled_providers"`
	ChatOrder         []string `json:"chat_provider_order"`
	DefaultModel      string   `json:"chat_default_model"`
	ChatModels        []string `json:"chat_models"`
	Think             bool     `json:"chat_think"`
	SyntheticChat     string   `json:"synthetic_chat_model"`
	OllamaChat        string   `json:"ollama_chat_model"`
	AlibabaChat       string   `json:"alibaba_chat_model"`
	OpenCodeChat      string   `json:"opencode_go_model"`
	Provider          string   `json:"ai_provider"`
	EnrichmentEnabled bool     `json:"ai_enrichment_enabled"`
	Model             string   `json:"ai_model"`
	Order             []string `json:"ai_provider_order"`
	SyntheticModel    string   `json:"synthetic_enrichment_model"`
	OllamaModel       string   `json:"ollama_enrichment_model"`
	HuggingFaceModel  string   `json:"huggingface_model"`
	AlibabaModel      string   `json:"alibaba_enrichment_model"`
	ZAIModel          string   `json:"zai_enrichment_model"`
	DocumentFallbacks []string `json:"ai_document_fallback_models"`
	PageFallbacks     []string `json:"ai_page_fallback_models"`
	CourseEnabled     bool     `json:"ai_course_synthesis_enabled"`
	CourseModel       string   `json:"ai_course_model"`
	CourseFallbacks   []string `json:"ai_course_fallback_models"`
	PracticeEnabled   bool     `json:"ai_practice_questions_enabled"`
	PracticeModel     string   `json:"ai_practice_model"`
	PracticeFallbacks []string `json:"ai_practice_fallback_models"`
}

func DefaultAI() AI {
	var result AI
	if err := json.Unmarshal(aiDefaults, &result); err != nil {
		panic(err)
	}
	return result
}
func (a AI) ChatModel(provider string) string {
	return map[string]string{
		"synthetic":   a.SyntheticChat,
		"ollama":      a.OllamaChat,
		"alibaba":     a.AlibabaChat,
		"opencode-go": a.OpenCodeChat,
		"huggingface": a.HuggingFaceModel,
	}[provider]
}
func (a AI) Enabled(provider string) bool { return !slices.Contains(a.Disabled, provider) }
func ProviderForModel(model string) string {
	model = strings.TrimSpace(model)
	switch {
	case strings.HasPrefix(model, "ali:") || model == "qwen3.8-flash" || model == "deepseek-v4-flash-0731":
		return "alibaba"
	case strings.HasPrefix(model, "zai:") || model == "glm-5.3" || model == "glm-5.3-flash":
		return "zai"
	case strings.HasPrefix(model, "syn:"):
		return "synthetic"
	case strings.HasPrefix(model, "hf:") || strings.Contains(model, "/"):
		return "huggingface"
	default:
		return "ollama"
	}
}
func model(value string) error {
	if value == "" || utf8.RuneCountInString(value) > 200 || strings.ContainsFunc(value, unicode.IsSpace) {
		return Invalid{"Model names must contain 1 to 200 characters without spaces"}
	}
	return nil
}
func unique(values []string) []string {
	result := []string{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" && !slices.Contains(result, value) {
			result = append(result, value)
		}
	}
	return result
}
func validateOrder(order, allowed []string) error {
	if len(order) == 0 {
		return Invalid{"At least one backend must be enabled"}
	}
	if len(unique(order)) != len(order) {
		return Invalid{"Each backend may appear only once"}
	}
	for _, provider := range order {
		if !slices.Contains(allowed, provider) {
			return Invalid{"Unsupported backend: " + provider}
		}
	}
	return nil
}
func (a *AI) Normalize() error {
	a.Disabled = unique(providerNames(a.Disabled))
	a.ChatOrder, a.Order = providerNames(a.ChatOrder), providerNames(a.Order)
	a.Provider = strings.ToLower(strings.TrimSpace(a.Provider))
	if err := a.validateBackends(); err != nil {
		return err
	}
	if !a.Enabled(a.Provider) {
		for _, provider := range append(slices.Clone(a.Order), AnalysisProviders...) {
			if a.Enabled(provider) {
				a.Provider = provider
				break
			}
		}
	}
	if err := a.normalizeModels(); err != nil {
		return err
	}
	if !slices.Contains(a.ChatModels, a.DefaultModel) {
		return Invalid{"Default Ask model must also appear in the Ask model list"}
	}
	a.fallbackDefaultModel()
	return nil
}

func (a *AI) validateBackends() error {
	for _, provider := range a.Disabled {
		if !slices.Contains(Providers, provider) {
			return Invalid{"Unsupported provider: " + provider}
		}
	}
	if len(a.Disabled) >= len(Providers) {
		return Invalid{"At least one provider must remain enabled"}
	}
	if err := validateOrder(a.ChatOrder, ChatProviders); err != nil {
		return err
	}
	if err := validateOrder(a.Order, AnalysisProviders); err != nil {
		return err
	}
	if !slices.Contains(AnalysisProviders, a.Provider) {
		return Invalid{"Unsupported document analysis backend"}
	}
	if !slices.ContainsFunc(a.ChatOrder, a.Enabled) {
		return Invalid{"At least one Ask backend must remain enabled"}
	}
	if !slices.ContainsFunc(AnalysisProviders, a.Enabled) {
		return Invalid{"At least one document analysis backend must remain enabled"}
	}
	return nil
}

func (a *AI) normalizeModels() error {
	for _, field := range a.modelFields() {
		*field = strings.TrimSpace(*field)
		if err := model(*field); err != nil {
			return err
		}
	}
	for _, models := range []*[]string{
		&a.ChatModels,
		&a.DocumentFallbacks,
		&a.PageFallbacks,
		&a.CourseFallbacks,
		&a.PracticeFallbacks,
	} {
		*models = unique(*models)
		if len(*models) > 30 {
			return Invalid{"A model list may contain at most 30 models"}
		}
		for _, name := range *models {
			if err := model(name); err != nil {
				return err
			}
		}
	}
	return nil
}

func (a *AI) fallbackDefaultModel() {
	served := false
	for _, provider := range ChatProviders {
		if a.Enabled(provider) && a.ChatModel(provider) == a.DefaultModel {
			served = true
		}
	}
	if !served && !a.Enabled(ProviderForModel(a.DefaultModel)) {
		for _, provider := range a.ChatOrder {
			if a.Enabled(provider) {
				a.DefaultModel = a.ChatModel(provider)
				break
			}
		}
		if !slices.Contains(a.ChatModels, a.DefaultModel) {
			a.ChatModels = append(a.ChatModels, a.DefaultModel)
		}
	}
}

func (a *AI) modelFields() map[string]*string {
	return map[string]*string{
		"chat_default_model":         &a.DefaultModel,
		"synthetic_chat_model":       &a.SyntheticChat,
		"ollama_chat_model":          &a.OllamaChat,
		"alibaba_chat_model":         &a.AlibabaChat,
		"opencode_go_model":          &a.OpenCodeChat,
		"ai_model":                   &a.Model,
		"synthetic_enrichment_model": &a.SyntheticModel,
		"ollama_enrichment_model":    &a.OllamaModel,
		"huggingface_model":          &a.HuggingFaceModel,
		"alibaba_enrichment_model":   &a.AlibabaModel,
		"zai_enrichment_model":       &a.ZAIModel,
		"ai_course_model":            &a.CourseModel,
		"ai_practice_model":          &a.PracticeModel,
	}
}
func AIFromForm(form url.Values) (AI, error) {
	a := DefaultAI()
	a.Disabled = []string{}
	for _, provider := range Providers {
		if form.Get("provider_enabled_"+provider) != "on" {
			a.Disabled = append(a.Disabled, provider)
		}
	}
	a.ChatOrder, a.Order = []string{}, []string{}
	for rank := 1; rank <= 4; rank++ {
		if value := strings.TrimSpace(form.Get("chat_provider_" + strconv.Itoa(rank))); value != "" {
			a.ChatOrder = append(a.ChatOrder, strings.ToLower(value))
		}
		if value := strings.TrimSpace(form.Get("ai_provider_" + strconv.Itoa(rank))); value != "" {
			a.Order = append(a.Order, strings.ToLower(value))
		}
	}
	optional := []string{
		"synthetic_enrichment_model",
		"ollama_enrichment_model",
		"huggingface_model",
		"alibaba_chat_model",
		"alibaba_enrichment_model",
		"zai_enrichment_model",
	}
	for name, target := range a.modelFields() {
		if form.Has(name) || !slices.Contains(optional, name) {
			*target = form.Get(name)
		}
	}
	a.Provider = strings.ToLower(strings.TrimSpace(form.Get("ai_provider")))
	for name, target := range map[string]*bool{
		"chat_think":                    &a.Think,
		"ai_enrichment_enabled":         &a.EnrichmentEnabled,
		"ai_course_synthesis_enabled":   &a.CourseEnabled,
		"ai_practice_questions_enabled": &a.PracticeEnabled,
	} {
		*target = form.Get(name) == "on"
	}
	for name, target := range map[string]*[]string{
		"chat_models":                 &a.ChatModels,
		"ai_document_fallback_models": &a.DocumentFallbacks,
		"ai_page_fallback_models":     &a.PageFallbacks,
		"ai_course_fallback_models":   &a.CourseFallbacks,
		"ai_practice_fallback_models": &a.PracticeFallbacks,
	} {
		*target = unique(strings.Split(form.Get(name), ","))
	}
	err := a.Normalize()
	return a, err
}
func (s Service) AI(ctx context.Context) (AI, error) {
	return ReadAI(ctx, s.Pool)
}

// ReadAI accepts a transaction so derived reads share their source snapshot.
func ReadAI(ctx context.Context, db interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}) (AI, error) {
	result := DefaultAI()
	var raw []byte
	err := db.QueryRow(ctx, `SELECT value FROM app.native_settings WHERE key='ai'`).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return result, nil
	}
	if err != nil {
		return result, err
	}
	if err = json.Unmarshal(raw, &result); err != nil {
		return result, err
	}
	err = result.Normalize()
	return result, err
}

func providerNames(values []string) []string {
	result := []string{}
	for _, value := range values {
		value = strings.ToLower(strings.TrimSpace(value))
		if value != "" {
			result = append(result, value)
		}
	}
	return result
}
func (s Service) SaveAI(ctx context.Context, a AI) error {
	if err := a.Normalize(); err != nil {
		return err
	}
	data, err := json.Marshal(a)
	if err != nil {
		return err
	}
	_, err = s.Pool.Exec(
		ctx,
		`INSERT INTO app.native_settings(key,value) VALUES('ai',$1) ON CONFLICT(key) DO UPDATE SET value=$1,updated_at=now()`,
		data,
	)
	return err
}
