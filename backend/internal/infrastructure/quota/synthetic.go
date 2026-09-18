package quota

import (
	"bytes"
	"encoding/json"
	"errors"
	"math"
	"strconv"
	"strings"
	"time"
)

func numeric(value any) *float64 {
	var number float64
	switch v := value.(type) {
	case json.Number:
		number, _ = strconv.ParseFloat(string(v), 64)
	case float64:
		number = v
	case string:
		cleaned := strings.NewReplacer("$", "", ",", "").Replace(strings.TrimSpace(v))
		n, err := strconv.ParseFloat(cleaned, 64)
		if err != nil {
			return nil
		}
		number = n
	default:
		return nil
	}
	if math.IsNaN(number) || math.IsInf(number, 0) {
		return nil
	}
	return &number
}
func date(value any) *time.Time {
	text, ok := value.(string)
	if !ok {
		return nil
	}
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02T15:04:05.999999999", "2006-01-02 15:04:05"} {
		parsed, err := time.Parse(layout, strings.TrimSpace(text))
		if err == nil {
			return &parsed
		}
	}
	return nil
}
func mapping(value any) map[string]any { m, _ := value.(map[string]any); return m }
func truth(value any) bool             { v, _ := value.(bool); return v }

func parseSynthetic(raw []byte) (Snapshot, error) {
	var payload map[string]any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if !json.Valid(raw) || decoder.Decode(&payload) != nil {
		return Snapshot{}, errors.New("quota response is not valid JSON")
	}
	rolling, weekly, subscription := mapping(
		payload["rollingFiveHourLimit"],
	), mapping(
		payload["weeklyTokenLimit"],
	), mapping(
		payload["subscription"],
	)
	remaining := numeric(rolling["remaining"])
	weeklyRemaining := numeric(weekly["percentRemaining"])
	if weeklyRemaining == nil {
		left, maximum := numeric(weekly["remainingCredits"]), numeric(weekly["maxCredits"])
		if left != nil && maximum != nil && *maximum > 0 {
			value := *left / (*maximum) * 100
			weeklyRemaining = &value
		}
	}
	var used *float64
	if weeklyRemaining != nil {
		value := 100 - min(100, max(0, *weeklyRemaining))
		used = &value
	}
	requests, limit := numeric(subscription["requests"]), numeric(subscription["limit"])
	if remaining == nil && weeklyRemaining == nil && requests == nil && limit == nil {
		return Snapshot{}, errors.New("quota response has no recognizable usage data")
	}
	var subscriptionRemaining *float64
	if requests != nil && limit != nil {
		value := *limit - *requests
		subscriptionRemaining = &value
	}
	return Snapshot{Windows: []Window{
		{Name: "rolling", Remaining: remaining, Limited: truth(rolling["limited"]), Reset: date(rolling["nextTickAt"])},
		{Name: "weekly", Used: used, Limit: 100, Reset: date(weekly["nextRegenAt"])},
		{
			Name:      "subscription",
			Remaining: subscriptionRemaining,
			Limited:   truth(subscription["limited"]),
			Reset:     date(subscription["renewsAt"]),
		},
	}}, nil
}
