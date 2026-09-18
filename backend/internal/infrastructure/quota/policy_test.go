package quota

import (
	"testing"
	"time"
)

func TestSyntheticWindowsAndLatestReset(t *testing.T) {
	now := time.Date(2026, 9, 12, 0, 0, 0, 0, time.UTC)
	snapshot, err := parseSynthetic(
		[]byte(
			`{"rollingFiveHourLimit":{"remaining":0,"limited":true,"nextTickAt":"2026-09-12T01:00:00Z"},"weeklyTokenLimit":{"remainingCredits":"$0","maxCredits":"1,000","nextRegenAt":"2026-09-13T00:00:00Z"},"subscription":{"requests":3,"limit":10}}`,
		),
	)
	if err != nil {
		t.Fatal(err)
	}
	state := evaluate(snapshot, now)
	if state.Status != "paused" || state.Blocked == nil || !state.Blocked.Equal(now.Add(24*time.Hour+30*time.Second)) {
		t.Fatal(state)
	}
	snapshot.Windows[1].Reset = nil
	if state = evaluate(snapshot, now); state.Blocked == nil || !state.Blocked.Equal(now.Add(time.Minute)) {
		t.Fatal("unknown reset failed open", state)
	}
	for _, body := range []string{`{}`, `{"rollingFiveHourLimit":{"remaining":true}}`, `{"weeklyTokenLimit":{"percentRemaining":"NaN"}}`, `{"subscription":{"requests":"Infinity"}}`} {
		if _, err = parseSynthetic([]byte(body)); err == nil {
			t.Fatal("malformed usage accepted", body)
		}
	}
}
func TestOllamaSessionThresholdAndCookie(t *testing.T) {
	snapshot, err := parseOllama(
		[]byte(
			`Hourly usage <span>95% used</span><time data-time="2026-09-12T01:00:00Z">Weekly usage <div style="width: 20%"></div>`,
		),
	)
	if err != nil || len(snapshot.Windows) != 2 {
		t.Fatal(snapshot, err)
	}
	now := time.Date(2026, 9, 12, 0, 0, 0, 0, time.UTC)
	if state := evaluate(snapshot, now); state.Status != "paused" {
		t.Fatal(state)
	}
	if _, err = parseOllama([]byte(`<form action="/login"><input type="password"></form>`)); err == nil {
		t.Fatal("signed-out page admitted requests")
	}
	for _, cookie := range []string{"", "a=b", "session=a\r\nInjected: true"} {
		if _, err = normalizeCookie(cookie); err == nil {
			t.Fatal("invalid cookie accepted")
		}
	}
	if value, err := normalizeCookie("Cookie: __Secure-next-auth.session-token.0=fixture; a=b"); err != nil ||
		value != "__Secure-next-auth.session-token.0=fixture; a=b" {
		t.Fatal(err)
	}
}
