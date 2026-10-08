package quota

import (
	"context"

	"tree-eclass/internal/domain/database"
)

type Store struct{ Pool database.Store }

func toQuotaState(state database.QuotaState) QuotaState {
	windows := make([]QuotaWindow, 0, len(state.QuotaSnapshot.Windows))
	for _, window := range state.QuotaSnapshot.Windows {
		windows = append(
			windows,
			QuotaWindow{
				Name:      window.Name,
				Used:      window.Used,
				Remaining: window.Remaining,
				Limit:     window.Limit,
				Limited:   window.Limited,
				Reset:     window.Reset,
			},
		)
	}
	return QuotaState{
		Status:   state.Status,
		Message:  state.Message,
		Checked:  state.Checked,
		Next:     state.Next,
		Blocked:  state.Blocked,
		Requests: state.Requests,
		QuotaSnapshot: QuotaSnapshot{
			Windows: windows,
		},
	}
}

func fromQuotaState(state QuotaState) database.QuotaState {
	windows := make([]database.QuotaWindow, 0, len(state.Windows))
	for _, window := range state.Windows {
		windows = append(
			windows,
			database.QuotaWindow{
				Name:      window.Name,
				Used:      window.Used,
				Remaining: window.Remaining,
				Limit:     window.Limit,
				Limited:   window.Limited,
				Reset:     window.Reset,
			},
		)
	}
	return database.QuotaState{
		Status:   state.Status,
		Message:  state.Message,
		Checked:  state.Checked,
		Next:     state.Next,
		Blocked:  state.Blocked,
		Requests: state.Requests,
		QuotaSnapshot: database.QuotaSnapshot{
			Windows: windows,
		},
	}
}

func (s Store) Load(ctx context.Context, provider string) (QuotaState, error) {
	state, err := s.Pool.Quota().LoadState(ctx, provider)
	if err != nil {
		return QuotaState{}, err
	}
	return toQuotaState(state), nil
}
func (s Store) Save(ctx context.Context, provider string, state QuotaState) error {
	return s.Pool.Quota().SaveState(ctx, provider, fromQuotaState(state))
}
