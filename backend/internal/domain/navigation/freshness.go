package navigation

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"tree-eclass/internal/domain/blueprints"
	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/settings"
)

type evidenceSnapshot struct {
	ID             string `json:"document_id"`
	Hash           string `json:"source_hash"`
	AnalysisHash   string `json:"enrichment_source_hash"`
	Version        string `json:"enrichment_analysis_version"`
	Model          string `json:"enrichment_model"`
	RequestedModel string `json:"enrichment_requested_model,omitempty"`
	PayloadHash    string `json:"enrichment_payload_hash"`
	ContextHash    string `json:"enrichment_context_hash"`
}

func packetSnapshots(packet map[string]any) (map[string]evidenceSnapshot, error) {
	raw, err := json.Marshal(packet)
	if err != nil {
		return nil, err
	}
	var wire struct {
		Evidence []struct {
			Ref string `json:"evidence_ref"`
		} `json:"evidence"`
		Snapshot *struct {
			Documents []evidenceSnapshot `json:"documents"`
		} `json:"source_snapshot"`
	}
	if err = json.Unmarshal(raw, &wire); err != nil {
		return nil, err
	}
	if wire.Snapshot == nil || wire.Snapshot.Documents == nil || len(wire.Snapshot.Documents) > 1000 {
		return nil, errors.New("cached_source_snapshot_invalid")
	}
	result := map[string]evidenceSnapshot{}
	for _, item := range wire.Snapshot.Documents {
		if item.ID == "" || result[item.ID].ID != "" {
			return nil, errors.New("cached_source_snapshot_duplicate")
		}
		if item.Hash == "" || item.AnalysisHash == "" || item.Version == "" || item.Model == "" ||
			item.PayloadHash == "" {
			return nil, errors.New("cached_evidence_insight_snapshot_missing")
		}
		result[item.ID] = item
	}
	for _, item := range wire.Evidence {
		if strings.HasPrefix(item.Ref, "document:") && result[strings.TrimPrefix(item.Ref, "document:")].ID == "" {
			return nil, errors.New("cached_evidence_insight_snapshot_missing")
		}
	}
	return result, nil
}

type evidenceDocument struct {
	ID, Hash, Kind, Name, Path, Origin, URL                                        string
	AnalysisStatus, AnalysisHash, Version, Model, RequestedModel, Context, Payload string
}

func evidenceDocumentFromRow(row database.NavigationEvidenceRow) evidenceDocument {
	return evidenceDocument{
		ID: row.ID, Hash: row.Hash, Kind: row.Kind, Name: row.Name, Path: row.Path,
		Origin: row.Origin, URL: row.URL, AnalysisStatus: row.AnalysisStatus,
		AnalysisHash: row.AnalysisHash, Version: row.Version, Model: row.Model,
		RequestedModel: row.RequestedModel, Context: row.Context, Payload: row.Payload,
	}
}

func freshEvidence(
	ctx context.Context,
	tx database.Tx,
	course int64,
	a settings.AI,
	packet map[string]any,
) (map[string]map[string]any, string, error) {
	snapshots, err := packetSnapshots(packet)
	if err != nil {
		return nil, err.Error(), nil
	}
	ids := make([]string, 0, len(snapshots))
	for id := range snapshots {
		ids = append(ids, id)
	}
	rows, err := tx.Navigation().EvidenceRows(ctx, course, ids)
	if err != nil {
		return nil, "", err
	}
	links := map[string]map[string]any{}
	for _, row := range rows {
		d := evidenceDocumentFromRow(row)
		if reason := checkSnapshot(d, snapshots[d.ID], a); reason != "" {
			return nil, reason, nil
		}
		links["document:"+d.ID] = documentLink(d)
	}
	if len(links) != len(snapshots) {
		return nil, "cached_evidence_document_stale", nil
	}
	if reason, err := freshCommunity(ctx, tx, course, packet); reason != "" || err != nil {
		return nil, reason, err
	}
	addCommunityLinks(links, packet)
	return links, "", nil
}

func checkSnapshot(d evidenceDocument, captured evidenceSnapshot, a settings.AI) string {
	if captured.RequestedModel == "" {
		captured.RequestedModel = captured.Model
	}
	if d.RequestedModel == "" {
		d.RequestedModel = d.Model
	}
	if captured.Hash != d.Hash || captured.AnalysisHash != d.Hash ||
		captured.Version != settings.DocumentVersion(d.Kind) ||
		captured.RequestedModel != a.Model {
		return "cached_evidence_generation_stale"
	}
	if d.AnalysisStatus != "ready" || d.AnalysisHash != d.Hash || d.Version != captured.Version ||
		d.Model != captured.Model ||
		d.RequestedModel != captured.RequestedModel {
		return "cached_evidence_insight_stale"
	}
	if captured.ContextHash != "" && captured.ContextHash != d.Context {
		return "cached_evidence_context_stale"
	}
	var payload map[string]any
	if json.Unmarshal([]byte(d.Payload), &payload) != nil {
		return "cached_evidence_insight_invalid"
	}
	summary, _ := payload["summary"].(string)
	hash, err := blueprints.PayloadHash([]byte(d.Payload))
	if strings.TrimSpace(summary) == "" || err != nil || hash != captured.PayloadHash {
		return "cached_evidence_insight_stale"
	}
	return ""
}
