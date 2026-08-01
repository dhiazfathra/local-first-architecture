package syncrepl

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/domain"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/proto/syncpb"
)

func TestEnvelopeRoundTrip(t *testing.T) {
	cause := domain.EventID{NodeID: "wh-a", Seq: 7}
	tests := []struct {
		name string
		env  domain.Envelope
	}{
		{
			name: "plain event",
			env: domain.Envelope{
				ID:          domain.EventID{NodeID: "wh-a", Seq: 1},
				AggregateID: "WIDGET",
				Type:        domain.TypePutAway,
				HLC:         domain.HLC{Wall: 1700000000000, Counter: 3, Node: "wh-a"},
				RecordedAt:  time.Date(2026, 7, 30, 8, 0, 0, 123, time.UTC),
				Payload:     json.RawMessage(`{"move":{"sku":"WIDGET","from":"RECV-01","to":"PICK-01","qty":4}}`),
			},
		},
		{
			name: "compensating event carries its causation",
			env: domain.Envelope{
				ID:          domain.EventID{NodeID: "central", Seq: 2},
				AggregateID: "WIDGET",
				Type:        domain.TypeStockAdjusted,
				HLC:         domain.HLC{Wall: 1700000000001, Counter: 0, Node: "central"},
				RecordedAt:  time.Date(2026, 7, 31, 9, 0, 0, 0, time.UTC),
				CausationID: &cause,
				Payload:     json.RawMessage(`{"move":{"sku":"WIDGET","from":"RECV-01","to":"external","qty":4},"reason":"po_overreceipt"}`),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DecodeEnvelope(EncodeEnvelope(tt.env))
			if err != nil {
				t.Fatalf("DecodeEnvelope: %v", err)
			}
			if got.ID != tt.env.ID || got.Type != tt.env.Type || got.AggregateID != tt.env.AggregateID {
				t.Errorf("identity round-tripped as %+v, want %+v", got, tt.env)
			}
			if got.HLC != tt.env.HLC {
				t.Errorf("HLC = %+v, want %+v", got.HLC, tt.env.HLC)
			}
			if !got.RecordedAt.Equal(tt.env.RecordedAt) {
				t.Errorf("RecordedAt = %v, want %v", got.RecordedAt, tt.env.RecordedAt)
			}
			if string(got.Payload) != string(tt.env.Payload) {
				t.Errorf("Payload = %s, want %s", got.Payload, tt.env.Payload)
			}
			switch {
			case tt.env.CausationID == nil && got.CausationID != nil:
				t.Errorf("CausationID = %v, want nil", got.CausationID)
			case tt.env.CausationID != nil && got.CausationID == nil:
				t.Error("CausationID = nil, want it preserved")
			case tt.env.CausationID != nil && *got.CausationID != *tt.env.CausationID:
				t.Errorf("CausationID = %v, want %v", *got.CausationID, *tt.env.CausationID)
			}
		})
	}
}

func TestDecodeEnvelopeRejectsMalformedFrames(t *testing.T) {
	tests := []struct {
		name string
		in   *syncpb.Event
	}{
		{"nil event", nil},
		{"missing id", &syncpb.Event{Type: domain.TypePutAway}},
		{"empty node id", &syncpb.Event{Id: &syncpb.EventID{Seq: 1}, Type: domain.TypePutAway}},
		{"zero sequence", &syncpb.Event{Id: &syncpb.EventID{NodeId: "wh-a"}, Type: domain.TypePutAway}},
		{"missing type", &syncpb.Event{Id: &syncpb.EventID{NodeId: "wh-a", Seq: 1}}},
		{"missing hlc", &syncpb.Event{Id: &syncpb.EventID{NodeId: "wh-a", Seq: 1}, Type: domain.TypePutAway}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := DecodeEnvelope(tt.in); err == nil {
				t.Fatalf("DecodeEnvelope(%+v) = nil error, want a rejection", tt.in)
			}
		})
	}
}

func TestBatchRoundTripAndRejection(t *testing.T) {
	envs := []domain.Envelope{
		{ID: domain.EventID{NodeID: "wh-a", Seq: 1}, Type: domain.TypePutAway, AggregateID: "W",
			HLC: domain.HLC{Wall: 1, Node: "wh-a"}, RecordedAt: time.Unix(0, 0).UTC(), Payload: json.RawMessage(`{}`)},
		{ID: domain.EventID{NodeID: "wh-a", Seq: 2}, Type: domain.TypePicked, AggregateID: "W",
			HLC: domain.HLC{Wall: 2, Node: "wh-a"}, RecordedAt: time.Unix(1, 0).UTC(), Payload: json.RawMessage(`{}`)},
	}
	got, err := DecodeBatch(EncodeBatch(envs))
	if err != nil {
		t.Fatalf("DecodeBatch: %v", err)
	}
	if len(got) != 2 || got[0].ID != envs[0].ID || got[1].ID != envs[1].ID {
		t.Fatalf("batch = %+v, want the two originals in order", got)
	}
	if _, err := DecodeBatch(&syncpb.EventBatch{Events: []*syncpb.Event{{}}}); err == nil {
		t.Error("DecodeBatch with a malformed event: expected an error")
	}
	if _, err := DecodeBatch(nil); err != nil {
		t.Errorf("DecodeBatch(nil) = %v, want no error and an empty result", err)
	}
}

func TestChunkBoundsCatchUp(t *testing.T) {
	tests := []struct {
		name       string
		count      int
		payload    int
		wantChunks int
	}{
		{"empty input yields no chunks", 0, 10, 0},
		{"under both caps is one chunk", 5, 10, 1},
		{"count cap splits", MaxBatchEvents + 1, 10, 2},
		{"exactly the count cap is one chunk", MaxBatchEvents, 10, 1},
		// Each envelope costs payload + type + aggregate + 64 bytes of framing, so
		// three of these fit in a 1 MiB chunk and eight of them need three chunks.
		{"byte cap splits before the count cap", 8, MaxBatchBytes / 4, 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			envs := make([]domain.Envelope, tt.count)
			for i := range envs {
				envs[i] = domain.Envelope{
					ID:      domain.EventID{NodeID: "wh-a", Seq: uint64(i + 1)},
					Type:    domain.TypePutAway,
					HLC:     domain.HLC{Wall: int64(i), Node: "wh-a"},
					Payload: json.RawMessage(make([]byte, tt.payload)),
				}
			}
			chunks := Chunk(envs)
			if len(chunks) != tt.wantChunks {
				t.Fatalf("len(chunks) = %d, want %d", len(chunks), tt.wantChunks)
			}
			var total int
			for _, c := range chunks {
				if len(c) == 0 {
					t.Fatal("a chunk is empty, which would stall the stream")
				}
				if len(c) > MaxBatchEvents {
					t.Errorf("chunk of %d events exceeds the cap of %d", len(c), MaxBatchEvents)
				}
				total += len(c)
			}
			if total != tt.count {
				t.Errorf("chunks contain %d events, want all %d", total, tt.count)
			}
		})
	}
}
