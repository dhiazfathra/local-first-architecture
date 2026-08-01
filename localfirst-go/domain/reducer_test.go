package domain_test

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/dhiazfathra/local-first-architecture/localfirst-go/clock"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/domain"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/eventlog"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/projection"
)

func rec(t *testing.T, seq uint64, typ string, ev any) eventlog.Record {
	t.Helper()
	payload, err := json.Marshal(ev)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return eventlog.Record{
		NodeID: "n1", Seq: seq, Clock: clock.HLC{Wall: int64(seq), NodeID: "n1"},
		Type: typ, Payload: payload,
	}
}

func TestBalanceReducer(t *testing.T) {
	tests := []struct {
		name    string
		recs    []eventlog.Record
		want    domain.State
		wantErr error
	}{
		{
			name: "received accumulates",
			recs: []eventlog.Record{
				rec(t, 1, domain.TypeReceived, domain.Received{SKU: "S", Location: "A", Qty: 5}),
				rec(t, 2, domain.TypeReceived, domain.Received{SKU: "S", Location: "A", Qty: 2}),
			},
			want: domain.State{{SKU: "S", Location: "A"}: 7},
		},
		{
			name: "issued subtracts",
			recs: []eventlog.Record{
				rec(t, 1, domain.TypeReceived, domain.Received{SKU: "S", Location: "A", Qty: 5}),
				rec(t, 2, domain.TypeIssued, domain.Issued{SKU: "S", Location: "A", Qty: 3}),
			},
			want: domain.State{{SKU: "S", Location: "A"}: 2},
		},
		{
			name: "moved shifts between locations, even across nodes",
			recs: []eventlog.Record{
				rec(t, 1, domain.TypeReceived, domain.Received{SKU: "S", Location: "A", Qty: 5}),
				rec(t, 2, domain.TypeMoved, domain.Moved{SKU: "S", From: "A", To: "Z", Qty: 4}),
			},
			want: domain.State{{SKU: "S", Location: "A"}: 1, {SKU: "S", Location: "Z"}: 4},
		},
		{
			name:    "unknown type is a hard error",
			recs:    []eventlog.Record{{NodeID: "n1", Seq: 1, Type: "inventory.Teleported"}},
			wantErr: eventlog.ErrUnknownType,
		},
		{
			name:    "corrupt payload is a hard error",
			recs:    []eventlog.Record{{NodeID: "n1", Seq: 1, Type: domain.TypeReceived, Payload: []byte("{")}},
			wantErr: domain.ErrBadPayload,
		},
		{
			name:    "corrupt issued payload is a hard error",
			recs:    []eventlog.Record{{NodeID: "n1", Seq: 1, Type: domain.TypeIssued, Payload: []byte("{")}},
			wantErr: domain.ErrBadPayload,
		},
		{
			name:    "corrupt moved payload is a hard error",
			recs:    []eventlog.Record{{NodeID: "n1", Seq: 1, Type: domain.TypeMoved, Payload: []byte("{")}},
			wantErr: domain.ErrBadPayload,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := projection.FoldRecords[domain.State](domain.BalanceReducer{}, tc.recs)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("err = %v, want %v", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("FoldRecords: %v", err)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("state = %v, want %v", got, tc.want)
			}
			for k, v := range tc.want {
				if got[k] != v {
					t.Fatalf("state[%v] = %d, want %d (full: %v)", k, got[k], v, got)
				}
			}
		})
	}
}

func TestStateCloneIsIndependent(t *testing.T) {
	s := domain.State{{SKU: "S", Location: "A"}: 1}
	c := s.Clone()
	c[domain.Key{SKU: "S", Location: "A"}] = 99
	if s[domain.Key{SKU: "S", Location: "A"}] != 1 {
		t.Fatal("Clone must not alias the source")
	}
}

func TestZeroIsEmpty(t *testing.T) {
	if got := (domain.BalanceReducer{}).Zero(); len(got) != 0 {
		t.Fatalf("Zero = %v, want empty", got)
	}
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}
