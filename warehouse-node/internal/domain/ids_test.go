package domain

import "testing"

func TestEventIDString(t *testing.T) {
	got := EventID{NodeID: "wh-a", Seq: 42}.String()
	if got != "wh-a/42" {
		t.Fatalf("got %q, want %q", got, "wh-a/42")
	}
}

func TestLocationTypeValid(t *testing.T) {
	tests := []struct {
		name string
		in   LocationType
		want bool
	}{
		{"receiving", LocReceiving, true},
		{"bulk", LocBulk, true},
		{"pick", LocPick, true},
		{"staging", LocStaging, true},
		{"quarantine", LocQuarantine, true},
		{"empty", LocationType(""), false},
		{"nonsense", LocationType("mezzanine"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.in.Valid(); got != tt.want {
				t.Fatalf("Valid() = %v, want %v", got, tt.want)
			}
		})
	}
}
