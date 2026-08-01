package domain

import (
	"math"
	"testing"
	"time"
)

func widget() Item {
	return Item{
		SKU:           "WIDGET",
		Description:   "Blue widget",
		BaseUoM:       "EA",
		AltUoM:        map[UoM]float64{"CASE": 12, "PALLET": 480},
		LotTracked:    true,
		ShelfLifeDays: 30,
	}
}

func TestItemToBase(t *testing.T) {
	tests := []struct {
		name    string
		item    Item
		qty     float64
		uom     UoM
		want    float64
		wantErr string // "" means no error
	}{
		{name: "base uom passes through", item: widget(), qty: 5, uom: "EA", want: 5},
		{name: "alternate uom multiplies by factor", item: widget(), qty: 3, uom: "CASE", want: 36},
		{name: "second alternate uom", item: widget(), qty: 2, uom: "PALLET", want: 960},
		{name: "unknown uom is rejected", item: widget(), qty: 1, uom: "TONNE", wantErr: RuleUoMValid},
		{name: "empty uom is rejected", item: widget(), qty: 1, uom: "", wantErr: RuleUoMValid},
		{
			name:    "non-positive factor in master is rejected",
			item:    Item{SKU: "X", BaseUoM: "EA", AltUoM: map[UoM]float64{"BAD": 0}},
			qty:     1,
			uom:     "BAD",
			wantErr: RuleUoMValid,
		},
		{name: "zero quantity is rejected", item: widget(), qty: 0, uom: "EA", wantErr: RuleQtyPositive},
		{name: "negative quantity is rejected", item: widget(), qty: -1, uom: "EA", wantErr: RuleQtyPositive},
		{name: "NaN quantity is rejected", item: widget(), qty: math.NaN(), uom: "EA", wantErr: RuleQtyPositive},
		{name: "infinite quantity is rejected", item: widget(), qty: math.Inf(1), uom: "EA", wantErr: RuleQtyPositive},
		{
			name:    "NaN conversion factor in master is rejected",
			item:    Item{SKU: "X", BaseUoM: "EA", AltUoM: map[UoM]float64{"BAD": math.NaN()}},
			qty:     1,
			uom:     "BAD",
			wantErr: RuleUoMValid,
		},
		{
			name:    "infinite conversion factor in master is rejected",
			item:    Item{SKU: "X", BaseUoM: "EA", AltUoM: map[UoM]float64{"BAD": math.Inf(1)}},
			qty:     1,
			uom:     "BAD",
			wantErr: RuleUoMValid,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.item.ToBase(tt.qty, tt.uom)
			if tt.wantErr != "" {
				if !IsViolation(err, tt.wantErr) {
					t.Fatalf("err = %v, want violation of %s", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("ToBase() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLotExpiredAt(t *testing.T) {
	expiry := time.Date(2026, 7, 30, 0, 0, 0, 0, time.UTC)
	lot := Lot{ID: "L1", SKU: "WIDGET", ExpiresOn: expiry}
	tests := []struct {
		name string
		lot  Lot
		at   time.Time
		want bool
	}{
		{"day before is fine", lot, expiry.AddDate(0, 0, -1), false},
		{"expiry day itself is still usable", lot, expiry, false},
		{"later that same day is still usable", lot, expiry.Add(23 * time.Hour), false},
		{"day after is expired", lot, expiry.AddDate(0, 0, 1), true},
		{"no expiry date never expires", Lot{ID: "L2", SKU: "WIDGET"}, expiry.AddDate(9, 0, 0), false},
		{
			// ExpiresOn is a calendar date. Only its year/month/day matter: a lot
			// dated 2026-07-30 in +07:00 is usable for all of 2026-07-30 in UTC and
			// must not expire early just because its instant lands on 07-29Z.
			"non-utc expiry date is read as its calendar day",
			Lot{ID: "L3", SKU: "WIDGET", ExpiresOn: time.Date(2026, 7, 30, 0, 0, 0, 0, time.FixedZone("ICT", 7*3600))},
			time.Date(2026, 7, 30, 23, 0, 0, 0, time.UTC),
			false,
		},
		{
			"non-utc expiry date expires the next utc day",
			Lot{ID: "L3", SKU: "WIDGET", ExpiresOn: time.Date(2026, 7, 30, 0, 0, 0, 0, time.FixedZone("ICT", 7*3600))},
			time.Date(2026, 7, 31, 0, 0, 0, 0, time.UTC),
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.lot.ExpiredAt(tt.at); got != tt.want {
				t.Fatalf("ExpiredAt() = %v, want %v", got, tt.want)
			}
		})
	}
}
