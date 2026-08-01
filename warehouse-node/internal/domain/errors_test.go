package domain

import (
	"errors"
	"fmt"
	"testing"
)

func TestRuleErrorMessageNamesTheRule(t *testing.T) {
	err := Violation(RuleStockNonNegative, "location %s would go to %v", LocationCode("PICK-01"), -3.0)
	want := "invariant violated [stock_non_negative]: location PICK-01 would go to -3"
	if err.Error() != want {
		t.Fatalf("Error() = %q, want %q", err.Error(), want)
	}
}

func TestIsViolation(t *testing.T) {
	tests := []struct {
		name string
		err  error
		rule string
		want bool
	}{
		{"matching rule", Violation(RuleUoMValid, "x"), RuleUoMValid, true},
		{"different rule", Violation(RuleUoMValid, "x"), RuleSKUExists, false},
		{"wrapped matching rule", fmt.Errorf("wrapped: %w", Violation(RuleSKUExists, "x")), RuleSKUExists, true},
		{"foreign error", errors.New("boom"), RuleSKUExists, false},
		{"nil error", nil, RuleSKUExists, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsViolation(tt.err, tt.rule); got != tt.want {
				t.Fatalf("IsViolation() = %v, want %v", got, tt.want)
			}
		})
	}
}
