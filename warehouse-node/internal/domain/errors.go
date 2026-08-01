package domain

import (
	"errors"
	"fmt"
)

// Rule names. Every node-enforced invariant failure carries one of these so the
// operator sees exactly which rule stopped the command, and so tests can assert
// on the rule rather than on message text.
const (
	RuleStockNonNegative     = "stock_non_negative"
	RuleReservationAvailable = "reservation_within_available"
	RuleLotNotExpired        = "lot_not_expired"
	RuleUoMValid             = "uom_valid_for_item"
	RuleSKUExists            = "sku_exists_in_item_master"
	RuleLocationExists       = "location_exists"
	RuleLotRequired          = "lot_required_for_tracked_item"
	RuleAggregateState       = "aggregate_state"
	RuleQtyPositive          = "quantity_positive"
)

// Adjustment reason codes. The first is produced by a node closing a stock
// count; the rest are produced only by central when it compensates an event.
const (
	ReasonCountVariance       = "count_variance"
	ReasonPOOverReceipt       = "po_overreceipt"
	ReasonDuplicateReceipt    = "duplicate_receipt"
	ReasonUnknownSKU          = "unknown_sku"
	ReasonTransferRejected    = "transfer_rejected"
	ReasonTransferOverReceipt = "transfer_overreceipt"
)

// RuleError reports that a command violated a named invariant. Nothing was
// appended to the log when one of these is returned.
type RuleError struct {
	Rule   string
	Detail string
}

// Error names the violated rule so the message is actionable on its own.
func (e RuleError) Error() string {
	return fmt.Sprintf("invariant violated [%s]: %s", e.Rule, e.Detail)
}

// Violation builds a RuleError for the named rule.
func Violation(rule, format string, args ...any) error {
	return RuleError{Rule: rule, Detail: fmt.Sprintf(format, args...)}
}

// IsViolation reports whether err is (or wraps) a violation of the named rule.
func IsViolation(err error, rule string) bool {
	var re RuleError
	return errors.As(err, &re) && re.Rule == rule
}
