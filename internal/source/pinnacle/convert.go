package pinnacle

// AmericanToDecimal converts an American odds value (e.g. -181, +147) to a
// decimal-format multiplier (1.55, 2.47). Returns 0 for 0.
func AmericanToDecimal(a int) float64 {
	switch {
	case a == 0:
		return 0
	case a > 0:
		return 1 + float64(a)/100.0
	default:
		return 1 + 100.0/float64(-a)
	}
}

// AmericanToImpliedProb returns the raw implied probability of an American
// odds value, before bookmaker overround removal. Returns 0 for 0.
func AmericanToImpliedProb(a int) float64 {
	switch {
	case a == 0:
		return 0
	case a > 0:
		return 100.0 / float64(a+100)
	default:
		return float64(-a) / float64(-a+100)
	}
}

// NoVigTwoWay normalizes a two-outcome market by dividing raw implied
// probabilities by their sum. Returns nil if prices is not length 2 or if
// the sum is non-positive.
func NoVigTwoWay(prices []Price) []float64 {
	if len(prices) != 2 {
		return nil
	}
	p0 := AmericanToImpliedProb(prices[0].Price)
	p1 := AmericanToImpliedProb(prices[1].Price)
	s := p0 + p1
	if s <= 0 {
		return nil
	}
	return []float64{p0 / s, p1 / s}
}

// MaxRiskStake returns the maxRiskStake limit from a market's Limits slice,
// or 0 if not present.
func MaxRiskStake(limits []Limit) float64 {
	for _, l := range limits {
		if l.Type == "maxRiskStake" {
			return l.Amount
		}
	}
	return 0
}
