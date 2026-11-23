package avro

import (
	"math/big"
)

// maxDecimalPrecision is the largest precision, and so scale, a decimal
// logical type may have. Decoding a decimal value computes 10^scale, and the
// *big.Rat it returns keeps that power as its denominator, so a schema from
// an untrusted OCF header or schema registry decides what each value costs:
// at scale 100000 a two-byte value took a millisecond and 80 KB of heap. At
// 1000 it costs a few microseconds and about 1 KB. PostgreSQL's numeric
// allows a declared precision of up to 1000, the most of any common system:
// MySQL stops at 65, and Spark, Hive and Oracle at 38.
const maxDecimalPrecision = 1000

// checkDecimalPrecision checks if the value exceeds the specified precision.
// returns the number of digits and whether it is valid.
func checkDecimalPrecision(value *big.Int, prec int) (int, bool) {
	unscaledAbsStr := new(big.Int).Abs(value).String()
	numDigits := len(unscaledAbsStr)

	if len(unscaledAbsStr) > prec {
		return numDigits, false
	}

	return numDigits, true
}
