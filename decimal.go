package avro

import (
	"math/big"
)

// maxDecimalPrecision is the largest precision, and so scale, a decimal
// logical type may have. Decoding a decimal value computes 10^scale, so a
// schema from an untrusted OCF header or schema registry claiming a scale
// of a billion spent unbounded time and memory on every value. Real
// systems stop far below this: MySQL's largest decimal has precision 65,
// and Spark, Hive and Oracle stop at 38.
const maxDecimalPrecision = 100_000

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
