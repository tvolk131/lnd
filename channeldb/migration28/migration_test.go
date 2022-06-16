package migration28

import (
	"testing"

	"github.com/lightningnetwork/lnd/channeldb/migtest"
	"github.com/lightningnetwork/lnd/kvdb"
)

var (
	hexStr = migtest.Hex

	hash1Str   = "02acee76ebd53d00824410cf6adecad4f50334dac702bd5a2d3ba01b91709f0e"
	hash1      = hexStr(hash1Str)
	paymentID1 = hexStr("0000000000000001")
	attemptID1 = hexStr("0000000000000001")
	attemptID2 = hexStr("0000000000000002")

	hash2Str   = "62eb3f0a48f954e495d0c14ac63df04a67cefa59dafdbcd3d5046d1f5647840c"
	hash2      = hexStr(hash2Str)
	paymentID2 = hexStr("0000000000000002")
	attemptID3 = hexStr("0000000000000003")

	hash3Str = "99eb3f0a48f954e495d0c14ac63df04af8cefa59dafdbcd3d5046d1f564784d1"
	hash3    = hexStr(hash3Str)

	hash4Str = "e312b334ac65ccf950d2411a04f43d7de7143685d87fbf270403433adf2b4961"
	hash4    = hexStr(hash4Str)

	// failing1 will fail because all payment hashes should point to sub
	// buckets containing payment data.
	failing1 = map[string]interface{}{
		hash1: "bogus",
	}

	// failing2 will fail because the "payment-htlcs-bucket" key must point
	// to an actual bucket or be non-existent, but never point to a value.
	failing2 = map[string]interface{}{
		hash1: map[string]interface{}{
			"payment-htlcs-bucket": "bogus",
		},
	}

	// failing3 will fail because the attempt
	// info is a bucket instead of a value.
	failing3 = map[string]interface{}{
		hash1: map[string]interface{}{
			"payment-creation-info": "aaaa",
			"payment-fail-info":     "bbbb",
			"payment-htlcs-bucket": map[string]interface{}{
				"ai" + attemptID2: map[string]interface{}{},
				"si" + attemptID2: "cccc",
				"fi" + attemptID2:  "dddd",
			},
			"payment-sequence-key": paymentID1,
		},
	}

	// failing4 will fail because the settle
	// info is a bucket instead of a value.
	failing4 = map[string]interface{}{
		hash1: map[string]interface{}{
			"payment-creation-info": "aaaa",
			"payment-fail-info":     "bbbb",
			"payment-htlcs-bucket": map[string]interface{}{
				"ai" + attemptID1: "cccc",
				"si" + attemptID1: map[string]interface{}{},
				"fi" + attemptID2:  "dddd",
			},
			"payment-sequence-key": paymentID1,
		},
	}

	// failing5 will fail because the fail
	// info is a bucket instead of a value.
	failing5 = map[string]interface{}{
		hash1: map[string]interface{}{
			"payment-creation-info": "aaaa",
			"payment-fail-info":     "bbbb",
			"payment-htlcs-bucket": map[string]interface{}{
				"ai" + attemptID1: "cccc",
				"si" + attemptID1: "dddd",
				"fi" + attemptID2:  map[string]interface{}{},
			},
			"payment-sequence-key": paymentID1,
		},
	}

	// pre is a sample snapshot (with fake values) before migration.
	pre = map[string]interface{}{
		hash1: map[string]interface{}{
			"payment-creation-info": "aaaa",
			"payment-fail-info":     "bbbb",
			"payment-htlcs-bucket": map[string]interface{}{
				"ai" + attemptID1: "cccc",
				"fi" + attemptID1: "dddd",
			},
			"payment-sequence-key": paymentID1,
		},
		hash2: map[string]interface{}{
			"payment-creation-info": "eeee",
			"payment-htlcs-bucket": map[string]interface{}{
				"ai" + attemptID2: "ffff",
				"fi" + attemptID2: "gggg",
				"ai" + attemptID3: "hhhh",
				"si" + attemptID3: "iiii",
			},
			"payment-sequence-key": paymentID2,
		},
		hash3: map[string]interface{}{
			"payment-creation-info": "aaaa",
			"payment-fail-info":     "bbbb",
			"payment-sequence-key":  paymentID1,
		},
		hash4: map[string]interface{}{
			"payment-creation-info": "cccc",
			"payment-htlcs-bucket": map[string]interface{}{
				"ai" + attemptID1: "eeee",
				"si" + attemptID1: "ffff",
				"fi" + attemptID1:  "gggg",
			},
			"payment-sequence-key":  paymentID2,
		},
	}

	// post is the expected data after migration.
	post = map[string]interface{}{
		hash1: map[string]interface{}{
			"payment-creation-info": "aaaa",
			"payment-fail-info":     "bbbb",
			"payment-htlcs-bucket": map[string]interface{}{
				"ai" + attemptID1: "cccc",
				"fi" + attemptID1: "dddd",
			},
			"payment-sequence-key": paymentID1,
		},
		hash2: map[string]interface{}{
			"payment-creation-info": "eeee",
			"payment-htlcs-bucket": map[string]interface{}{
				"ai" + attemptID3: "hhhh",
				"si" + attemptID3: "iiii",
			},
			"payment-sequence-key": paymentID2,
		},
		hash3: map[string]interface{}{
			"payment-creation-info": "aaaa",
			"payment-fail-info":     "bbbb",
			"payment-sequence-key":  paymentID1,
		},
		hash4: map[string]interface{}{
			"payment-creation-info": "cccc",
			"payment-htlcs-bucket": map[string]interface{}{
			},
			"payment-sequence-key":  paymentID2,
		},
	}
)

// TestMigrateHtlcAttempts tests that migration htlc attempts to the flattened
// structure succeeds.
func TestMigrateHtlcAttempts(t *testing.T) {
	var paymentsRootBucket = []byte("payments-root-bucket")
	tests := []struct {
		name       string
		shouldFail bool
		pre        map[string]interface{}
		post       map[string]interface{}
	}{
		{
			name:       "migration ok",
			shouldFail: false,
			pre:        pre,
			post:       post,
		},
		{
			name:       "non-bucket payments-root-bucket",
			shouldFail: true,
			pre:        failing1,
			post:       failing1,
		},
		{
			name:       "non-bucket payment-htlcs-bucket",
			shouldFail: true,
			pre:        failing2,
			post:       failing2,
		},
		{
			name:       "bucket attempt info",
			shouldFail: true,
			pre:        failing3,
			post:       failing3,
		},
		{
			name:       "bucket settle info",
			shouldFail: true,
			pre:        failing4,
			post:       failing4,
		},
		{
			name:       "bucket fail info",
			shouldFail: true,
			pre:        failing5,
			post:       failing5,
		},
	}

	for _, test := range tests {
		test := test

		migtest.ApplyMigration(
			t,
			func(tx kvdb.RwTx) error {
				return migtest.RestoreDB(
					tx, paymentsRootBucket, test.pre,
				)
			},
			func(tx kvdb.RwTx) error {
				return migtest.VerifyDB(
					tx, paymentsRootBucket, test.post,
				)
			},
			MigratePayments,
			test.shouldFail,
		)
	}
}
