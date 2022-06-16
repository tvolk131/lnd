package migration28

import (
	"fmt"

	"github.com/btcsuite/btcwallet/walletdb"
	"github.com/lightningnetwork/lnd/kvdb"
)

var (
	// paymentsRootBucket is the name of the top-level bucket within the
	// database that stores all data related to payments.
	paymentsRootBucket = []byte("payments-root-bucket")

	// paymentHtlcsBucket is a bucket where we'll store the information
	// about the HTLCs that were attempted for a payment.
	paymentHtlcsBucket = []byte("payment-htlcs-bucket")

	// htlcAttemptInfoKey is the key used as the prefix of an HTLC attempt
	// to store the info about the attempt that was done for the HTLC in
	// question. The HTLC attempt ID is concatenated at the end.
	htlcAttemptInfoKey = []byte("ai")

	// htlcSettleInfoKey is the key used as the prefix of an HTLC attempt
	// settle info, if any. The HTLC attempt ID is concatenated at the end.
	htlcSettleInfoKey = []byte("si")

	// htlcFailInfoKey is the key used as the prefix of an HTLC attempt
	// failure information, if any. The HTLC attempt ID is concatenated at
	// the end.
	htlcFailInfoKey = []byte("fi")
)

// htlcBucketKey creates a composite key from prefix and id where the result is
// simply the two concatenated. This is the exact copy from payments.go.
func htlcBucketKey(prefix, id []byte) []byte {
	key := make([]byte, len(prefix)+len(id))
	copy(key, prefix)
	copy(key[len(prefix):], id)
	return key
}

// getAttemptIdFromHtlcBucketKey extracts the htlc attempt id from a given key.
func getAttemptIdFromHtlcBucketKey(htlcBucketKey []byte) string {
	// At the time of this migration, all htlc bucket keys consist of a
	// prefix concatenated with the htlc's attempt id. And since all three
	// prefixes ("ai", "si", and "fi") are exactly 2 characters in length,
	// we can get the attempt id by simply 'hacking off' these first two
	// characters.
	return string(htlcBucketKey)[2:]
}

// MigratePayments will delete failed htlcs from all settled payments.
func MigratePayments(tx kvdb.RwTx) error {
	payments := tx.ReadWriteBucket(paymentsRootBucket)
	if payments == nil {
		return nil
	}

	if err := payments.ForEach(func(hash, v []byte) error {
		// Get the bucket which contains the payment, fail if the key
		// does not have a bucket.
		payment := payments.NestedReadWriteBucket(hash)
		if payment == nil {
			return fmt.Errorf("key must be a bucket: '%v'",
				string(paymentsRootBucket))
		}

		if payment.Get(paymentHtlcsBucket) != nil {
			return fmt.Errorf("key must be a bucket: '%v'",
				string(paymentHtlcsBucket))
		}

		htlcs := payment.NestedReadWriteBucket(paymentHtlcsBucket)
		if htlcs == nil {
			// Nothing to migrate for this payment.
			return nil
		}

		isSettled, err := paymentIsSettled(htlcs)
		if err != nil {
			return err
		}

		if isSettled {
			if err := deleteFailedHtlcs(htlcs, string(hash)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}

	return nil
}

// paymentIsSettled checks whether a payment is settled. Given how payments
// are stored at the time of this migration, this consists of iterating
// over all htlcs for the given payment and seeing if any contain a value
// under their settle info key ("si").
func paymentIsSettled(htlcs walletdb.ReadBucket) (bool, error) {
	// Collect attempt ids so that we can check attempts one-by-one
	// to avoid any bugs bbolt might have when invalidating cursors.
	// We're using a map here rather than a slice because each attempt id
	// maps to multiple keys within the htlcs bucket. Using a map will
	// prevent duplicate attempt ids. We're treating this map as a set, so
	// the value is not important - only the key.
	aids := make(map[string]bool)

	// First we collect all htlc attempt ids.
	if err := htlcs.ForEach(func(htlcBucketKey, v []byte) error {
		aids[getAttemptIdFromHtlcBucketKey(htlcBucketKey)] = true
		return nil
	}); err != nil {
		return false, err
	}

	// Next we go over these attempts, returning true
	// if we run into any containing settle info.
	for aid := range aids {
		aidKey := []byte(aid)

		settleInfoBucketKey := htlcBucketKey(htlcSettleInfoKey, aidKey)

		if htlcs.NestedReadBucket(settleInfoBucketKey) != nil {
			return false, fmt.Errorf("key must not be a bucket: '%v'",
				string(settleInfoBucketKey))
		}

		settleInfo := htlcs.Get(settleInfoBucketKey)
		if len(settleInfo) > 0 {
			return true, nil
		}
	}

	return false, nil
}

// deleteFailedHtlcs removes failed htlcs from a payment's htlcs bucket.
func deleteFailedHtlcs(htlcs kvdb.RwBucket, hash string) error {
	// Collect attempt ids so that we can migrate attempts one-by-one
	// to avoid any bugs bbolt might have when invalidating cursors.
	// We're using a map here rather than a slice because each attempt id
	// maps to multiple keys within the htlcs bucket. Using a map will
	// prevent duplicate attempt ids. We're treating this map as a set, so
	// the value is not important - only the key.
	aids := make(map[string]bool)

	// First we collect all htlc attempt ids.
	if err := htlcs.ForEach(func(htlcBucketKey, v []byte) error {
		aids[getAttemptIdFromHtlcBucketKey(htlcBucketKey)] = true
		return nil
	}); err != nil {
		return err
	}

	// Next we go over these attempts, and delete any containing fail info.
	for aid := range aids {
		aidKey := []byte(aid)

		failInfoBucketKey := htlcBucketKey(htlcFailInfoKey, aidKey)
		attemptInfoBucketKey := htlcBucketKey(
			htlcAttemptInfoKey, aidKey)
		settleInfoBucketKey := htlcBucketKey(htlcSettleInfoKey, aidKey)

		if htlcs.NestedReadBucket(failInfoBucketKey) != nil {
			return fmt.Errorf("key must not be a bucket: '%v'",
				string(failInfoBucketKey))
		}

		if htlcs.NestedReadBucket(attemptInfoBucketKey) != nil {
			return fmt.Errorf("key must not be a bucket: '%v'",
				string(attemptInfoBucketKey))
		}

		if htlcs.NestedReadBucket(settleInfoBucketKey) != nil {
			return fmt.Errorf("key must not be a bucket: '%v'",
				string(settleInfoBucketKey))
		}

		failInfo := htlcs.Get(failInfoBucketKey)
		if len(failInfo) > 0 {
			err := htlcs.Delete(failInfoBucketKey)
			if err != nil {
				return err
			}

			err = htlcs.Delete(attemptInfoBucketKey)
			if err != nil {
				return err
			}

			err = htlcs.Delete(settleInfoBucketKey)
			if err != nil {
				return err
			}
		}
	}

	return nil
}
