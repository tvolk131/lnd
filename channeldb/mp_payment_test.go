package channeldb

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestLazySessionKeyDeserialize tests that we can read htlc attempt session
// keys that were previously serialized as a private key as raw bytes.
func TestLazySessionKeyDeserialize(t *testing.T) {
	var b bytes.Buffer

	// Serialize as a private key.
	err := WriteElements(&b, priv)
	require.NoError(t, err)

	// Deserialize into [btcec.PrivKeyBytesLen]byte.
	attempt := HTLCAttemptInfo{}
	err = ReadElements(&b, &attempt.sessionKey)
	require.NoError(t, err)
	require.Zero(t, b.Len())

	sessionKey := attempt.SessionKey()
	require.Equal(t, priv, sessionKey)
}

// TestGetAttemptReturnsReferenceToPayment tests that GetAttempt returns a
// reference to the payment that it is called on, and not just a copy.
func TestGetAttemptReturnsReferenceToPayment(t *testing.T) {
	mpp := MPPayment{}
	attempt, err := mpp.GetAttempt(1234)
	require.Nil(t, attempt)
	require.Error(t, err)

	mpp.HTLCs = append(mpp.HTLCs, HTLCAttempt{
		HTLCAttemptInfo: HTLCAttemptInfo{
			AttemptID: 1234,
		},
	})

	attempt, err = mpp.GetAttempt(1234)
	require.NotNil(t, attempt)
	require.NoError(t, err)

	// Changing the htlc through the payment object should affect
	// the reference.
	mpp.HTLCs[0].Failure = &HTLCFailInfo{
		Reason: HTLCFailInternal,
	}
	require.NotNil(t, attempt.Failure)
}
