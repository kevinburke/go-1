// Package verify provides helpers used for verifying if the ingested data is
// correct.
package verify

import (
	"crypto/sha256"
	"encoding/base64"
	stdio "io"

	"github.com/stellar/go/exp/ingest/io"
	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/xdr"
)

var (
	errKeyNotFound      = errors.New("Key not found")
	errKeyAlreadyExists = errors.New("Key already exists")
)

// TempSet is an interface that must be implemented by stores that
// hold temporary set of objects for StateVerifier. The implementation
// does not need to be thread-safe.
// TempSet method are always called in the following order:
//   - Open
//   - Multiple Add
//   - Multiple Remove
//   - IsEmpty
//   - Close
// So it's possible to batch Add and Remove calls.
type StateVerifyTempSet interface {
	Open() error
	// Add adds the key to the set. It should return error if the key already
	// exists.
	Add(key string) error
	// Remove removes the key from the set. It should return error if the key is
	// not found.
	Remove(key string) error
	IsEmpty() (bool, error)
	Close() error
}

// TransformLedgerEntryFunction is a function that transforms ledger entry
// into a form that should be compared to checkpoint state. It can be also used
// to decide if the given entry should be ignored during verification.
// Sometimes the application needs only specific type entries or specific fields
// for a given entry type. Use this function to create a common form of an entry
// that will be used for equality check.
type TransformLedgerEntryFunction func(xdr.LedgerEntry) (ignore bool, newEntry xdr.LedgerEntry)

// StateVerifier verifies if ledger entries provided by Add method are the same
// as in the checkpoint ledger entries provided by SingleLedgerStateReader.
// Users should always call `Open` and `Close`.
type StateVerifier struct {
	StateReader *io.SingleLedgerStateReader
	// TempSet is a StateVerifyTempSet implementation.
	TempSet StateVerifyTempSet
	// TransformFunction transforms (or ignores) ledger entries streamed from
	// checkpoint buckets to match the form added by `Add`. Read
	// TransformLedgerEntryFunction godoc for more information.
	TransformFunction TransformLedgerEntryFunction

	stateCorruptedError error
}

func (v *StateVerifier) Open() error {
	return v.TempSet.Open()
}

func (v *StateVerifier) Close() error {
	return v.TempSet.Close()
}

func (v *StateVerifier) Add(entry xdr.LedgerEntry) error {
	key, err := v.entryToKey(entry)
	if err != nil {
		return err
	}

	return v.TempSet.Add(key)
}

func (v *StateVerifier) entryToKey(entry xdr.LedgerEntry) (string, error) {
	xdrEncoded, err := entry.MarshalBinary()
	if err != nil {
		return "", err
	}

	sum := sha256.Sum256(xdrEncoded)
	key := base64.StdEncoding.EncodeToString(sum[:])
	return key, nil
}

// Verify checks if (transformed) ledger entries from checkpoint buckets match
// entries provided by `Add`.
// Returns (false, nil) if state found to be invalid. Use `StateError` to get
// actual reason why the state is found to be invalid. For other errors, it
// returns (true, err).
func (v *StateVerifier) Verify() (bool, error) {
	for {
		entryChange, err := v.StateReader.Read()
		if err != nil {
			if err == stdio.EOF {
				break
			}
			return true, err
		}

		entry := entryChange.MustState()
		preTransformEntry := entry

		var ignore bool
		if v.TransformFunction != nil {
			ignore, entry = v.TransformFunction(entry)
			if ignore {
				continue
			}
		}

		key, err := v.entryToKey(entry)
		if err != nil {
			return true, err
		}

		err = v.TempSet.Remove(key)
		if err != nil {
			if err == errKeyNotFound {
				// We ignore errors here because "corrupted state error" has a priority
				entryMarshaled, _ := entry.MarshalBinary()
				preTransformEntryMarshaled, _ := preTransformEntry.MarshalBinary()

				v.stateCorruptedError = errors.Errorf(
					"Could not find entry %s (transformed = %s, key = %s) in added entries",
					base64.StdEncoding.EncodeToString(preTransformEntryMarshaled),
					base64.StdEncoding.EncodeToString(entryMarshaled),
					key,
				)
				return false, nil
			}
			return true, err
		}
	}

	empty, err := v.TempSet.IsEmpty()
	if err != nil {
		v.stateCorruptedError = errors.New("Some entries added in Add has not been found in history archives")
		return true, err
	}

	return empty, nil
}

// StateError returns an explanation why the state is corrupted if
// Verify returns false.
func (v *StateVerifier) StateError() error {
	return v.stateCorruptedError
}
