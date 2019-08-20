package expingest

import (
	"database/sql"
	"time"

	"github.com/stellar/go/exp/ingest/io"
	"github.com/stellar/go/exp/ingest/verify"
	"github.com/stellar/go/services/horizon/internal/db2/history"
	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/support/historyarchive"
	ilog "github.com/stellar/go/support/log"
	"github.com/stellar/go/xdr"
)

func (s *System) verifyState() error {
	if s.stateVerificationRunning {
		log.Warn("State verification is already running...")
		return nil
	}

	s.stateVerificationRunning = true
	startTime := time.Now()
	session := s.historySession.Clone()

	defer func() {
		log.WithField("duration", time.Since(startTime).Seconds()).Info("State verification finished")
		session.Rollback()
		s.stateVerificationRunning = false
	}()

	err := session.BeginTx(&sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
		ReadOnly:  true,
	})
	if err != nil {
		return errors.Wrap(err, "Error starting transaction")
	}

	historyQ := &history.Q{session}

	// Ensure the ledger is a checkpoint ledger
	ledgerSequence, err := historyQ.GetLastLedgerExpIngestNonBlocking()
	if err != nil {
		return err
	}

	localLog := log.WithFields(ilog.F{
		"subservice": "state_verify",
		"ledger":     ledgerSequence,
	})

	if !historyarchive.IsCheckpoint(ledgerSequence) {
		localLog.Info("Current ledger is not a checkpoint ledger. Cancelling...")
		return nil
	}

	localLog.Info("Starting state verification...")

	// Wait for stellar-core to publish HAS
	time.Sleep(20 * time.Second)

	stateReader, err := io.MakeSingleLedgerStateReader(
		s.session.Archive,
		&io.MemoryTempSet{}, // TODO change to postgres
		ledgerSequence,
	)
	if err != nil {
		return errors.Wrap(err, "Error running io.MakeSingleLedgerStateReader")
	}

	verifier := &verify.StateVerifier{
		StateReader:       stateReader,
		TempSet:           &verify.MemoryStateVerifyTempSet{},
		TransformFunction: transformEntry,
	}

	err = verifier.Open()
	if err != nil {
		return errors.Wrap(err, "Error opening StateVerifier")
	}
	defer verifier.Close()

	localLog.Info("Adding accounts to StateVerifier...")
	err = addAccountsToStateVerifier(verifier, historyQ)
	if err != nil {
		return errors.Wrap(err, "addAccountsToStateVerifier failed")
	}
	localLog.Info("Accounts added to StateVerifier")

	localLog.Info("Comparing with history archive...")
	ok, err := verifier.Verify()
	if err != nil {
		return errors.Wrap(err, "Error running verifier.Verify")
	}

	if !ok {
		// STATE IS INVALID! TODO: log reason why
		localLog.WithField("err", verifier.StateError()).Error("STATE IS INVALID!")
		// Save invalid state flag to a DB and panic.
		panic(true)
	}

	localLog.Info("State correct")
	return nil
}

func addAccountsToStateVerifier(verifier *verify.StateVerifier, q *history.Q) error {
	rows, err := q.StreamAccounts()
	if err != nil {
		return errors.Wrap(err, "Error running history.Q.StreamAccounts")
	}
	defer rows.Close()

	var account *xdr.AccountEntry

	for rows.Next() {
		var row history.AccountSigner
		if err := rows.Scan(&row.Account, &row.Signer, &row.Weight); err != nil {
			return errors.Wrap(err, "rows.Scan returned error")
		}

		if account == nil || account.AccountId.Address() != row.Account {
			if account != nil {
				// Sort signers
				account.Signers = xdr.SortSignersByKey(account.Signers)

				entry := xdr.LedgerEntry{
					Data: xdr.LedgerEntryData{
						Type:    xdr.LedgerEntryTypeAccount,
						Account: account,
					},
				}
				err := verifier.Add(entry)
				if err != nil {
					return err
				}
			}

			account = &xdr.AccountEntry{
				AccountId: xdr.MustAddress(row.Account),
				Signers:   []xdr.Signer{},
			}
		}

		if row.Account == row.Signer {
			// Master key
			account.Thresholds = [4]byte{
				// Store master weight only
				byte(row.Weight), 0, 0, 0,
			}
		} else {
			// Normal signer
			account.Signers = append(account.Signers, xdr.Signer{
				Key:    xdr.MustSigner(row.Signer),
				Weight: xdr.Uint32(row.Weight),
			})
		}
	}

	if err := rows.Err(); err != nil {
		return errors.Wrap(err, "rows.Err returned error")
	}

	// Add last created in a loop account
	entry := xdr.LedgerEntry{
		Data: xdr.LedgerEntryData{
			Type:    xdr.LedgerEntryTypeAccount,
			Account: account,
		},
	}
	err = verifier.Add(entry)
	if err != nil {
		return err
	}

	return nil
}

func transformEntry(entry xdr.LedgerEntry) (bool, xdr.LedgerEntry) {
	switch entry.Data.Type {
	case xdr.LedgerEntryTypeAccount:
		accountEntry := entry.Data.Account

		// We don't store account accounts with no signers (including master).
		// Ignore such accounts for now.
		if accountEntry.MasterKeyWeight() == 0 && len(accountEntry.Signers) == 0 {
			return true, xdr.LedgerEntry{}
		}

		// We store account id, master weight and signers only
		return false, xdr.LedgerEntry{
			Data: xdr.LedgerEntryData{
				Type: xdr.LedgerEntryTypeAccount,
				Account: &xdr.AccountEntry{
					AccountId: accountEntry.AccountId,
					Thresholds: [4]byte{
						// Store master weight only
						accountEntry.Thresholds[0], 0, 0, 0,
					},
					Signers: xdr.SortSignersByKey(accountEntry.Signers),
				},
			},
		}
	case xdr.LedgerEntryTypeTrustline:
		// Ignore
		return true, xdr.LedgerEntry{}
	case xdr.LedgerEntryTypeOffer:
		// TODO check offers
		return true, xdr.LedgerEntry{}
	case xdr.LedgerEntryTypeData:
		// Ignore
		return true, xdr.LedgerEntry{}
	default:
		panic("Invalid type")
	}
}
