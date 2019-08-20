package history

import (
	"database/sql"

	sq "github.com/Masterminds/squirrel"
	"github.com/stellar/go/services/horizon/internal/db2"
	"github.com/stellar/go/support/errors"
)

// StreamAccounts streams accounts from a DB. This currently only
// streams signers. Used in state verification code.
func (q *Q) StreamAccounts() (*sql.Rows, error) {
	// TODO index on `account`
	return sq.Select(
		"accounts_signers.account",
		"accounts_signers.signer",
		"accounts_signers.weight",
	).
		From("accounts_signers").
		OrderBy("accounts_signers.account ASC").
		RunWith(q.Session.GetTx().Tx).
		Query()
}

// AccountsForSigner returns a list of `AccountSigner` rows for a given signer
func (q *Q) AccountsForSigner(signer string, page db2.PageQuery) ([]AccountSigner, error) {
	sql := selectAccountSigners.Where("accounts_signers.signer = ?", signer)
	sql, err := page.ApplyToUsingCursor(sql, "accounts_signers.account", page.Cursor)
	if err != nil {
		return nil, errors.Wrap(err, "could not apply query to page")
	}

	var results []AccountSigner
	if err := q.Select(&results, sql); err != nil {
		return nil, errors.Wrap(err, "could not run select query")
	}

	return results, nil
}

// InsertAccountSigner creates a row in the accounts_signers table
func (q *Q) InsertAccountSigner(account, signer string, weight int32) error {
	sql := sq.Insert("accounts_signers").
		Columns("account", "signer", "weight").
		Values(account, signer, weight)
	_, err := q.Exec(sql)
	return err
}

// UpsertAccountSigner creates a row in the accounts_signers table
func (q *Q) UpsertAccountSigner(account, signer string, weight int32) error {
	sql := sq.Insert("accounts_signers").
		Columns("account", "signer", "weight").
		Values(account, signer, weight).
		Suffix("ON CONFLICT (signer, account) DO UPDATE SET weight=EXCLUDED.weight")

	_, err := q.Exec(sql)
	return err
}

// RemoveAccountSigner deletes a row in the accounts_signers table
func (q *Q) RemoveAccountSigner(account, signer string) error {
	sql := sq.Delete("accounts_signers").Where(sq.Eq{
		"account": account,
		"signer":  signer,
	})

	_, err := q.Exec(sql)
	return err
}

var selectAccountSigners = sq.Select("accounts_signers.*").From("accounts_signers")
