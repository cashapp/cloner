// Package autotx provides wrappers for managing the lifecycle of a transaction and handling retries.
//
// With the raw sql library, a transactional operation looks something like
//
//     tx, err := db.BeginTx(ctx)
//     if err != nil {
//         return err
//     }
//     _, err := tx.ExecContext(ctx, "INSERT INTO dinosaurs VALUES (?)", "Nothronychus")
//     if err != nil {
//         tx.Rollback()
//         return err
//     }
//     return tx.Commit()
//
// It can get repetitive and be easy to forget to call Rollback on every error. Also, in this example,
// panics are not handled, and the error from Rollback is discarded.
//
// Manage the transaction lifecycle
//
// Using the The Transact function wraps the transactional operation, committing on completion, or rolling back the
// transaction on error or panic. This can simplify operations significantly. The operation above would look like
//
//     autotx.Transact(ctx, db, func(tx *sql.Tx) error {
//         _, err := tx.ExecContext(ctx, "INSERT INTO dinosaurs VALUES (?)", "Nothronychus")
//         return err
//     })
//
// The TransactWithRetry function behaves identically like the Transact function, but will retry errors returned
// by the operation according to policies defined in autotx.RetryOptions{}.
//
//     autotx.TransactWithRetry(ctx, db, autotx.RetryOptions{}, func(tx *sql.Tx) error {
//         _, err := tx.ExecContext(ctx, "INSERT INTO dinosaurs VALUES (?)", "Nothronychus")
//         return err
//     }
package autotx
