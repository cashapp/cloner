package clone

import (
	"context"
	"database/sql"
)

func Write(ctx context.Context, conn *sql.Conn, batches chan Batch) error {
	for {
		select {
		case _ /*batch*/, more := <-batches:
			if more {
				// TODO write the batch
			} else {
				return nil
			}
		case <-ctx.Done():
			return nil
		}
	}
}
