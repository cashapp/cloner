package clone

import (
	"context"
	"database/sql"
	"reflect"
)

type DiffType string

const (
	Insert DiffType = "Insert"
	Update DiffType = "Update"
	Delete DiffType = "Delete"
)

type Diff struct {
	Type DiffType
	Row  *Row
}

// StreamDiff sends the changes need to make target become exactly like source
func StreamDiff(ctx context.Context, source RowStream, target RowStream, diffs chan Diff) error {
	advanceSource := true
	advanceTarget := true

	var err error
	var sourceRow *Row
	var targetRow *Row
	for {
		if advanceSource {
			sourceRow, err = source.Next(ctx)
			if err != nil {
				return err
			}
		}
		if advanceTarget {
			targetRow, err = target.Next(ctx)
			if err != nil {
				return err
			}
		}
		advanceSource = false
		advanceTarget = false

		if sourceRow != nil {
			if targetRow != nil {
				if sourceRow.ID < targetRow.ID {
					diffs <- Diff{Insert, sourceRow}
					advanceSource = true
					advanceTarget = false
				} else if sourceRow.ID > targetRow.ID {
					diffs <- Diff{Delete, targetRow}
					advanceSource = false
					advanceTarget = true
				} else if !reflect.DeepEqual(sourceRow.Data, targetRow.Data) {
					diffs <- Diff{Update, sourceRow}
					advanceSource = true
					advanceTarget = true
				} else {
					// Same!
					advanceSource = true
					advanceTarget = true
				}
			} else {
				diffs <- Diff{Insert, sourceRow}
				advanceSource = true
			}
		} else if targetRow != nil {
			diffs <- Diff{Delete, targetRow}
			advanceTarget = true
		} else {
			return nil
		}
	}
}

func DiffChunks(ctx context.Context, from *sql.Conn, to *sql.Conn, chunks chan Chunk, diffs chan Diff) error {
	for {
		select {
		case chunk, more := <-chunks:
			if more {
				fromStream := StreamChunk(ctx, from, chunk)
				toStream := StreamChunk(ctx, to, chunk)
				err := StreamDiff(ctx, fromStream, toStream, diffs)
				if err != nil {
					return err
				}
			} else {
				return nil
			}
		case <-ctx.Done():
			return nil
		}
	}
}
