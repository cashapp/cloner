package clone

import (
	"context"
)

type Batch struct {
	Type  DiffType
	Table *Table
	Rows  []*Row
}

// BatchWrites consumes diffs and batches them up into batches by type and table
func BatchWrites(ctx context.Context, diffs chan Diff, batches chan Batch) error {
	batchesByType := make(map[DiffType]map[string]Batch)

readChannel:
	for {
		select {
		case diff, more := <-diffs:
			if more {
				batchesByTable, ok := batchesByType[diff.Type]
				if !ok {
					batchesByTable = make(map[string]Batch)
					batchesByType[diff.Type] = batchesByTable
				}
				batch, ok := batchesByTable[diff.Row.Table.Name]
				if !ok {
					batch = Batch{diff.Type, diff.Row.Table, nil}
				}
				batch.Rows = append(batch.Rows, diff.Row)

				if len(batch.Rows) >= diff.Row.Table.Config.WriteBatchSize {
					// Batch is full send it
					batches <- batch
					// and clear it
					batch.Rows = nil
				}

				batchesByTable[diff.Row.Table.Name] = batch
			} else {
				break readChannel
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// Write the final unfilled batches
	// TODO this means we will always write the final unfilled batch of _every_ table after we've processed all
	//      other tables, I wonder if we should flush all the unfilled batches "regularly"
	for _, batchesByTable := range batchesByType {
		for _, batch := range batchesByTable {
			if len(batch.Rows) > 0 {
				select {
				case batches <- batch:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}
	}
	close(batches)

	return nil
}

// BatchTableWrites consumes diffs for a single table and batches them up into batches by type
func BatchTableWrites(ctx context.Context, diffs chan Diff, batches chan Batch) error {
	batchesByType := make(map[DiffType]Batch)

	for {
		select {
		case diff, more := <-diffs:
			if !more {
				// Write the final unfilled batches
				for _, batch := range batchesByType {
					if len(batch.Rows) > 0 {
						select {
						case batches <- batch:
						case <-ctx.Done():
							return ctx.Err()
						}
					}
				}
				return nil
			}

			batch, ok := batchesByType[diff.Type]
			if !ok {
				batch = Batch{diff.Type, diff.Row.Table, nil}
			}
			batch.Rows = append(batch.Rows, diff.Row)

			if len(batch.Rows) >= diff.Row.Table.Config.WriteBatchSize {
				select {
				case batches <- batch:
				case <-ctx.Done():
					return ctx.Err()
				}

				// and clear it
				batch.Rows = nil
			}

			batchesByType[diff.Type] = batch
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// BatchTableWritesSync synchronously batches up diffs into batches by type
func BatchTableWritesSync(diffs []Diff) ([]Batch, error) {
	var batches []Batch

	batchesByType := make(map[DiffType]Batch)
	for _, diff := range diffs {
		batch, ok := batchesByType[diff.Type]
		if !ok {
			batch = Batch{diff.Type, diff.Row.Table, nil}
		}
		batch.Rows = append(batch.Rows, diff.Row)

		if len(batch.Rows) >= diff.Row.Table.Config.WriteBatchSize {
			batches = append(batches, batch)
			// Clear the full batch
			batch.Rows = nil
		}

		batchesByType[diff.Type] = batch
	}

	// Write the final unfilled batches
	for _, batch := range batchesByType {
		if len(batch.Rows) > 0 {
			batches = append(batches, batch)
		}
	}

	return batches, nil
}
