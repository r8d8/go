package history

import (
	"fmt"
	sq "github.com/Masterminds/squirrel"
	"github.com/stellar/go/xdr"
)

// Aggregate bucket trades according to paging information
func (q *Q) SelectTradeAggregations(dest interface{}, baseAsset xdr.Asset, counterAsset xdr.Asset, resolution, from int64, to int64, limit uint64, order string) error {

	baseAssetId, err := q.GetAssetID(baseAsset)
	if err != nil {
		return err
	}

	counterAssetId, err := q.GetAssetID(counterAsset)
	if err != nil {
		return err
	}

	//prepare the trade buckets sql statement based on canonical ordering of assets
	orderPreserved, baseAssetId, counterAssetId := getCanonicalAssetOrder(baseAssetId, counterAssetId)
	var bucketSelect sq.SelectBuilder
	if orderPreserved {
		bucketSelect = q.bucketTrades(resolution)
	} else {
		bucketSelect = q.reverseBucketTrades(resolution)
	}

	//adjust time range and apply filters
	from, to = fixTimeRange(from, to, resolution)
	bucketSelect = bucketSelect.Where(sq.GtOrEq{"ledger_closed_at": toTimestamp(from)}).
		Where(sq.Lt{"ledger_closed_at": toTimestamp(to)})

	s := sq.Select(
		"timestamp",
		"count(*) as count",
		"sum(base_amount) as base_volume",
		"sum(counter_amount) as counter_volume",
		"avg(price) as avg",
		"max(price) as high",
		"min(price) as low",
		"first(price) as open",
		"last(price) as close").
		FromSelect(bucketSelect, "htrd").
		GroupBy("timestamp").
		Limit(limit).
		OrderBy("timestamp " + order)
	return q.Select(dest, s)
}

// formatBucketTimestampSelect formats a sql select clause for a bucketed timestamp, based on given resolution
func formatBucketTimestampSelect(resolution int64) string {
	return fmt.Sprintf("div(cast((extract(epoch from ledger_closed_at) * 1000 ) as bigint), %d)*%d as timestamp",
		resolution, resolution)
}

// BucketTrades provides a helper to filter rows from the `history_trades` table in
// a compact form, with a timestamp rounded to resolution.
// For external use: see BucketTradesForAssetPair
func (q *Q) bucketTrades(resolution int64) sq.SelectBuilder {
	return sq.Select(
		formatBucketTimestampSelect(resolution),
		"base_asset_id",
		"base_amount",
		"counter_asset_id",
		"counter_amount",
		"counter_amount::float/base_amount as price",
	).From("history_trades")
}

// ReverseBucketTrades provides a helper to filter rows from the `history_trades` table in
// a compact form, with a timestamp rounded to resolution and reversed base/counter.
// For external use: see BucketTradesForAssetPair
func (q *Q) reverseBucketTrades(resolution int64) sq.SelectBuilder {
	return sq.Select(
		formatBucketTimestampSelect(resolution),
		"counter_asset_id as base_asset_id",
		"counter_amount as base_amount",
		"base_asset_id as counter_asset_id",
		"base_amount as counter_amount",
		"base_amount::float/counter_amount as price",
	).From("history_trades")
}

func fixTimeRange(startTime int64, endTime int64, resolution int64) (startTimeFixed int64, endTimeFixed int64) {
	// Push lower boundary to next bucket start
	if startTime%resolution != 0 {
		startTimeFixed =
			int64(startTime/resolution) * (resolution + 1)
	} else {
		startTimeFixed = startTime
	}

	// Pull upper boundary to previous bucket start
	endTimeFixed = int64(endTime/resolution) * resolution
	return
}
