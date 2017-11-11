package history

import (
	"fmt"
	"github.com/stellar/go/xdr"
	sq "github.com/Masterminds/squirrel"
)

// Return a query for bucketed trades
func (q *Q) BucketTradesForAssetPair(baseAsset xdr.Asset, counterAsset xdr.Asset, bucketResolution int64)(err error, retval *TradeAggregationsQ) {
	baseAssetId, err := q.GetAssetID(baseAsset)
	if err != nil {
		return
	}

	counterAssetId, err := q.GetAssetID(counterAsset)
	if err != nil {
		return
	}

	flipped, baseAssetId, counterAssetId := getCanonicalAssetOrder(baseAssetId, counterAssetId)
	var trades *TradeAggregationsQ

	if !flipped {
		trades = q.bucketTrades(bucketResolution)
	} else {
		trades = q.reverseBucketTrades(bucketResolution)
	}

	return nil, trades.forAssetPair(baseAssetId, counterAssetId)
}

//Filter by asset pair. This function is private to ensure that correct order and proper select statement are coupled
func (q *TradeAggregationsQ) forAssetPair(baseAssetId int64, counterAssetId int64) *TradeAggregationsQ {
	q.sql = q.sql.Where(sq.Eq{"base_asset_id": baseAssetId, "counter_asset_id": counterAssetId})
	return q
}


// Aggregate bucket trades according to paging information
func (q *TradeAggregationsQ) SelectAggregateByBucket(dest interface{}, limit uint64, order string) error {
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
		FromSelect(q.sql, "htrd").
		GroupBy("timestamp").
		Limit(limit).
		OrderBy("timestamp " + order)
	return q.parent.Select(dest, s)
}


// formatBucketTimestampSelect formats a sql select clause for a bucketed timestamp, based on given resolution
func formatBucketTimestampSelect(resolution int64) string {
	return fmt.Sprintf("div(cast((extract(epoch from ledger_closed_at) * 1000 ) as bigint), %d)*%d as timestamp",
		resolution, resolution)
}

// BucketTrades provides a helper to filter rows from the `history_trades` table in
// a compact form, with a timestamp rounded to resolution.
// For external use: see BucketTradesForAssetPair
func (q *Q) bucketTrades(resolution int64) *TradeAggregationsQ {
	return &TradeAggregationsQ{
		parent: q,
		sql: sq.Select(
			formatBucketTimestampSelect(resolution),
			"base_asset_id",
			"base_amount",
			"counter_asset_id",
			"counter_amount",
			"counter_amount::float/base_amount as price",
		).From("history_trades"),
	}
}

// ReverseBucketTrades provides a helper to filter rows from the `history_trades` table in
// a compact form, with a timestamp rounded to resolution and reversed base/counter.
// For external use: see BucketTradesForAssetPair
func (q *Q) reverseBucketTrades(resolution int64) *TradeAggregationsQ {
	return &TradeAggregationsQ{
		parent: q,
		sql: sq.Select(
			formatBucketTimestampSelect(resolution),
			"counter_asset_id as base_asset_id",
			"counter_amount as base_amount",
			"base_asset_id as counter_asset_id",
			"base_amount as counter_amount",
			"base_amount::float/counter_amount as price",
		).From("history_trades"),
	}
}


func (q *TradeAggregationsQ) FromStartTime(from int64) *TradeAggregationsQ {
	q.sql = q.sql.Where(sq.GtOrEq{"ledger_closed_at": toTimestamp(from)})
	return q
}

func (q *TradeAggregationsQ) FromEndTime(to int64) *TradeAggregationsQ {
	q.sql = q.sql.Where(sq.Lt{"ledger_closed_at": toTimestamp(to)})
	return q
}

func (q *TradeAggregationsQ) OrderBy(order string) *TradeAggregationsQ {
	q.sql = q.sql.OrderBy("ledger_closed_at " + order)
	return q
}
