package history

import (
	"fmt"
	sq "github.com/Masterminds/squirrel"
	"github.com/stellar/go/services/horizon/internal/db2"
)

// GetTradeAggregationsQ initializes a TradeAggregationsQ query builder based on the required parameters
func (q Q) GetTradeAggregationsQ(baseAssetId int64, counterAssetId int64, resolution int64, pagingParams db2.PageQuery) *TradeAggregationsQ {
	return &TradeAggregationsQ{
		baseAssetId:    baseAssetId,
		counterAssetId: counterAssetId,
		resolution:     resolution,
		pagingParams:   pagingParams,
	}
}

// WithStartTime adds an optional lower time boundary filter to the trades being aggregated
func (q *TradeAggregationsQ) WithStartTime(startTime int64) *TradeAggregationsQ {
	q.startTime = startTime
	// Round lower boundary up, if start time is in the middle of a bucket
	if q.startTime%q.resolution != 0 {
		q.startTime = int64(q.startTime/q.resolution) * (q.resolution + 1)
	}
	return q
}

// WithEndTime adds an upper optional time boundary filter to the trades being aggregated
func (q *TradeAggregationsQ) WithEndTime(endTime int64) *TradeAggregationsQ {
	// Round upper boundary down, to not deliver partial bucket
	q.endTime = int64(endTime/q.resolution) * q.resolution
	return q
}

// Generate a sql statement to aggregate Trades based on given parameters
func (q *TradeAggregationsQ) GetSql() sq.SelectBuilder {
	var orderPreserved bool
	orderPreserved, q.baseAssetId, q.counterAssetId = getCanonicalAssetOrder(q.baseAssetId, q.counterAssetId)

	var bucketSql sq.SelectBuilder
	if orderPreserved {
		bucketSql = bucketTrades(q.resolution)
	} else {
		bucketSql = reverseBucketTrades(q.resolution)
	}
	bucketSql = bucketSql.From("history_trades")

	//adjust time range and apply time filters
	bucketSql = bucketSql.Where(sq.GtOrEq{"ledger_closed_at": toTimestamp(q.startTime)})
	if q.endTime > 0 {
		bucketSql = bucketSql.Where(sq.Lt{"ledger_closed_at": toTimestamp(q.endTime)})
	}

	return sq.Select(
		"timestamp",
		"count(*) as count",
		"sum(base_amount) as base_volume",
		"sum(counter_amount) as counter_volume",
		"avg(price) as avg",
		"max(price) as high",
		"min(price) as low",
		"first(price) as open",
		"last(price) as close").
		FromSelect(bucketSql, "htrd").
		GroupBy("timestamp").
		Limit(q.pagingParams.Limit).
		OrderBy("timestamp " + q.pagingParams.Order)
}

// formatBucketTimestampSelect formats a sql select clause for a bucketed timestamp, based on given resolution
func formatBucketTimestampSelect(resolution int64) string {
	return fmt.Sprintf("div(cast((extract(epoch from ledger_closed_at) * 1000 ) as bigint), %d)*%d as timestamp",
		resolution, resolution)
}

// bucketTrades generates a select statement to filter rows from the `history_trades` table in
// a compact form, with a timestamp rounded to resolution and reversed base/counter.
func bucketTrades(resolution int64) sq.SelectBuilder {
	return sq.Select(
		formatBucketTimestampSelect(resolution),
		"base_asset_id",
		"base_amount",
		"counter_asset_id",
		"counter_amount",
		"counter_amount::float/base_amount as price",
	)
}

// reverseBucketTrades generates a select statement to filter rows from the `history_trades` table in
// a compact form, with a timestamp rounded to resolution and reversed base/counter.
func reverseBucketTrades(resolution int64) sq.SelectBuilder {
	return sq.Select(
		formatBucketTimestampSelect(resolution),
		"counter_asset_id as base_asset_id",
		"counter_amount as base_amount",
		"base_asset_id as counter_asset_id",
		"base_amount as counter_amount",
		"base_amount::float/counter_amount as price",
	)
}