package history_test

import (
	"testing"

	"github.com/stellar/go/services/horizon/internal/db2"
	. "github.com/stellar/go/services/horizon/internal/db2/history"
	. "github.com/stellar/go/services/horizon/internal/test/trades"
	"github.com/stellar/go/services/horizon/internal/test"
	"github.com/stellar/go/xdr"
)

func TestTradeQueries(t *testing.T) {
	tt := test.Start(t).Scenario("kahuna")
	defer tt.Finish()
	q := &Q{tt.HorizonSession()}
	var trades []Trade

	// All trades
	err := q.Trades().Select(&trades)
	if tt.Assert.NoError(err) {
		tt.Assert.Len(trades, 4)
	}

	// Paging
	pq := db2.MustPageQuery(trades[0].PagingToken(), "asc", 1)
	var pt []Trade

	err = q.Trades().Page(pq).Select(&pt)
	if tt.Assert.NoError(err) {
		tt.Assert.Len(pt, 1)
		tt.Assert.Equal(trades[1], pt[0])
	}

	// Cursor bounds checking
	pq = db2.MustPageQuery("", "desc", 1)
	err = q.Trades().Page(pq).Select(&pt)
	tt.Assert.NoError(err)

	// test for asset pairs
	q.TradesForAssetPair(2, 3).Select(&trades)
	tt.Assert.Len(trades, 0)

	q.TradesForAssetPair(1, 2).Select(&trades)
	tt.Assert.Len(trades, 1)

	tt.Assert.Equal(xdr.Int64(2000000000), trades[0].BaseAmount)
	tt.Assert.Equal(xdr.Int64(1000000000), trades[0].CounterAmount)
	tt.Assert.Equal(true, trades[0].BaseIsSeller)
}

func TestTradeAggQueries(t *testing.T) {
	tt := test.Start(t).Scenario("base")
	defer tt.Finish()

	const numOfTrades = 10
	const start = 0
	const second = 1000
	const minute = 60 * second
	const hour = minute * 60

	q := &Q{tt.HorizonSession()}
	err, ass1, ass2 := PopulateTestTrades(q, start, numOfTrades, minute)

	if !tt.Assert.NoError(err) {
		return
	}

	var aggs []TradeAggregation

	//test one bucket for all
	expected := TradeAggregation{start, 10, 5500, 38500, 5.5, 10, 1, 1, 10}
	_, tradesQ := q.BucketTradesForAssetPair(ass1, ass2, hour)
	tradesQ.FromStartTime(start).
		FromEndTime(start + minute*(numOfTrades+1)).
		SelectAggregateByBucket(&aggs, 10, "asc")

	if tt.Assert.NoError(err) {
		if tt.Assert.Len(aggs, 1) {
			tt.Assert.Equal(expected, aggs[0])
		}
	}

	//test one bucket for all - reverse
	expected = TradeAggregation{start, 10, 38500, 5500, 0.2928968253968254, 1, 0.1, 1, 0.1}
	err, tradesQ = q.BucketTradesForAssetPair(ass2, ass1, hour)
	err = tradesQ.FromStartTime(start).
		FromEndTime(start + minute*(numOfTrades+1)).
		SelectAggregateByBucket(&aggs, 10, "asc")

	if tt.Assert.NoError(err) {
		if tt.Assert.Len(aggs, 1) {
			tt.Assert.Equal(expected, aggs[0])
		}
	}

	//Test one bucket each, sample test one aggregation
	expected = TradeAggregation{240000, 1, 500, 2500, 5, 5, 5, 5, 5}
	err, tradesQ = q.BucketTradesForAssetPair(ass1, ass2, minute)
	err = tradesQ.FromStartTime(start).
		FromEndTime(start + minute*(numOfTrades+1)).
		SelectAggregateByBucket(&aggs, 10, "asc")
	if tt.Assert.NoError(err) {
		if tt.Assert.Len(aggs, 10) {
			tt.Assert.Equal(aggs[4], expected)
		}
	}

	//Test two bucket each, sample test one aggregation
	expected = TradeAggregation{240000, 2, 1100, 6100, 5.5, 6, 5, 5, 6}
	err, tradesQ = q.BucketTradesForAssetPair(ass1, ass2, 2*minute)
	err = tradesQ.FromStartTime(start).
		FromEndTime(start + minute*(numOfTrades+1)).
		SelectAggregateByBucket(&aggs, 10, "asc")
	if tt.Assert.NoError(err) {
		if tt.Assert.Len(aggs, 5) {
			tt.Assert.Equal(aggs[2], expected)
		}
	}
}
