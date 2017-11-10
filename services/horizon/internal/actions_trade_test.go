package horizon

import (
	"net/url"
	"testing"
	"time"

	"github.com/stellar/go/services/horizon/internal/db2/history"
	"github.com/stellar/go/services/horizon/internal/resource"
	. "github.com/stellar/go/services/horizon/internal/db2/history"
	. "github.com/stellar/go/services/horizon/internal/test/trades"
	"strconv"
	"github.com/stellar/go/xdr"
)

func TestTradeActions_Index(t *testing.T) {
	ht := StartHTTPTest(t, "trades")
	defer ht.Finish()

	// All trades
	w := ht.Get("/trades")
	if ht.Assert.Equal(200, w.Code) {
		ht.Assert.PageOf(1, w.Body)
	}

	// 	ensure created_at is populated correctly
	records := []resource.Trade{}
	ht.UnmarshalPage(w.Body, &records)

	l := history.Ledger{}
	hq := history.Q{Session: ht.HorizonSession()}
	ht.Require.NoError(hq.LedgerBySequence(&l, 6))

	ht.Assert.WithinDuration(l.ClosedAt, records[0].LedgerCloseTime, 1*time.Second)

	//
	var q = make(url.Values)
	q.Add("base_asset_type", "credit_alphanum4")
	q.Add("base_asset_code", "USD")
	q.Add("base_asset_issuer", "GC23QF2HUE52AMXUFUH3AYJAXXGXXV2VHXYYR6EYXETPKDXZSAW67XO4")
	q.Add("counter_asset_type", "credit_alphanum4")
	q.Add("counter_asset_code", "EUR")
	q.Add("counter_asset_issuer", "GCQPYGH4K57XBDENKKX55KDTWOTK5WDWRQOH2LHEDX3EKVIQRLMESGBG")

	w = ht.GetWithParams("/trades", q)
	if ht.Assert.Equal(200, w.Code) {
		ht.Assert.PageOf(1, w.Body)

		records := []map[string]interface{}{}
		ht.UnmarshalPage(w.Body, &records)

		ht.Assert.Contains(records[0], "base_amount")
		ht.Assert.Contains(records[0], "counter_amount")
	}

	q = make(url.Values)
	q.Add("base_asset_type", "credit_alphanum4")
	q.Add("base_asset_code", "EUR")
	q.Add("base_asset_issuer", "GCQPYGH4K57XBDENKKX55KDTWOTK5WDWRQOH2LHEDX3EKVIQRLMESGBG")
	q.Add("counter_asset_type", "credit_alphanum4")
	q.Add("counter_asset_code", "USD")
	q.Add("counter_asset_issuer", "GC23QF2HUE52AMXUFUH3AYJAXXGXXV2VHXYYR6EYXETPKDXZSAW67XO4")

	w = ht.GetWithParams("/trades", q)
	if ht.Assert.Equal(200, w.Code) {
		ht.Assert.PageOf(1, w.Body)

		records := []map[string]interface{}{}
		ht.UnmarshalPage(w.Body, &records)

		ht.Assert.Contains(records[0], "base_amount")
		ht.Assert.Contains(records[0], "counter_amount")
	}
}

// setAssetQuery adds an asset filter with a given prefix to a query
func setAssetQuery(q *url.Values, prefix string, asset xdr.Asset){
	var assetType, assetCode, assetFilter string
	asset.Extract(&assetType, &assetCode, &assetFilter)
	q.Add(prefix+"asset_type", assetType)
	q.Add(prefix+"asset_code", assetCode)
	q.Add(prefix+"asset_issuer", assetFilter)
}

func TestTradeActions_Aggregation(t *testing.T) {
	ht := StartHTTPTest(t, "base")
	defer ht.Finish()

	const numOfTrades = 10
	const start = 0
	const second = 1000
	const minute = 60 * second
	const hour = minute * 60

	dbQ := &Q{ht.HorizonSession()}
	err, ass1, ass2 := PopulateTestTrades(dbQ, start, numOfTrades, minute)

	ht.Require.NoError(err)

	var records []resource.TradeAggregation
	var nextLink string

	q := make(url.Values)
	setAssetQuery(&q, "base_", ass1)
	setAssetQuery(&q, "counter_", ass2)
	q.Add("resolution", strconv.FormatInt(minute, 10))
	q.Add("start_time", strconv.FormatInt(start, 10))
	q.Add("end_time", strconv.FormatInt(start+hour, 10))
	w := ht.GetWithParams("/trades/aggregate", q)
	if ht.Assert.Equal(200, w.Code) {
		ht.Assert.PageOf(numOfTrades, w.Body)
	}

	//test partial range by modifying endTime to be one minute above half range.
	//half of the results are expected
	endTime := start+(numOfTrades/2)*minute
	q.Set("end_time", strconv.Itoa(endTime))
	w = ht.GetWithParams("/trades/aggregate", q)
	if ht.Assert.Equal(200, w.Code) {
		ht.Assert.PageOf(numOfTrades/2, w.Body)
	}

	//test limit
	limit := 3
	q.Add("limit", strconv.Itoa(limit))
	w = ht.GetWithParams("/trades/aggregate", q)
	if ht.Assert.Equal(200, w.Code) {
		ht.Assert.PageOf(limit, w.Body)
	}

	//test next link
	w = ht.GetWithParams("/trades/aggregate", q)
	nextLink = ht.UnmarshalNext(w.Body)
	w = ht.Get(nextLink)
	if ht.Assert.Equal(200, w.Code) {
		ht.Assert.PageOf(numOfTrades/2-limit, w.Body)
		ht.UnmarshalPage(w.Body, &records)
		ht.Assert.Equal(int64(limit*minute), records[0].Timestamp)
	}

	//test direction (desc)
	q.Add("order", "desc")
	w = ht.GetWithParams("/trades/aggregate", q)
	if ht.Assert.Equal(200, w.Code) {
		if ht.Assert.PageOf(limit, w.Body) {
			ht.UnmarshalPage(w.Body, &records)
			ht.Assert.Equal(int64(start+(numOfTrades/2-1)*minute), records[0].Timestamp)
		}
	}

	//test next link desc
	w = ht.GetWithParams("/trades/aggregate", q)
	nextLink = ht.UnmarshalNext(w.Body)
	w = ht.Get(nextLink)
	if ht.Assert.Equal(200, w.Code) {
		ht.Assert.PageOf(numOfTrades/2-limit, w.Body)
	}

	//test next next link empty
	w = ht.GetWithParams("/trades/aggregate", q)
	nextLink = ht.UnmarshalNext(w.Body)
	w = ht.Get(nextLink)
	nextLink = ht.UnmarshalNext(w.Body)
	w = ht.Get(nextLink)
	if ht.Assert.Equal(200, w.Code) {
		ht.Assert.PageOf(0, w.Body)
	}
}

func TestTradeActions_IndexRegressions(t *testing.T) {
	ht := StartHTTPTest(t, "trades")
	defer ht.Finish()

	// Regression:  https://github.com/stellar/go/services/horizon/internal/issues/318
	var q = make(url.Values)
	q.Add("base_asset_type", "credit_alphanum4")
	q.Add("base_asset_code", "EUR")
	q.Add("base_asset_issuer", "GCQPYGH4K57XBDENKKX55KDTWOTK5WDWRQOH2LHEDX3EKVIQRLMESGBG")
	q.Add("counter_asset_type", "native")

	w := ht.Get("/trades?" + q.Encode())

	ht.Assert.Equal(404, w.Code) //This used to be 200 with length 0
}
