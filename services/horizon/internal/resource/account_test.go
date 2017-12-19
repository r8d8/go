package resource

import (
	"testing"
	"github.com/stellar/go/services/horizon/internal/db2/core"
	"github.com/stellar/go/xdr"
	"github.com/tj/assert"
)

func TestSplitTrustlines(t *testing.T) {
	tr := make([]core.Trustline, 0, 2)
	tr = append(tr, core.Trustline{"testID",
		xdr.AssetTypeAssetTypeCreditAlphanum12,
		"",
		"TEST_ASSET_1",
		100,
		10,
		1,
		})
	tr = append(tr, core.Trustline{"testID",
		xdr.AssetTypeAssetTypeCreditAlphanum12,
		"",
		"TEST_ASSET_2",
		100,
		10,
		2,
	})

	auth, unauth := splitTrustlines(tr)
	assert.Equal(t, len(auth), 1)
	assert.Equal(t, len(unauth), 1)
	assert.Equal(t, auth[0].Assetcode, "TEST_ASSET_1")
	assert.Equal(t, unauth[0].Assetcode, "TEST_ASSET_2")
}