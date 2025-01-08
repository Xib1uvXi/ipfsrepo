package fsrepo

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestAddDatastoreConfigHandler(t *testing.T) {
	err := AddDatastoreConfigHandler("levelds-test", LevelDBDatastoreConfigParser())
	require.NoError(t, err)
}

func TestAnyDatastoreConfig(t *testing.T) {
	_, err := AnyDatastoreConfig(DefaultDiskSpec())
	require.NoError(t, err)
}
