package client

import (
	"context"
	"testing"

	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

func TestClearTable(t *testing.T) {
	c := require.New(t)

	srv := ConnectTestServer()
	defer srv.DisconnectTestServer()

	client := newClient(srv.URL)

	err := ensurePokemonTable(client)
	c.NoError(err)

	err = ensurePokemonTypeIndex(client)
	c.NoError(err)

	err = createPokemon(client, pokemon{
		ID:   "001",
		Type: "grass",
		Name: "Bulbasaur",
	})
	c.NoError(err)

	item, err := getPokemon(client, "001")
	c.NoError(err)
	c.Equal("Bulbasaur", item["name"].(*dynamodbtypes.AttributeValueMemberS).Value)

	items, err := getPokemonsByType(client, "grass")
	c.NoError(err)
	c.Len(items, 1)

	err = ClearTable(client, tableName)
	c.NoError(err)

	item, err = getPokemon(client, "001")
	c.NoError(err)
	c.Empty(item)

	items, err = getPokemonsByType(client, "grass")
	c.NoError(err)
	c.Empty(items)
}

func TestEmulateFailure(t *testing.T) {
	c := require.New(t)

	srv := ConnectTestServer()
	defer srv.DisconnectTestServer()

	srv.EmulateFailure(FailureConditionInternalServerError)

	c.Equal(srv.forceFailureErr, &emulatedInternalServeError)
}

func TestAddIndex(t *testing.T) {
	c := require.New(t)

	srv := ConnectTestServer()
	defer srv.DisconnectTestServer()

	client := newClient(srv.URL)
	err := AddIndex(context.Background(), client, tableName, "bad-index", "hk", "rk")
	c.Equal("ResourceNotFoundException: Cannot do operations on a non-existent table", err.Error())

	err = ensurePokemonTable(client)
	c.NoError(err)

	err = AddIndex(context.Background(), client, tableName, "index", "hk", "rk")
	c.NoError(err)

	// c.Len(client.tables[tableName].Indexes, 1)
}

func BenchmarkClearTable(b *testing.B) {
	c := require.New(b)

	srv := ConnectTestServer()
	defer srv.DisconnectTestServer()

	client := newClient(srv.URL)
	err := ensurePokemonTable(client)
	c.NoError(err)

	err = createPokemon(client, pokemon{
		ID:   "001",
		Type: "grass",
		Name: "Bulbasaur",
	})
	c.NoError(err)

	item, err := getData(client, tableName, "a", "x")
	c.NoError(err)
	c.Equal("a", item["partition"].(*dynamodbtypes.AttributeValueMemberS).Value)

	items, err := getDataInIndex(client, "custom-index", tableName, "a", "")
	c.NoError(err)
	c.Len(items, 1)

	for i := 0; i < b.N; i++ {
		err = ClearTable(client, tableName)
		c.NoError(err)
	}
}
