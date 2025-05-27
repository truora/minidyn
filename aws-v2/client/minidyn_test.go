package client

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

func TestClearTable(t *testing.T) {
	c := require.New(t)
	client := NewClient()

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
	c.Equal(&dynamodbtypes.AttributeValueMemberS{Value: "Bulbasaur"}, item["name"])

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

	c.Panics(func() {
		cfg, err := config.LoadDefaultConfig(context.Background())
		c.NoError(err)

		client := dynamodb.NewFromConfig(cfg)
		EmulateFailure(client, FailureConditionInternalServerError)
	})

	client := NewClient()
	EmulateFailure(client, FailureConditionInternalServerError)
}

func TestAddIndex(t *testing.T) {
	c := require.New(t)

	client := NewClient()

	err := AddIndex(context.Background(), client, tableName, "bad-index", "hk", "rk")
	c.Equal("ResourceNotFoundException: Cannot do operations on a non-existent table", err.Error())

	err = ensurePokemonTable(client)
	c.NoError(err)

	err = AddIndex(context.Background(), client, tableName, "index", "hk", "rk")
	c.NoError(err)

	c.Len(client.tables[tableName].Indexes, 1)
}

func BenchmarkClearTable(b *testing.B) {
	c := require.New(b)
	client := NewClient()

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
	c.Equal(&dynamodbtypes.AttributeValueMemberS{Value: "a"}, item["partition"])

	items, err := getDataInIndex(client, "custom-index", tableName, "a", "")
	c.NoError(err)
	c.Len(items, 1)

	for i := 0; i < b.N; i++ {
		err = ClearTable(client, tableName)
		c.NoError(err)
	}
}
