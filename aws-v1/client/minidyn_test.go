package client

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
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
	c.Equal("Bulbasaur", aws.StringValue(item["name"].S))

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
		config := &aws.Config{}
		config.Endpoint = aws.String("http://localhost:8000")

		client := dynamodb.New(session.Must(session.NewSession(config)))
		EmulateFailure(client, FailureConditionInternalServerError)
	})

	client := NewClient()
	EmulateFailure(client, FailureConditionInternalServerError)
}

func TestAddIndex(t *testing.T) {
	c := require.New(t)

	client := NewClient()

	err := AddIndex(client, tableName, "bad-index", "hk", "rk")
	c.Equal("ResourceNotFoundException: Cannot do operations on a non-existent table", err.Error())

	err = ensurePokemonTable(client)
	c.NoError(err)

	err = AddIndex(client, tableName, "index", "hk", "rk")
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
	c.Equal("a", aws.StringValue(item["partition"].S))

	items, err := getDataInIndex(client, "custom-index", tableName, "a", "")
	c.NoError(err)
	c.Len(items, 1)

	for i := 0; i < b.N; i++ {
		err = ClearTable(client, tableName)
		c.NoError(err)
	}
}
