package client

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMapProjectionToTypesNil(t *testing.T) {
	c := require.New(t)

	c.Nil(mapProjectionToTypes(nil))
}

func TestMapDeleteItemInputToTypesNil(t *testing.T) {
	c := require.New(t)

	c.Nil(mapDeleteItemInputToTypes(nil))
}

func TestMapPutItemInputToTypesNil(t *testing.T) {
	c := require.New(t)

	c.Nil(mapPutItemInputToTypes(nil))
}
