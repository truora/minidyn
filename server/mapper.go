package server

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/truora/minidyn/types"
)

// AttributeValue (JSON) -> types.Item
func mapAttributeValueToTypes(av *AttributeValue) *types.Item {
	if av == nil {
		return nil
	}

	return &types.Item{
		B:    av.B,
		BOOL: av.BOOL,
		BS:   av.BS,
		L:    mapAttributeValueListToTypes(av.L),
		M:    mapAttributeValueMapToTypes(av.M),
		N:    av.N,
		NS:   av.NS,
		NULL: av.NULL,
		S:    av.S,
		SS:   av.SS,
	}
}

func mapAttributeValueListToTypes(list []*AttributeValue) []*types.Item {
	if list == nil {
		return nil
	}

	out := make([]*types.Item, len(list))
	for i, v := range list {
		out[i] = mapAttributeValueToTypes(v)
	}

	return out
}

func mapAttributeValueMapToTypes(m map[string]*AttributeValue) map[string]*types.Item {
	if m == nil {
		return nil
	}

	out := make(map[string]*types.Item, len(m))
	for k, v := range m {
		out[k] = mapAttributeValueToTypes(v)
	}

	return out
}

// types.Item -> AttributeValue (JSON)
func mapTypesToAttributeValue(it *types.Item) *AttributeValue {
	if it == nil {
		return nil
	}

	return &AttributeValue{
		B:    it.B,
		BOOL: it.BOOL,
		BS:   it.BS,
		L:    mapTypesListToAttributeValue(it.L),
		M:    mapTypesMapToAttributeValue(it.M),
		N:    it.N,
		NS:   it.NS,
		NULL: it.NULL,
		S:    it.S,
		SS:   it.SS,
	}
}

func mapTypesListToAttributeValue(list []*types.Item) []*AttributeValue {
	if list == nil {
		return nil
	}

	out := make([]*AttributeValue, len(list))
	for i, v := range list {
		out[i] = mapTypesToAttributeValue(v)
	}

	return out
}

func mapTypesMapToAttributeValue(m map[string]*types.Item) map[string]*AttributeValue {
	if m == nil {
		return nil
	}

	out := make(map[string]*AttributeValue, len(m))
	for k, v := range m {
		out[k] = mapTypesToAttributeValue(v)
	}

	return out
}

// map types.Item to dynamodb AttributeValue interfaces (for smithy errors).
//
//nolint:gocyclo // mapping all shapes in a single switch for readability
func mapTypesToDDBAttributeValue(it *types.Item) ddbtypes.AttributeValue {
	if it == nil {
		return nil
	}

	switch {
	case len(it.B) != 0:
		return &ddbtypes.AttributeValueMemberB{Value: it.B}
	case it.BOOL != nil:
		return &ddbtypes.AttributeValueMemberBOOL{Value: *it.BOOL}
	case len(it.BS) != 0:
		return &ddbtypes.AttributeValueMemberBS{Value: it.BS}
	case it.N != nil:
		return &ddbtypes.AttributeValueMemberN{Value: aws.ToString(it.N)}
	case len(it.NS) != 0:
		return &ddbtypes.AttributeValueMemberNS{Value: fromStringPtrs(it.NS)}
	case it.S != nil:
		return &ddbtypes.AttributeValueMemberS{Value: aws.ToString(it.S)}
	case len(it.SS) != 0:
		return &ddbtypes.AttributeValueMemberSS{Value: fromStringPtrs(it.SS)}
	case len(it.L) != 0:
		lv := make([]ddbtypes.AttributeValue, len(it.L))
		for i, v := range it.L {
			lv[i] = mapTypesToDDBAttributeValue(v)
		}

		return &ddbtypes.AttributeValueMemberL{Value: lv}
	case len(it.M) != 0:
		mv := make(map[string]ddbtypes.AttributeValue, len(it.M))
		for k, v := range it.M {
			mv[k] = mapTypesToDDBAttributeValue(v)
		}

		return &ddbtypes.AttributeValueMemberM{Value: mv}
	default:
		return &ddbtypes.AttributeValueMemberNULL{Value: true}
	}
}

func mapTypesMapToDDBAttributeValue(m map[string]*types.Item) map[string]ddbtypes.AttributeValue {
	if len(m) == 0 {
		return nil
	}

	out := make(map[string]ddbtypes.AttributeValue, len(m))
	for k, v := range m {
		out[k] = mapTypesToDDBAttributeValue(v)
	}

	return out
}

// Table / schema helpers (ddbtypes -> types)

func mapAttributeDefinitions(input []ddbtypes.AttributeDefinition) []*types.AttributeDefinition {
	if len(input) == 0 {
		return nil
	}

	out := make([]*types.AttributeDefinition, len(input))
	for i, a := range input {
		out[i] = &types.AttributeDefinition{
			AttributeName: a.AttributeName,
			AttributeType: aws.String(string(a.AttributeType)),
		}
	}

	return out
}

func mapKeySchema(input []ddbtypes.KeySchemaElement) []*types.KeySchemaElement {
	if len(input) == 0 {
		return nil
	}

	out := make([]*types.KeySchemaElement, len(input))
	for i, ks := range input {
		out[i] = &types.KeySchemaElement{
			AttributeName: aws.ToString(ks.AttributeName),
			KeyType:       string(ks.KeyType),
		}
	}

	return out
}

func mapProjection(input *ddbtypes.Projection) *types.Projection {
	if input == nil {
		return nil
	}

	return &types.Projection{
		NonKeyAttributes: toStringPtrs(input.NonKeyAttributes),
		ProjectionType:   toStringPtr(string(input.ProjectionType)),
	}
}

func mapProvisionedThroughput(input *ddbtypes.ProvisionedThroughput) *types.ProvisionedThroughput {
	if input == nil {
		return nil
	}

	return &types.ProvisionedThroughput{
		ReadCapacityUnits:  aws.ToInt64(input.ReadCapacityUnits),
		WriteCapacityUnits: aws.ToInt64(input.WriteCapacityUnits),
	}
}

func mapGSI(input []ddbtypes.GlobalSecondaryIndex) []*types.GlobalSecondaryIndex {
	if len(input) == 0 {
		return nil
	}

	out := make([]*types.GlobalSecondaryIndex, len(input))
	for i, g := range input {
		out[i] = &types.GlobalSecondaryIndex{
			IndexName:             g.IndexName,
			KeySchema:             mapKeySchema(g.KeySchema),
			Projection:            mapProjection(g.Projection),
			ProvisionedThroughput: mapProvisionedThroughput(g.ProvisionedThroughput),
		}
	}

	return out
}

func mapLSI(input []ddbtypes.LocalSecondaryIndex) []*types.LocalSecondaryIndex {
	if len(input) == 0 {
		return nil
	}

	out := make([]*types.LocalSecondaryIndex, len(input))
	for i, l := range input {
		out[i] = &types.LocalSecondaryIndex{
			IndexName:  l.IndexName,
			KeySchema:  mapKeySchema(l.KeySchema),
			Projection: mapProjection(l.Projection),
		}
	}

	return out
}

func mapGSIUpdate(input []ddbtypes.GlobalSecondaryIndexUpdate) []*types.GlobalSecondaryIndexUpdate {
	if len(input) == 0 {
		return nil
	}

	out := make([]*types.GlobalSecondaryIndexUpdate, len(input))
	for i, u := range input {
		out[i] = &types.GlobalSecondaryIndexUpdate{
			Create: mapCreateGSI(u.Create),
			Delete: mapDeleteGSI(u.Delete),
			Update: mapUpdateGSI(u.Update),
		}
	}

	return out
}

func mapCreateGSI(input *ddbtypes.CreateGlobalSecondaryIndexAction) *types.CreateGlobalSecondaryIndexAction {
	if input == nil {
		return nil
	}

	return &types.CreateGlobalSecondaryIndexAction{
		IndexName:             input.IndexName,
		KeySchema:             mapKeySchema(input.KeySchema),
		Projection:            mapProjection(input.Projection),
		ProvisionedThroughput: mapProvisionedThroughput(input.ProvisionedThroughput),
	}
}

func mapDeleteGSI(input *ddbtypes.DeleteGlobalSecondaryIndexAction) *types.DeleteGlobalSecondaryIndexAction {
	if input == nil {
		return nil
	}

	return &types.DeleteGlobalSecondaryIndexAction{
		IndexName: input.IndexName,
	}
}

func mapUpdateGSI(input *ddbtypes.UpdateGlobalSecondaryIndexAction) *types.UpdateGlobalSecondaryIndexAction {
	if input == nil {
		return nil
	}

	return &types.UpdateGlobalSecondaryIndexAction{
		IndexName:             input.IndexName,
		ProvisionedThroughput: mapProvisionedThroughput(input.ProvisionedThroughput),
	}
}

func toStringPtr(s string) *string {
	if s == "" {
		return nil
	}

	return aws.String(s)
}

func toStringPtrs(in []string) []*string {
	if len(in) == 0 {
		return nil
	}

	out := make([]*string, len(in))
	for i, v := range in {
		out[i] = toStringPtr(v)
	}

	return out
}

// Map types.TableDescription to ddbtypes.TableDescription for responses.
func mapTableDescriptionToDDB(td *types.TableDescription) *ddbtypes.TableDescription {
	if td == nil {
		return nil
	}

	return &ddbtypes.TableDescription{
		TableName:              aws.String(td.TableName),
		ItemCount:              aws.Int64(td.ItemCount),
		KeySchema:              mapTypesKeySchema(td.KeySchema),
		GlobalSecondaryIndexes: mapTypesGSI(td.GlobalSecondaryIndexes),
		LocalSecondaryIndexes:  mapTypesLSI(td.LocalSecondaryIndexes),
	}
}

func mapTypesKeySchema(in []types.KeySchemaElement) []ddbtypes.KeySchemaElement {
	if len(in) == 0 {
		return nil
	}

	out := make([]ddbtypes.KeySchemaElement, len(in))
	for i, ks := range in {
		ksCopy := ks
		out[i] = ddbtypes.KeySchemaElement{
			AttributeName: &ksCopy.AttributeName,
			KeyType:       ddbtypes.KeyType(ksCopy.KeyType),
		}
	}

	return out
}

func mapTypesGSI(in []types.GlobalSecondaryIndexDescription) []ddbtypes.GlobalSecondaryIndexDescription {
	if len(in) == 0 {
		return nil
	}

	out := make([]ddbtypes.GlobalSecondaryIndexDescription, len(in))

	for i, g := range in {
		gCopy := g
		out[i] = ddbtypes.GlobalSecondaryIndexDescription{
			IndexName: gCopy.IndexName,
			ItemCount: aws.Int64(gCopy.ItemCount),
			KeySchema: mapTypesKeySchema(gCopy.KeySchema),
			Projection: &ddbtypes.Projection{
				NonKeyAttributes: fromStringPtrs(gCopy.Projection.NonKeyAttributes),
				ProjectionType:   ddbtypes.ProjectionType(aws.ToString(gCopy.Projection.ProjectionType)),
			},
		}
	}

	return out
}

func mapTypesLSI(in []types.LocalSecondaryIndexDescription) []ddbtypes.LocalSecondaryIndexDescription {
	if len(in) == 0 {
		return nil
	}

	out := make([]ddbtypes.LocalSecondaryIndexDescription, len(in))

	for i, l := range in {
		lCopy := l
		out[i] = ddbtypes.LocalSecondaryIndexDescription{
			IndexName: lCopy.IndexName,
			ItemCount: aws.Int64(lCopy.ItemCount),
			KeySchema: mapTypesKeySchema(lCopy.KeySchema),
			Projection: &ddbtypes.Projection{
				NonKeyAttributes: fromStringPtrs(lCopy.Projection.NonKeyAttributes),
				ProjectionType:   ddbtypes.ProjectionType(aws.ToString(lCopy.Projection.ProjectionType)),
			},
		}
	}

	return out
}

func fromStringPtrs(in []*string) []string {
	if len(in) == 0 {
		return nil
	}

	out := make([]string, len(in))
	for i, v := range in {
		out[i] = aws.ToString(v)
	}

	return out
}
