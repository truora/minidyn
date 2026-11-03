package main

import (
	"fmt"
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const (
	serverImportPath = "github.com/truora/minidyn/server"
	sdkTypesPath     = "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// Finding kinds.
const (
	kindOperation = "operation" // SDK operation the server does not dispatch
	kindRequest   = "request"   // request field the server decodes but never reads
	kindResponse  = "response"  // SDK response field the server never sets
)

// finding is one gap in minidyn's API surface. Its key (kind + name) is what the
// allowlist matches.
type finding struct {
	Kind   string
	Name   string
	Detail string
}

func (f finding) key() string { return f.Kind + " " + f.Name }

// sdkOutputs are the SDK output shapes of the operations the server implements. The server
// mirrors each one with a struct of the same name in server/responses.go.
var sdkOutputs = []reflect.Type{
	reflect.TypeFor[dynamodb.CreateTableOutput](),
	reflect.TypeFor[dynamodb.DeleteTableOutput](),
	reflect.TypeFor[dynamodb.UpdateTableOutput](),
	reflect.TypeFor[dynamodb.DescribeTableOutput](),
	reflect.TypeFor[dynamodb.PutItemOutput](),
	reflect.TypeFor[dynamodb.DeleteItemOutput](),
	reflect.TypeFor[dynamodb.UpdateItemOutput](),
	reflect.TypeFor[dynamodb.GetItemOutput](),
	reflect.TypeFor[dynamodb.QueryOutput](),
	reflect.TypeFor[dynamodb.ScanOutput](),
	reflect.TypeFor[dynamodb.BatchWriteItemOutput](),
	reflect.TypeFor[dynamodb.BatchGetItemOutput](),
	reflect.TypeFor[dynamodb.TransactWriteItemsOutput](),
	reflect.TypeFor[dynamodb.TransactGetItemsOutput](),
}

// sdkNested are SDK types the server builds directly (not mirrored), whose fields it must set.
var sdkNested = []reflect.Type{
	reflect.TypeFor[ddbtypes.TableDescription](),
	reflect.TypeFor[ddbtypes.GlobalSecondaryIndexDescription](),
	reflect.TypeFor[ddbtypes.LocalSecondaryIndexDescription](),
	reflect.TypeFor[ddbtypes.CancellationReason](),
}

// fieldUses maps "<pkg path>.<type name>" to the field names the code reads or sets
// (selector expressions and keyed composite literals).
type fieldUses map[string]map[string]bool

func (u fieldUses) add(named *types.Named, field string) {
	obj := named.Obj()
	if obj.Pkg() == nil {
		return
	}

	key := obj.Pkg().Path() + "." + obj.Name()
	if u[key] == nil {
		u[key] = map[string]bool{}
	}

	u[key][field] = true
}

func (u fieldUses) has(pkgPath, typeName, field string) bool {
	return u[pkgPath+"."+typeName][field]
}

// serverPackage is the type-checked server package (test files excluded).
type serverPackage struct {
	files map[string]*ast.File // by base name
	pkg   *types.Package
	// reads are fields the code reads; uses also include assignments and struct literals.
	// Request fields must be read to be honored; response fields must be used to be set.
	reads fieldUses
	uses  fieldUses
}

func loadServer(root string) (*serverPackage, error) {
	dir := filepath.Join(root, "server")
	fset := token.NewFileSet()

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	sp := &serverPackage{files: map[string]*ast.File{}, reads: fieldUses{}, uses: fieldUses{}}

	var files []*ast.File

	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}

		f, perr := parser.ParseFile(fset, filepath.Join(dir, name), nil, parser.SkipObjectResolution)
		if perr != nil {
			return nil, perr
		}

		sp.files[name] = f
		files = append(files, f)
	}

	info := &types.Info{
		Types:      map[ast.Expr]types.TypeAndValue{},
		Selections: map[*ast.SelectorExpr]*types.Selection{},
	}

	conf := types.Config{Importer: importer.ForCompiler(fset, "source", nil)}

	sp.pkg, err = conf.Check(serverImportPath, fset, files, info)
	if err != nil {
		return nil, fmt.Errorf("type-check server: %w", err)
	}

	for _, f := range files {
		collectFieldUses(f, info, sp.reads, sp.uses)
	}

	for _, required := range []string{"server.go", "requests.go"} {
		if sp.files[required] == nil {
			return nil, fmt.Errorf("server/%s not found: the analysis expects the dispatch switch and "+
				"the generated request types there", required)
		}
	}

	return sp, nil
}

func namedOf(t types.Type) (*types.Named, bool) {
	for {
		p, ok := t.(*types.Pointer)
		if !ok {
			break
		}

		t = p.Elem()
	}

	n, ok := t.(*types.Named)

	return n, ok
}

// collectFieldUses records field reads in reads, and every field use (reads, assignment
// targets and keyed struct literals) in uses.
func collectFieldUses(f *ast.File, info *types.Info, reads, uses fieldUses) {
	written := assignmentTargets(f)

	ast.Inspect(f, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.SelectorExpr:
			addSelectorUse(x, info, uses)

			if !written[x] {
				addSelectorUse(x, info, reads)
			}
		case *ast.CompositeLit:
			addCompositeLitUses(x, info, uses)
		}

		return true
	})
}

// assignmentTargets returns the selectors that are written by "x.F = ..." or "x.F++" (a
// "x.F += 1" also reads x.F, so it is not included).
func assignmentTargets(f *ast.File) map[*ast.SelectorExpr]bool {
	out := map[*ast.SelectorExpr]bool{}

	ast.Inspect(f, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.AssignStmt:
			if x.Tok != token.ASSIGN && x.Tok != token.DEFINE {
				return true
			}

			for _, lhs := range x.Lhs {
				if sel, ok := lhs.(*ast.SelectorExpr); ok {
					out[sel] = true
				}
			}
		case *ast.IncDecStmt:
			if sel, ok := x.X.(*ast.SelectorExpr); ok {
				out[sel] = true
			}
		}

		return true
	})

	return out
}

// addSelectorUse records the field selected by x.Field.
func addSelectorUse(x *ast.SelectorExpr, info *types.Info, uses fieldUses) {
	sel, ok := info.Selections[x]
	if !ok || sel.Kind() != types.FieldVal {
		return
	}

	if named, ok := namedOf(sel.Recv()); ok {
		uses.add(named, sel.Obj().Name())
	}
}

// addCompositeLitUses records the keys of a keyed struct literal T{Field: ...}.
func addCompositeLitUses(x *ast.CompositeLit, info *types.Info, uses fieldUses) {
	named, ok := namedOf(info.TypeOf(x))
	if !ok {
		return
	}

	if _, isStruct := named.Underlying().(*types.Struct); !isStruct {
		return
	}

	for _, elt := range x.Elts {
		kv, ok := elt.(*ast.KeyValueExpr)
		if !ok {
			continue
		}

		if id, ok := kv.Key.(*ast.Ident); ok {
			uses.add(named, id.Name)
		}
	}
}

// analyze returns every gap, sorted by kind and name.
func analyze(root string) ([]finding, error) {
	sp, err := loadServer(root)
	if err != nil {
		return nil, err
	}

	var out []finding

	out = append(out, missingOperations(sp)...)
	out = append(out, unreadRequestFields(sp)...)
	out = append(out, unsetResponseFields(sp)...)

	sort.Slice(out, func(i, j int) bool { return out[i].key() < out[j].key() })

	return out, nil
}

// missingOperations lists SDK client operations with no case in the server's dispatch switch.
func missingOperations(sp *serverPackage) []finding {
	dispatched := map[string]bool{}

	ast.Inspect(sp.files["server.go"], func(n ast.Node) bool {
		cc, ok := n.(*ast.CaseClause)
		if !ok {
			return true
		}

		for _, e := range cc.List {
			if lit, ok := e.(*ast.BasicLit); ok && lit.Kind == token.STRING {
				if s, err := strconv.Unquote(lit.Value); err == nil {
					dispatched[s] = true
				}
			}
		}

		return true
	})

	var out []finding

	client := reflect.TypeFor[*dynamodb.Client]()

	for m := range client.Methods() {
		if !isOperation(m) || dispatched[m.Name] {
			continue
		}

		out = append(out, finding{Kind: kindOperation, Name: m.Name, Detail: "not dispatched by server/server.go"})
	}

	return out
}

// isOperation reports methods shaped like func(ctx, *<Name>Input, ...func(*Options)).
func isOperation(m reflect.Method) bool {
	t := m.Type // includes the receiver
	if t.NumIn() != 4 || !t.IsVariadic() {
		return false
	}

	in := t.In(2)

	return in.Kind() == reflect.Pointer && in.Elem().Name() == m.Name+"Input"
}

// unreadRequestFields lists fields of the structs in server/requests.go that no server code reads.
func unreadRequestFields(sp *serverPackage) []finding {
	var out []finding

	for typeName, st := range structDecls(sp.files["requests.go"]) {
		for _, field := range st.Fields.List {
			for _, name := range field.Names {
				if !sp.reads.has(serverImportPath, typeName, name.Name) {
					out = append(out, finding{kindRequest, typeName + "." + name.Name, "decoded but never read"})
				}
			}
		}
	}

	return out
}

// structDecls returns the struct types declared at the top level of f, by name.
func structDecls(f *ast.File) map[string]*ast.StructType {
	out := map[string]*ast.StructType{}

	for _, decl := range f.Decls {
		gd, ok := decl.(*ast.GenDecl)
		if !ok || gd.Tok != token.TYPE {
			continue
		}

		for _, spec := range gd.Specs {
			ts, ok := spec.(*ast.TypeSpec)
			if !ok {
				continue
			}

			if st, ok := ts.Type.(*ast.StructType); ok {
				out[ts.Name.Name] = st
			}
		}
	}

	return out
}

// unsetResponseFields lists SDK output fields the server's mirrored response struct lacks or
// never sets, and fields of SDK types the server builds directly but never sets.
func unsetResponseFields(sp *serverPackage) []finding {
	var out []finding

	for _, sdk := range sdkOutputs {
		obj := sp.pkg.Scope().Lookup(sdk.Name())

		var mirror *types.Struct
		if obj != nil {
			mirror, _ = obj.Type().Underlying().(*types.Struct)
		}

		for _, f := range sdkFields(sdk) {
			switch {
			case mirror == nil:
				out = append(out, finding{kindResponse, sdk.Name() + "." + f, "no " + sdk.Name() + " in server"})
			case !structHasField(mirror, f):
				out = append(out, finding{kindResponse, sdk.Name() + "." + f, "not in server." + sdk.Name()})
			case !sp.uses.has(serverImportPath, sdk.Name(), f):
				out = append(out, finding{kindResponse, sdk.Name() + "." + f, "never set"})
			}
		}
	}

	for _, sdk := range sdkNested {
		for _, f := range sdkFields(sdk) {
			if !sp.uses.has(sdkTypesPath, sdk.Name(), f) {
				out = append(out, finding{kindResponse, sdk.Name() + "." + f, "never set"})
			}
		}
	}

	return out
}

// sdkFields returns the exported, serialized fields of an SDK shape.
func sdkFields(t reflect.Type) []string {
	var out []string

	for f := range t.Fields() {
		if !f.IsExported() || f.Name == "ResultMetadata" {
			continue
		}

		out = append(out, f.Name)
	}

	return out
}

func structHasField(st *types.Struct, name string) bool {
	for field := range st.Fields() {
		if field.Name() == name {
			return true
		}
	}

	return false
}
