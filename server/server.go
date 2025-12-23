package server

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strings"
)

// Server implements http.Handler for DynamoDB JSON API subset.
type Server struct {
	client *Client
}

// NewServer creates an HTTP handler exposing the DynamoDB-compatible API.
func NewServer() *Server {
	return &Server{client: NewClient()}
}

// ServeHTTP dispatches DynamoDB JSON API requests based on X-Amz-Target.
//
//gocyclo:ignore
//gocognit:ignore
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	defer func() {
		err := r.Body.Close()
		if err != nil {
			log.Printf("error closing body: %v", err)
		}
	}()

	op := ""

	target := r.Header.Get("X-Amz-Target")
	if target != "" {
		parts := strings.Split(target, ".")
		op = parts[len(parts)-1]
	}

	decoder := json.NewDecoder(r.Body)
	encoder := json.NewEncoder(w)
	encoder.SetEscapeHTML(false)

	var (
		resp interface{}
		err  error
	)

	switch op {
	case "CreateTable":
		var input CreateTableInput
		if err = decoder.Decode(&input); err == nil {
			resp, err = s.client.CreateTable(context.Background(), &input)
		}
	case "UpdateTable":
		var input UpdateTableInput
		if err = decoder.Decode(&input); err == nil {
			resp, err = s.client.UpdateTable(context.Background(), &input)
		}
	case "DeleteTable":
		var input DeleteTableInput
		if err = decoder.Decode(&input); err == nil {
			resp, err = s.client.DeleteTable(context.Background(), &input)
		}
	case "DescribeTable":
		var input DescribeTableInput
		if err = decoder.Decode(&input); err == nil {
			resp, err = s.client.DescribeTable(context.Background(), &input)
		}
	case "PutItem":
		var input PutItemInput
		if err = decoder.Decode(&input); err == nil {
			resp, err = s.client.PutItem(context.Background(), &input)
		}
	case "DeleteItem":
		var input DeleteItemInput
		if err = decoder.Decode(&input); err == nil {
			resp, err = s.client.DeleteItem(context.Background(), &input)
		}
	case "UpdateItem":
		var input UpdateItemInput
		if err = decoder.Decode(&input); err == nil {
			resp, err = s.client.UpdateItem(context.Background(), &input)
		}
	case "GetItem":
		var input GetItemInput
		if err = decoder.Decode(&input); err == nil {
			resp, err = s.client.GetItem(context.Background(), &input)
		}
	case "Query":
		var input QueryInput
		if err = decoder.Decode(&input); err == nil {
			resp, err = s.client.Query(context.Background(), &input)
		}
	case "Scan":
		var input ScanInput
		if err = decoder.Decode(&input); err == nil {
			resp, err = s.client.Scan(context.Background(), &input)
		}
	case "BatchWriteItem":
		var input BatchWriteItemInput
		if err = decoder.Decode(&input); err == nil {
			resp, err = s.client.BatchWriteItem(context.Background(), &input)
		}
	default:
		http.Error(w, "unsupported operation", http.StatusBadRequest)
		return
	}

	if err != nil {
		writeError(w, err)
		return
	}

	w.Header().Set("Content-Type", "application/x-amz-json-1.0")

	if err := encoder.Encode(resp); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func writeError(w http.ResponseWriter, err error) {
	type errorBody struct {
		Type    string `json:"__type"`
		Message string `json:"message"`
	}

	code := http.StatusBadRequest
	msg := err.Error()
	typ := "InternalFailure"

	if apiErr, ok := err.(interface {
		ErrorCode() string
		ErrorMessage() string
	}); ok {
		typ = apiErr.ErrorCode()
		msg = apiErr.ErrorMessage()
	}

	if typ == "InternalServerError" {
		code = http.StatusInternalServerError
	}

	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(errorBody{Type: typ, Message: msg})
}
