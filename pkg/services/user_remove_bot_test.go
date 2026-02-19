package services

import (
	"context"
	"errors"
	"testing"

	"github.com/ViktorsBaikers/teldrive/internal/api"
)

func TestUsersRemoveBot_RejectsNonNumericTokenID(t *testing.T) {
	svc := &apiService{}

	tests := []struct {
		name string
		id   string
	}{
		{name: "empty", id: ""},
		{name: "whitespace", id: "   "},
		{name: "percent", id: "%"},
		{name: "underscore", id: "_"},
		{name: "mixed_wildcard", id: "123%"},
		{name: "mixed_wildcard_underscore", id: "123_"},
		{name: "alpha", id: "abc"},
		{name: "zero", id: "0"},
		{name: "negative", id: "-1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := svc.UsersRemoveBot(context.Background(), api.UsersRemoveBotParams{ID: tt.id})
			if err == nil {
				t.Fatalf("expected error for id=%q", tt.id)
			}
			var apiErr *apiError
			if !errors.As(err, &apiErr) {
				t.Fatalf("expected apiError, got %T", err)
			}
			if apiErr.code != 400 {
				t.Fatalf("expected 400 code, got %d", apiErr.code)
			}
		})
	}
}

