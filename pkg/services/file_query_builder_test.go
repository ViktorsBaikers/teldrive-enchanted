package services

import (
	"errors"
	"testing"

	"github.com/ViktorsBaikers/teldrive/internal/api"
	"gorm.io/gorm"
)

func TestShouldResolvePathID(t *testing.T) {
	tests := []struct {
		name   string
		params api.FilesListParams
		want   bool
	}{
		{
			name: "list operation resolves path when no parent id",
			params: api.FilesListParams{
				Path:      api.NewOptString("/root/folder"),
				Operation: api.NewOptFileQueryOperation(api.FileQueryOperationList),
			},
			want: true,
		},
		{
			name: "list operation skips path resolution when parent id is provided",
			params: api.FilesListParams{
				Path:      api.NewOptString("/root/stale"),
				ParentId:  api.NewOptString("parent-1"),
				Operation: api.NewOptFileQueryOperation(api.FileQueryOperationList),
			},
			want: false,
		},
		{
			name: "find deep search resolves path",
			params: api.FilesListParams{
				Path:       api.NewOptString("/root/folder"),
				Query:      api.NewOptString("query"),
				DeepSearch: api.NewOptBool(true),
				Operation:  api.NewOptFileQueryOperation(api.FileQueryOperationFind),
			},
			want: true,
		},
		{
			name: "find operation skips path resolution when parent id is provided",
			params: api.FilesListParams{
				Path:      api.NewOptString("/root/stale"),
				ParentId:  api.NewOptString("parent-1"),
				Operation: api.NewOptFileQueryOperation(api.FileQueryOperationFind),
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := shouldResolvePathID(&tt.params); got != tt.want {
				t.Fatalf("shouldResolvePathID() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestResolvePathIDIfNeeded(t *testing.T) {
	originalResolver := resolvePathIDForFilesQuery
	t.Cleanup(func() {
		resolvePathIDForFilesQuery = originalResolver
	})

	t.Run("does not resolve path when query does not depend on path", func(t *testing.T) {
		calls := 0
		resolvePathIDForFilesQuery = func(_ *gorm.DB, _ string, _ int64) (*string, error) {
			calls++
			return nil, errors.New("unexpected resolver call")
		}

		builder := &fileQueryBuilder{}
		params := &api.FilesListParams{
			Path:      api.NewOptString("/root/stale"),
			ParentId:  api.NewOptString("parent-1"),
			Operation: api.NewOptFileQueryOperation(api.FileQueryOperationList),
		}

		pathID, err := builder.resolvePathIDIfNeeded(params, 1)
		if err != nil {
			t.Fatalf("resolvePathIDIfNeeded returned unexpected error: %v", err)
		}
		if pathID != nil {
			t.Fatalf("expected nil path ID when path resolution is skipped")
		}
		if calls != 0 {
			t.Fatalf("expected resolver not to be called, got %d calls", calls)
		}
	})

	t.Run("returns 404 api error when required path resolution fails", func(t *testing.T) {
		resolvePathIDForFilesQuery = func(_ *gorm.DB, _ string, _ int64) (*string, error) {
			return nil, errors.New("path not found")
		}

		builder := &fileQueryBuilder{}
		params := &api.FilesListParams{
			Path:      api.NewOptString("/root/missing"),
			Operation: api.NewOptFileQueryOperation(api.FileQueryOperationList),
		}

		_, err := builder.resolvePathIDIfNeeded(params, 1)
		if err == nil {
			t.Fatal("expected error when required path resolution fails")
		}
		var apiErr *apiError
		if !errors.As(err, &apiErr) {
			t.Fatalf("expected apiError, got %T", err)
		}
		if apiErr.code != 404 {
			t.Fatalf("expected 404 code, got %d", apiErr.code)
		}
	})
}
