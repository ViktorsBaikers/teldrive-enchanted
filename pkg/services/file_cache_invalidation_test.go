package services

import (
	"context"
	"testing"
	"time"

	"github.com/tgdrive/teldrive/internal/api"
	"github.com/tgdrive/teldrive/internal/cache"
	"github.com/tgdrive/teldrive/pkg/models"
)

func TestInvalidateAllFilePathCache(t *testing.T) {
	ctx := context.Background()
	c := cache.NewMemoryCache(1 << 20)

	pathKey := cache.KeyFilePath("file-1")
	stalePathKey := cache.Key(pathKey, "stale")
	otherKey := cache.KeyFile("file-1")

	if err := c.Set(ctx, pathKey, "path-value", time.Minute); err != nil {
		t.Fatalf("set path key: %v", err)
	}
	if err := c.Set(ctx, stalePathKey, "stale-path-value", time.Minute); err != nil {
		t.Fatalf("set stale path key: %v", err)
	}
	if err := c.Set(ctx, otherKey, "file-metadata", time.Minute); err != nil {
		t.Fatalf("set non-path key: %v", err)
	}

	invalidateAllFilePathCache(ctx, c)

	var value string
	if err := c.Get(ctx, pathKey, &value); err == nil {
		t.Fatalf("expected path key to be removed")
	}
	if err := c.Get(ctx, stalePathKey, &value); err == nil {
		t.Fatalf("expected stale path key to be removed")
	}
	if err := c.Get(ctx, otherKey, &value); err != nil {
		t.Fatalf("expected non-path key to remain: %v", err)
	}
}

func TestShouldInvalidateDescendantPathCache(t *testing.T) {
	tests := []struct {
		name string
		file models.File
		req  *api.FileUpdate
		want bool
	}{
		{
			name: "folder rename invalidates descendants",
			file: models.File{Type: "folder"},
			req:  &api.FileUpdate{Name: api.NewOptString("renamed-folder")},
			want: true,
		},
		{
			name: "folder move invalidates descendants",
			file: models.File{Type: "folder"},
			req:  &api.FileUpdate{ParentId: api.NewOptString("new-parent")},
			want: true,
		},
		{
			name: "file rename does not invalidate descendants",
			file: models.File{Type: "file"},
			req:  &api.FileUpdate{Name: api.NewOptString("renamed-file")},
			want: false,
		},
		{
			name: "folder content update does not invalidate descendants",
			file: models.File{Type: "folder"},
			req:  &api.FileUpdate{Size: api.NewOptInt64(10)},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := shouldInvalidateDescendantPathCache(tt.file, tt.req); got != tt.want {
				t.Fatalf("shouldInvalidateDescendantPathCache() = %v, want %v", got, tt.want)
			}
		})
	}
}
