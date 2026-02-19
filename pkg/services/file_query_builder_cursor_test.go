package services

import (
	"strings"
	"testing"

	"github.com/ViktorsBaikers/teldrive/internal/api"
	"github.com/ViktorsBaikers/teldrive/pkg/models"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func TestBuildCursorListQuery_DoesNotUseWindowFunctions(t *testing.T) {
	db, err := gorm.Open(postgres.New(postgres.Config{
		DSN: "host=localhost user=postgres password=postgres dbname=postgres sslmode=disable",
	}), &gorm.Config{
		DryRun:               true,
		DisableAutomaticPing: true,
	})
	if err != nil {
		t.Fatalf("failed to create dry-run db: %v", err)
	}

	base := db.Where("user_id = ?", int64(1)).
		Where("status = ?", "active").
		Where("parent_id = ?", "parent-1")

	params := &api.FilesListParams{
		Operation: api.NewOptFileQueryOperation(api.FileQueryOperationList),
		Sort:      api.NewOptFileQuerySort(api.FileQuerySortID),
		Order:     api.NewOptFileQueryOrder(api.FileQueryOrderAsc),
		Limit:     api.NewOptInt(10),
		Page:      api.NewOptInt(1),
		Cursor:    api.NewOptString("cursor-1"),
	}

	qb, err := buildCursorListQuery(base, params)
	if err != nil {
		t.Fatalf("failed to build cursor query: %v", err)
	}
	q := qb.Limit(params.Limit.Value).Find(&[]models.File{})
	sql := strings.ToLower(q.Statement.SQL.String())

	if strings.Contains(sql, "count(*) over") || strings.Contains(sql, "row_number() over") {
		t.Fatalf("unexpected window function in SQL: %s", q.Statement.SQL.String())
	}
	if !strings.Contains(sql, "order by") || !strings.Contains(sql, "limit") {
		t.Fatalf("expected order/limit in SQL, got: %s", q.Statement.SQL.String())
	}
	if !strings.Contains(sql, "id >") && !strings.Contains(sql, "id\" >") {
		t.Fatalf("expected cursor condition using '>' in SQL, got: %s", q.Statement.SQL.String())
	}
}

func TestBuildCursorListQuery_DescOrderUsesLessThanCursor(t *testing.T) {
	db, err := gorm.Open(postgres.New(postgres.Config{
		DSN: "host=localhost user=postgres password=postgres dbname=postgres sslmode=disable",
	}), &gorm.Config{
		DryRun:               true,
		DisableAutomaticPing: true,
	})
	if err != nil {
		t.Fatalf("failed to create dry-run db: %v", err)
	}

	base := db.Where("user_id = ?", int64(1)).
		Where("status = ?", "active").
		Where("parent_id = ?", "parent-1")

	params := &api.FilesListParams{
		Operation: api.NewOptFileQueryOperation(api.FileQueryOperationList),
		Sort:      api.NewOptFileQuerySort(api.FileQuerySortID),
		Order:     api.NewOptFileQueryOrder(api.FileQueryOrderDesc),
		Limit:     api.NewOptInt(10),
		Page:      api.NewOptInt(1),
		Cursor:    api.NewOptString("cursor-1"),
	}

	qb, err := buildCursorListQuery(base, params)
	if err != nil {
		t.Fatalf("failed to build cursor query: %v", err)
	}
	q := qb.Limit(params.Limit.Value).Find(&[]models.File{})
	sql := strings.ToLower(q.Statement.SQL.String())

	if strings.Contains(sql, "count(*) over") || strings.Contains(sql, "row_number() over") {
		t.Fatalf("unexpected window function in SQL: %s", q.Statement.SQL.String())
	}
	if !strings.Contains(sql, "id <") && !strings.Contains(sql, "id\" <") {
		t.Fatalf("expected cursor condition using '<' in SQL, got: %s", q.Statement.SQL.String())
	}
}

func TestBuildCursorListQuery_NameSortUsesCompositeCursor(t *testing.T) {
	db, err := gorm.Open(postgres.New(postgres.Config{
		DSN: "host=localhost user=postgres password=postgres dbname=postgres sslmode=disable",
	}), &gorm.Config{
		DryRun:               true,
		DisableAutomaticPing: true,
	})
	if err != nil {
		t.Fatalf("failed to create dry-run db: %v", err)
	}

	base := db.Where("user_id = ?", int64(1)).
		Where("status = ?", "active").
		Where("parent_id = ?", "parent-1")

	tok, err := encodeKeysetCursorToken("foo", "cursor-id")
	if err != nil {
		t.Fatalf("failed to encode cursor token: %v", err)
	}

	params := &api.FilesListParams{
		Operation: api.NewOptFileQueryOperation(api.FileQueryOperationList),
		Sort:      api.NewOptFileQuerySort(api.FileQuerySortName),
		Order:     api.NewOptFileQueryOrder(api.FileQueryOrderAsc),
		Limit:     api.NewOptInt(10),
		Page:      api.NewOptInt(1),
		Cursor:    api.NewOptString(tok),
	}

	qb, err := buildCursorListQuery(base, params)
	if err != nil {
		t.Fatalf("failed to build cursor query: %v", err)
	}
	q := qb.Limit(params.Limit.Value).Find(&[]models.File{})
	sql := strings.ToLower(q.Statement.SQL.String())

	if strings.Contains(sql, "count(*) over") || strings.Contains(sql, "row_number() over") {
		t.Fatalf("unexpected window function in SQL: %s", q.Statement.SQL.String())
	}
	if !strings.Contains(sql, "(name, id)") {
		t.Fatalf("expected composite cursor predicate, got: %s", q.Statement.SQL.String())
	}
	if !strings.Contains(sql, "order by") || !strings.Contains(sql, "name") || !strings.Contains(sql, "id") {
		t.Fatalf("expected order by name,id in SQL, got: %s", q.Statement.SQL.String())
	}
}
