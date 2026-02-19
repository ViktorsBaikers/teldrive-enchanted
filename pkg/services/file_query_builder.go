package services

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/ViktorsBaikers/teldrive/internal/api"
	"github.com/ViktorsBaikers/teldrive/internal/utils"
	"github.com/ViktorsBaikers/teldrive/pkg/mapper"
	"github.com/ViktorsBaikers/teldrive/pkg/models"
	"github.com/WinterYukky/gorm-extra-clause-plugin/exclause"

	"gorm.io/gorm"
)

type fileQueryBuilder struct {
	db *gorm.DB
}

type fileResponse struct {
	models.File
	Total int
}

const folderCategory = "folder"

var selectedFields = []string{"id", "name", "type", "mime_type", "category", "hash", "channel_id", "encrypted", "size", "parent_id", "updated_at"}

var resolvePathIDForFilesQuery = resolvePathID

func (afb *fileQueryBuilder) execute(filesQuery *api.FilesListParams, userId int64) (*api.FileList, error) {
	pathID, err := afb.resolvePathIDIfNeeded(filesQuery, userId)
	if err != nil {
		return nil, err
	}

	query := afb.db.Where("user_id = ?", userId).Where("status = ?", filesQuery.Status.Value)
	switch filesQuery.Operation.Value {
	case api.FileQueryOperationList:
		query = afb.applyListFilters(query, filesQuery, pathID)
		if shouldUseCursorPagination(filesQuery) {
			return afb.executeCursorList(query, filesQuery)
		}
	case api.FileQueryOperationFind:
		var err error
		query, err = afb.applyFindFilters(query, filesQuery, userId, pathID)
		if err != nil {
			return nil, &apiError{err: err, code: 400}
		}

	}
	query = afb.buildFileQuery(query, filesQuery, userId, pathID)
	res := []fileResponse{}
	if err := query.Scan(&res).Error; err != nil {
		if strings.Contains(err.Error(), "file not found") {
			return nil, &apiError{err: errors.New("invalid path"), code: 404}
		}
		return nil, &apiError{err: err}
	}
	count := 0

	if len(res) > 0 {
		count = res[0].Total
	}

	files := utils.Map(res, func(item fileResponse) api.File { return *mapper.ToFileOut(item.File) })

	return &api.FileList{Items: files,
		Meta: api.Meta{Count: count,
			TotalPages:  int(math.Ceil(float64(count) / float64(filesQuery.Limit.Value))),
			CurrentPage: filesQuery.Page.Value}}, nil
}

func shouldUseCursorPagination(filesQuery *api.FilesListParams) bool {
	return filesQuery.Operation.Value == api.FileQueryOperationList && filesQuery.Cursor.IsSet()
}

func (afb *fileQueryBuilder) executeCursorList(query *gorm.DB, filesQuery *api.FilesListParams) (*api.FileList, error) {
	var files []models.File
	q, err := buildCursorListQuery(query, filesQuery)
	if err != nil {
		return nil, &apiError{err: err, code: 400}
	}

	limit := filesQuery.Limit.Value
	// Fetch one extra record to know if there is a next page without running COUNT(*).
	if err := q.Limit(limit + 1).Find(&files).Error; err != nil {
		return nil, &apiError{err: err}
	}

	hasMore := len(files) > limit
	if hasMore {
		files = files[:limit]
	}

	meta := api.Meta{
		Count:       0,
		TotalPages:  0,
		CurrentPage: filesQuery.Page.Value,
		HasMore:     api.NewOptBool(hasMore),
	}

	if hasMore && len(files) > 0 {
		next, err := encodeNextCursor(filesQuery.Sort.Value, files[len(files)-1])
		if err != nil {
			return nil, &apiError{err: err}
		}
		meta.NextCursor = api.NewOptString(next)
	}

	out := utils.Map(files, func(item models.File) api.File { return *mapper.ToFileOut(item) })
	return &api.FileList{
		Items: out,
		Meta:  meta,
	}, nil
}

const cursorTokenPrefix = "v1:"

type keysetCursor struct {
	Value string `json:"v"`
	ID    string `json:"id"`
}

func decodeKeysetCursorToken(cursor string) (keysetCursor, error) {
	if !strings.HasPrefix(cursor, cursorTokenPrefix) {
		return keysetCursor{}, fmt.Errorf("invalid cursor token")
	}

	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(cursor, cursorTokenPrefix))
	if err != nil {
		return keysetCursor{}, fmt.Errorf("invalid cursor token")
	}

	var out keysetCursor
	if err := json.Unmarshal(raw, &out); err != nil {
		return keysetCursor{}, fmt.Errorf("invalid cursor token")
	}
	if out.ID == "" {
		return keysetCursor{}, fmt.Errorf("invalid cursor token")
	}
	return out, nil
}

func encodeKeysetCursorToken(v string, id string) (string, error) {
	raw, err := json.Marshal(keysetCursor{Value: v, ID: id})
	if err != nil {
		return "", err
	}
	return cursorTokenPrefix + base64.RawURLEncoding.EncodeToString(raw), nil
}

func encodeNextCursor(sort api.FileQuerySort, last models.File) (string, error) {
	switch sort {
	case api.FileQuerySortID:
		return last.ID, nil
	case api.FileQuerySortName:
		return encodeKeysetCursorToken(last.Name, last.ID)
	case api.FileQuerySortUpdatedAt:
		if last.UpdatedAt == nil {
			return "", fmt.Errorf("cannot encode cursor for missing updated_at")
		}
		return encodeKeysetCursorToken(last.UpdatedAt.Format(time.RFC3339Nano), last.ID)
	case api.FileQuerySortSize:
		var size int64
		if last.Size != nil {
			size = *last.Size
		}
		return encodeKeysetCursorToken(strconv.FormatInt(size, 10), last.ID)
	default:
		return last.ID, nil
	}
}

func buildCursorOrder(sort api.FileQuerySort, orderDir string) string {
	orderField := getValidSortField(sort)
	if sort == api.FileQuerySortSize {
		orderField = "COALESCE(size, 0)"
	}
	if sort == api.FileQuerySortID {
		return fmt.Sprintf("%s %s", orderField, orderDir)
	}
	return fmt.Sprintf("%s %s, id %s", orderField, orderDir, orderDir)
}

func buildCursorListQuery(query *gorm.DB, filesQuery *api.FilesListParams) (*gorm.DB, error) {
	orderDir := getValidOrderDirection(filesQuery.Order.Value)
	sort := filesQuery.Sort.Value

	if cursor, ok := filesQuery.Cursor.Get(); ok && cursor != "" {
		op := ">"
		if orderDir == "DESC" {
			op = "<"
		}

		switch sort {
		case api.FileQuerySortID:
			query = query.Where(fmt.Sprintf("id %s ?", op), cursor)
		case api.FileQuerySortName:
			tok, err := decodeKeysetCursorToken(cursor)
			if err != nil {
				return nil, err
			}
			query = query.Where(fmt.Sprintf("(name, id) %s (?, ?)", op), tok.Value, tok.ID)
		case api.FileQuerySortUpdatedAt:
			tok, err := decodeKeysetCursorToken(cursor)
			if err != nil {
				return nil, err
			}
			t, err := time.Parse(time.RFC3339Nano, tok.Value)
			if err != nil {
				t, err = time.Parse(time.RFC3339, tok.Value)
			}
			if err != nil {
				return nil, fmt.Errorf("invalid cursor token")
			}
			query = query.Where(fmt.Sprintf("(updated_at, id) %s (?, ?)", op), t, tok.ID)
		case api.FileQuerySortSize:
			tok, err := decodeKeysetCursorToken(cursor)
			if err != nil {
				return nil, err
			}
			size, err := strconv.ParseInt(tok.Value, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid cursor token")
			}
			query = query.Where(fmt.Sprintf("(COALESCE(size, 0), id) %s (?, ?)", op), size, tok.ID)
		default:
			query = query.Where(fmt.Sprintf("id %s ?", op), cursor)
		}
	}

	return query.Model(&models.File{}).
		Select(selectedFields).
		Order(buildCursorOrder(sort, orderDir)), nil
}

func shouldResolvePathID(filesQuery *api.FilesListParams) bool {
	if filesQuery.Path.Value == "" {
		return false
	}

	switch filesQuery.Operation.Value {
	case api.FileQueryOperationList:
		return filesQuery.ParentId.Value == ""
	case api.FileQueryOperationFind:
		return filesQuery.DeepSearch.Value && filesQuery.Query.Value != "" ||
			filesQuery.ParentId.Value == "" && filesQuery.Query.Value == ""
	default:
		return false
	}
}

func (afb *fileQueryBuilder) resolvePathIDIfNeeded(filesQuery *api.FilesListParams, userId int64) (*string, error) {
	if !shouldResolvePathID(filesQuery) {
		return nil, nil
	}

	pathID, err := resolvePathIDForFilesQuery(afb.db, filesQuery.Path.Value, userId)
	if err != nil {
		return nil, &apiError{err: errors.New("invalid path"), code: 404}
	}
	return pathID, nil
}

func (afb *fileQueryBuilder) applyListFilters(query *gorm.DB, filesQuery *api.FilesListParams, pathID *string) *gorm.DB {
	if filesQuery.Path.Value != "" && filesQuery.ParentId.Value == "" {
		if pathID == nil {
			query = query.Where("parent_id IS NULL")
		} else {
			query = query.Where("parent_id = ?", *pathID)
		}
	}
	if filesQuery.ParentId.Value != "" {
		query = query.Where("parent_id = ?", filesQuery.ParentId.Value)
	}
	if filesQuery.Type.Value != "" {
		query = query.Where("type = ?", filesQuery.Type.Value)
	}
	query = afb.applyCategoryFilter(query, filesQuery.Category)
	return query
}

func (afb *fileQueryBuilder) applyFindFilters(query *gorm.DB, filesQuery *api.FilesListParams, userId int64, pathID *string) (*gorm.DB, error) {
	var err error
	if filesQuery.DeepSearch.Value && filesQuery.Query.Value != "" && filesQuery.Path.Value != "" {
		query = query.Where("files.id in (select id  from subdirs)")
	}
	if filesQuery.UpdatedAt.Value != "" {
		query, err = afb.applyDateFilters(query, filesQuery.UpdatedAt.Value)
		if err != nil {
			return nil, err
		}
	}

	if filesQuery.Query.Value != "" {
		query = afb.applySearchQuery(query, filesQuery)
	}

	query = afb.applyCategoryFilter(query, filesQuery.Category)

	query = afb.applyFileSpecificFilters(query, filesQuery, userId, pathID)

	return query, nil
}

func (afb *fileQueryBuilder) applyFileSpecificFilters(query *gorm.DB, filesQuery *api.FilesListParams, userId int64, pathID *string) *gorm.DB {
	if filesQuery.Name.Value != "" {
		query = query.Where("name = ?", filesQuery.Name.Value)
	}

	if filesQuery.ParentId.Value != "" {
		if filesQuery.ParentId.Value == "nil" {
			query = query.Where("parent_id is NULL")
		} else {
			query = query.Where("parent_id = ?", filesQuery.ParentId.Value)
		}

	}

	if filesQuery.ParentId.Value == "" && filesQuery.Path.Value != "" && filesQuery.Query.Value == "" {
		if pathID == nil {
			query = query.Where("parent_id IS NULL")
		} else {
			query = query.Where("parent_id = ?", *pathID)
		}
	}

	if filesQuery.Type.Value != "" {
		query = query.Where("type = ?", filesQuery.Type.Value)
	}

	if filesQuery.Shared.Value {
		query = query.Where("id in (SELECT file_id FROM teldrive.file_shares where user_id = ?)", userId)
	}

	return query
}

func (afb *fileQueryBuilder) applyDateFilters(query *gorm.DB, dateFilters string) (*gorm.DB, error) {
	dateFiltersArr := strings.SplitSeq(dateFilters, ",")
	for dateFilter := range dateFiltersArr {
		query = afb.applySingleDateFilter(query, dateFilter)
	}
	return query, nil
}

func (afb *fileQueryBuilder) applySingleDateFilter(query *gorm.DB, dateFilter string) *gorm.DB {
	parts := strings.Split(dateFilter, ":")
	if len(parts) != 2 {
		return query
	}
	op, date := parts[0], parts[1]
	t, err := time.Parse(time.DateOnly, date)
	if err != nil {
		return query
	}

	formattedDate := t.Format(time.RFC3339)
	switch op {
	case "gte":
		query = query.Where("updated_at >= ?", formattedDate)
	case "lte":
		query = query.Where("updated_at <= ?", formattedDate)
	case "eq":
		query = query.Where("updated_at = ?", formattedDate)
	case "gt":
		query = query.Where("updated_at > ?", formattedDate)
	case "lt":
		query = query.Where("updated_at < ?", formattedDate)
	}
	return query
}

func (afb *fileQueryBuilder) applySearchQuery(query *gorm.DB, filesQuery *api.FilesListParams) *gorm.DB {
	switch filesQuery.SearchType.Value {
	case api.FileQuerySearchTypeText:
		query = query.Where("teldrive.clean_name(name) &@~ teldrive.clean_name(?)", filesQuery.Query.Value)
	case api.FileQuerySearchTypeRegex:
		query = query.Where("name &~ ?", filesQuery.Query.Value)
	}
	return query
}

func (afb *fileQueryBuilder) applyCategoryFilter(query *gorm.DB, categories []api.Category) *gorm.DB {
	if len(categories) == 0 {
		return query
	}
	var filterQuery *gorm.DB
	if categories[0] == folderCategory {
		filterQuery = afb.db.Where("type = ?", categories[0])
	} else {
		filterQuery = afb.db.Where("category = ?", categories[0])
	}

	if len(categories) > 1 {
		for _, category := range categories[1:] {
			if category == folderCategory {
				filterQuery = filterQuery.Or("type = ?", category)
			} else {
				filterQuery = filterQuery.Or("category = ?", category)
			}
		}
	}
	return query.Where(filterQuery)
}

func (afb *fileQueryBuilder) buildFileQuery(query *gorm.DB, filesQuery *api.FilesListParams, userId int64, pathID *string) *gorm.DB {
	orderField := getValidSortField(filesQuery.Sort.Value)
	orderDir := getValidOrderDirection(filesQuery.Order.Value)
	op := getOrderOperation(filesQuery)

	return afb.buildSubqueryCTE(query, filesQuery, pathID).Clauses(exclause.NewWith("ranked_scores", afb.db.Model(&models.File{}).Select(orderField, "count(*) OVER () as total",
		fmt.Sprintf("ROW_NUMBER() OVER (ORDER BY %s %s) AS rank", orderField, orderDir)).
		Where(query))).Model(&models.File{}).
		Select(selectedFields, "(select total from ranked_scores limit 1) as total").
		Where(fmt.Sprintf("%s %s (SELECT %s FROM ranked_scores WHERE rank = ?)", orderField, op, orderField),
			max((filesQuery.Page.Value-1)*filesQuery.Limit.Value, 1)).
		Where(query).Order(getOrder(filesQuery)).Limit(filesQuery.Limit.Value)
}

func getValidSortField(sort api.FileQuerySort) string {
	switch sort {
	case api.FileQuerySortName:
		return "name"
	case api.FileQuerySortUpdatedAt:
		return "updated_at"
	case api.FileQuerySortSize:
		return "size"
	case api.FileQuerySortID:
		return "id"
	default:
		return "updated_at"
	}
}

func getValidOrderDirection(order api.FileQueryOrder) string {
	switch order {
	case api.FileQueryOrderAsc:
		return "ASC"
	case api.FileQueryOrderDesc:
		return "DESC"
	default:
		return "DESC"
	}
}

func (afb *fileQueryBuilder) buildSubqueryCTE(query *gorm.DB, filesQuery *api.FilesListParams, pathID *string) *gorm.DB {
	if filesQuery.DeepSearch.Value && filesQuery.Query.Value != "" && filesQuery.Path.Value != "" {
		var whereClause string
		var args []any
		if pathID == nil {
			whereClause = "parent_id IS NULL"
		} else {
			whereClause = "id = ?"
			args = []any{*pathID}
		}
		return afb.db.Clauses(exclause.With{Recursive: true, CTEs: []exclause.CTE{{Name: "subdirs",
			Subquery: exclause.Subquery{DB: afb.db.Model(&models.File{}).Select("id", "parent_id").
				Where(whereClause, args...).
				Clauses(exclause.NewUnion("ALL ?",
					afb.db.Table("teldrive.files as f").Select("f.id", "f.parent_id").
						Joins("inner join subdirs ON f.parent_id = subdirs.id")))}}}})
	}
	return query
}

func getOrder(filesQuery *api.FilesListParams) string {
	orderField := getValidSortField(filesQuery.Sort.Value)
	orderDir := getValidOrderDirection(filesQuery.Order.Value)
	return fmt.Sprintf("%s %s", orderField, orderDir)
}

func getOrderOperation(filesQuery *api.FilesListParams) string {
	if filesQuery.Page.Value == 1 {
		if filesQuery.Order.Value == api.FileQueryOrderAsc {
			return ">="
		} else {
			return "<="
		}
	} else {
		if filesQuery.Order.Value == api.FileQueryOrderAsc {
			return ">"
		} else {
			return "<"
		}
	}
}
