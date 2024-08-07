package crud

import (
	"errors"
	"fmt"
)

// StringContains checks if a string is present in a slice of strings.
// Returns true if the string is found; otherwise, returns false.
func StringContains(slice []string, str string) bool {
	for _, item := range slice {
		if item == str {
			return true
		}
	}
	return false
}

type Pagination struct {
	Limit      int64         `json:"limit,omitempty;query:limit"`
	Page       int64         `json:"page,omitempty;query:page"`
	Sort       string        `json:"sort,omitempty;query:sort"`
	TotalRows  int64         `json:"total_rows"`
	TotalPages int64         `json:"total_pages"`
	Rows       []interface{} `json:"rows"`
}

func (p *Pagination) GetOffset() int64 {
	return (p.GetPage() - 1) * p.GetLimit()
}
func (p *Pagination) GetLimit() int64 {
	if p.Limit == 0 {
		p.Limit = 10
	}
	return p.Limit
}
func (p *Pagination) GetPage() int64 {
	if p.Page == 0 {
		p.Page = 1
	}
	return p.Page
}
func (p *Pagination) GetSort() string {
	if p.Sort == "" {
		p.Sort = "\"id\" desc"
	}
	return p.Sort
}

// PaginateQueryExtractor extracts pagination query parameters from a Gin context
// and returns a config.Pagination object along with an error message if any.
func PaginateQueryExtractor(page, pageSize int64, sort, direction string, validSortFields []string) (*Pagination,
	error) {
	if page <= 0 {
		page = 1
	}
	if pageSize <= 0 {
		pageSize = 10 // Default to 10 if not specified or invalid
	}
	var sortWithDirection string
	if sort != "" {
		// Validate sort field
		if !StringContains(validSortFields, sort) {
			return nil, errors.New("Invalid sort field")
		}
		if direction == "true" {
			sortWithDirection = fmt.Sprintf("\"%s\" DESC", sort)
		} else {
			sortWithDirection = fmt.Sprintf("\"%s\" ASC", sort)
		}
	}

	return &Pagination{
		Limit: pageSize,
		Page:  page,
		Sort:  sortWithDirection,
	}, nil
}
