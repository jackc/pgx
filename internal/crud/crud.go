package crud

import (
	"context"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"math"
)

type RowMapperFunc func(pgx.Row) (interface{}, error)

type DBPool interface {
	Ping(ctx context.Context) error
	Begin(ctx context.Context) (pgx.Tx, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Close()
}

var _ DBPool = (*pgxpool.Pool)(nil)

//go:generate mockery --name CRUD
type CRUD interface {
	BeginTransaction() (pgx.Tx, error)
	CommitTransaction(tx pgx.Tx) error
	RollBackTransaction(tx pgx.Tx) error
	Delete(query string, args ...any) error
	Update(query string, args ...any) error
	Create(query string, args ...any) (interface{}, error)
	GetOne(query string, mapper RowMapperFunc, args ...any) (interface{}, error)
	Get(query string, mapper RowMapperFunc, args ...any) ([]interface{}, error)
	GetWithPagination(finalSQL string, mapper RowMapperFunc, pagination *Pagination, args ...any) (*Pagination, error)
	GetCountForPagination(countSql string, args ...any) (int64, error)
}

type crud struct {
	db   DBPool
	lock bool
}

func NewCrudOperation(db DBPool) CRUD {
	return &crud{
		db: db,
	}
}

func (crud *crud) Delete(query string, args ...any) error {
	// Begin a transaction
	tx, txErr := crud.BeginTransaction()
	if txErr != nil {
		return txErr
	}
	// Execute the DELETE statement within the transaction
	cmdTag, err := tx.Exec(context.Background(), query, args...)
	if err != nil {
		fmt.Println(fmt.Errorf("failed to delete from database: %v", err))
		return crud.RollBackTransaction(tx)
	}
	fmt.Printf("Rows Affected by delete:%v \n", cmdTag.RowsAffected())
	if cmdTag.RowsAffected() == 0 {
		fmt.Printf("No object found with the given criteria to delete")
		return errors.New("no rows affected")
	}
	// Commit the transaction
	return crud.CommitTransaction(tx)
}
func (crud *crud) Create(query string, args ...any) (interface{}, error) {
	var id interface{}
	// Begin a transaction
	tx, txErr := crud.BeginTransaction()
	if txErr != nil {
		return -1, txErr
	}
	if err := tx.QueryRow(context.Background(), query, args...).Scan(&id); err != nil {
		fmt.Println(fmt.Errorf("failed to create object in database: %v", err))
		fmt.Println(fmt.Errorf("rolling back Create transaction"))
		return -1, crud.RollBackTransaction(tx)
	}

	cErr := crud.CommitTransaction(tx)
	if cErr != nil {
		return -1, crud.RollBackTransaction(tx)
	}
	return id, nil
}
func (crud *crud) BeginTransaction() (pgx.Tx, error) {
	tx, err := crud.db.Begin(context.Background())
	if err != nil {
		return nil, err
	}
	return tx, nil
}

func (crud *crud) CommitTransaction(tx pgx.Tx) error {
	if err := tx.Commit(context.Background()); err != nil {
		fmt.Println(fmt.Errorf("failed to commit transaction for update: %v", err))
		return err
	}
	return nil
}

func (crud *crud) RollBackTransaction(tx pgx.Tx) error {
	if rollbackErr := tx.Rollback(context.Background()); rollbackErr != nil {
		fmt.Println(fmt.Errorf("failed to rollback transaction for update: %v", rollbackErr))
		return rollbackErr
	}
	return errors.New(fmt.Sprintf("Failed to update object"))
}

func (crud *crud) Update(query string, args ...any) error {
	// Begin a transaction
	tx, txerr := crud.BeginTransaction()
	if txerr != nil {
		return txerr
	}
	// Execute the UPDATE statement within the transaction
	cmdTag, err := tx.Exec(context.Background(), query, args...)
	if err != nil {
		// If an error occurs, rollback the transaction
		return crud.RollBackTransaction(tx)
	}

	// Check if any row was actually updated
	if cmdTag.RowsAffected() == 0 {
		// No rows affected, might want to handle this as an error or just a no-op
		fmt.Printf("no object found with the given criteria to update\n")
		return errors.New("no rows affected")
	}
	return crud.CommitTransaction(tx)
}

func (crud *crud) GetCountForPagination(countSql string, args ...any) (int64, error) {
	ctx := context.Background()
	var totalRows int64
	err := crud.db.QueryRow(ctx, countSql, args...).Scan(&totalRows)
	if err != nil {
		fmt.Println(fmt.Errorf("Failed to count total rows: %v", err))
		return 0, err
	}
	return totalRows, nil
}
func (crud *crud) GetWithPagination(finalSQL string, mapper RowMapperFunc, pagination *Pagination, args ...any) (*Pagination, error) {
	ctx := context.Background()
	rows, err := crud.db.Query(ctx, finalSQL, args...)
	if err != nil {
		fmt.Println(fmt.Errorf("Failed to execute query: %v", err))
		return nil, err
	}
	defer rows.Close()
	var results []interface{}
	for rows.Next() {
		item, err := mapper(rows)
		if err != nil {
			return nil, err
		}
		results = append(results, item)
	}
	pagination.TotalPages = int64(math.Ceil(float64(pagination.TotalRows) / float64(pagination.GetLimit())))
	pagination.Rows = results
	return pagination, nil
}

func (crud *crud) Get(query string, mapper RowMapperFunc, args ...any) ([]interface{}, error) {

	ctx := context.Background()
	rows, err := crud.db.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []interface{}
	for rows.Next() {
		item, err := mapper(rows)
		if err != nil {
			return nil, err
		}
		results = append(results, item)
	}
	return results, nil
}

func (crud *crud) GetOne(query string, mapper RowMapperFunc, args ...any) (interface{}, error) {
	row := crud.db.QueryRow(context.Background(), query, args...)
	item, err := mapper(row)
	if err != nil {
		return nil, err
	}
	return item, nil
}
