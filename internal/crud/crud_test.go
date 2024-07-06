package crud

import (
	"errors"
	"github.com/jackc/pgx/v5"
	"github.com/pashagolub/pgxmock/v3"
	"github.com/stretchr/testify/assert"
	"testing"
)

type testStruct struct {
	id int
}

var testMapper = func(row pgx.Row) (interface{}, error) {
	var test testStruct
	err := row.Scan(&test.id)
	if err != nil {
		return nil, err
	}
	return &test, nil
}

func Test_Get_Success(t *testing.T) {
	dbMock, _ := pgxmock.NewPool()
	c := NewCrudOperation(dbMock)
	defer dbMock.Close()
	dbMock.ExpectQuery(`SELECT * `).
		WithArgs(1).
		WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow(1))
	_, err := c.Get(`SELECT * `, testMapper, 1)
	assert.Nil(t, err)
	if e := dbMock.ExpectationsWereMet(); e != nil {
		t.Errorf("there were unfulfilled expectations: %s", e)
	}
}

func Test_Get_Count(t *testing.T) {
	dbMock, _ := pgxmock.NewPool()

	c := NewCrudOperation(dbMock)
	defer dbMock.Close()
	dbMock.ExpectQuery(`SELECT COUNT`).
		WillReturnRows(pgxmock.NewRows([]string{"count"}).
			AddRow(int64(1)))
	count, err := c.GetCountForPagination(`SELECT COUNT(*)`)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), count)
}
func Test_Get_Paginated_Success(t *testing.T) {
	dbMock, _ := pgxmock.NewPool()

	c := NewCrudOperation(dbMock)
	defer dbMock.Close()
	dbMock.ExpectQuery(`SELECT * `).
		WithArgs(1).
		WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow(1))

	_, err := c.GetWithPagination(`SELECT *`, testMapper, &Pagination{}, 1)
	assert.Nil(t, err)
	if e := dbMock.ExpectationsWereMet(); e != nil {
		t.Errorf("there were unfulfilled expectations: %s", e)
	}
}
func Test_GetOne_Success(t *testing.T) {
	dbMock, _ := pgxmock.NewPool()
	c := NewCrudOperation(dbMock)
	defer dbMock.Close()
	dbMock.ExpectQuery(`SELECT * `).
		WithArgs(1).
		WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow(1))
	_, err := c.GetOne(`SELECT * `, testMapper, 1)
	assert.Nil(t, err)
	if e := dbMock.ExpectationsWereMet(); e != nil {
		t.Errorf("there were unfulfilled expectations: %s", e)
	}
}

func Test_CRUDRepository_Delete(t *testing.T) {
	dbMock, _ := pgxmock.NewPool()

	c := NewCrudOperation(dbMock)
	defer dbMock.Close()
	dbMock.ExpectBegin()
	dbMock.ExpectExec(`DELETE`).
		WithArgs(1).WillReturnResult(pgxmock.NewResult("DELETE", 1))
	dbMock.ExpectCommit()

	err := c.Delete(`DELETE FROM "users" WHERE "userEmailId" = $1`, 1)
	if err != nil {
		t.Error(err)
	}
	if e := dbMock.ExpectationsWereMet(); e != nil {
		t.Errorf("there were unfulfilled expectations: %s", e)
	}
}

func Test_CRUDRepository_Create(t *testing.T) {
	dbMock, _ := pgxmock.NewPool()
	c := NewCrudOperation(dbMock)
	defer dbMock.Close()
	dbMock.ExpectBegin()
	dbMock.ExpectQuery(`INSERT INTO`).
		WithArgs(1).WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow(1))
	dbMock.ExpectCommit()
	_, err := c.Create(`INSERT INTO`, 1)
	assert.Nil(t, err)
	if e := dbMock.ExpectationsWereMet(); e != nil {
		t.Errorf("there were unfulfilled expectations: %s", e)
	}
}

func Test_CRUDRepository_Update(t *testing.T) {
	dbMock, _ := pgxmock.NewPool()

	c := NewCrudOperation(dbMock)
	defer dbMock.Close()
	dbMock.ExpectBegin()
	dbMock.ExpectExec(`Update`).
		WithArgs(1).WillReturnResult(pgxmock.NewResult("Update", 1))
	dbMock.ExpectCommit()
	err := c.Update(`Update`, 1)
	assert.Nil(t, err)
	if e := dbMock.ExpectationsWereMet(); e != nil {
		t.Errorf("there were unfulfilled expectations: %s", e)
	}
}

func Test_CRUDRepository_Delete2(t *testing.T) {
	dbMock, _ := pgxmock.NewPool()
	c := NewCrudOperation(dbMock)
	defer dbMock.Close()
	dbMock.ExpectBegin()
	dbMock.ExpectExec(`DELETE`).
		WithArgs(1).WillReturnResult(pgxmock.NewResult("DELETE", 0))

	c.Delete(`DELETE FROM "users" WHERE "userEmailId" = $1`, 1)

	if e := dbMock.ExpectationsWereMet(); e != nil {
		t.Errorf("there were unfulfilled expectations: %s", e)
	}
}

func Test_CRUDRepository_Delete_Fail(t *testing.T) {
	dbMock, _ := pgxmock.NewPool()

	c := NewCrudOperation(dbMock)
	defer dbMock.Close()
	dbMock.ExpectBegin()
	dbMock.ExpectExec(`DELETE`).
		WithArgs(1).WillReturnError(errors.New("some error"))
	dbMock.ExpectRollback()

	err := c.Delete(`DELETE FROM "users" WHERE "userEmailId" = $1`, 1)
	assert.NotNil(t, err)
	if e := dbMock.ExpectationsWereMet(); e != nil {
		t.Errorf("there were unfulfilled expectations: %s", e)
	}
}

func Test_CRUDRepository_Delete_Fail2(t *testing.T) {
	dbMock, _ := pgxmock.NewPool()

	c := NewCrudOperation(dbMock)
	defer dbMock.Close()
	dbMock.ExpectBegin().WillReturnError(errors.New("some error"))
	err := c.Delete(`DELETE FROM "users" WHERE "userEmailId" = $1`, 1)
	assert.NotNil(t, err)
	if e := dbMock.ExpectationsWereMet(); e != nil {
		t.Errorf("there were unfulfilled expectations: %s", e)
	}
}

func Test_CRUDRepository_Delete_Fail3(t *testing.T) {
	dbMock, _ := pgxmock.NewPool()

	c := NewCrudOperation(dbMock)
	defer dbMock.Close()
	dbMock.ExpectBegin()
	dbMock.ExpectExec(`DELETE`).
		WithArgs(1).WillReturnResult(pgxmock.NewResult("DELETE", 1))
	dbMock.ExpectCommit().WillReturnError(errors.New("some error"))

	err := c.Delete(`DELETE FROM "users" WHERE "userEmailId" = $1`, 1)
	assert.NotNil(t, err)
	if e := dbMock.ExpectationsWereMet(); e != nil {
		t.Errorf("there were unfulfilled expectations: %s", e)
	}
}

func Test_CRUDRepository_Delete_Fail5(t *testing.T) {
	dbMock, _ := pgxmock.NewPool()

	c := NewCrudOperation(dbMock)
	defer dbMock.Close()
	dbMock.ExpectBegin()
	dbMock.ExpectExec(`DELETE`).
		WithArgs(1).WillReturnError(errors.New("some error"))
	dbMock.ExpectRollback().WillReturnError(errors.New("some error"))

	err := c.Delete(`DELETE FROM "users" WHERE "userEmailId" = $1`, 1)
	assert.NotNil(t, err)
	if e := dbMock.ExpectationsWereMet(); e != nil {
		t.Errorf("there were unfulfilled expectations: %s", e)
	}
}

func Test_CRUDRepository_Create_Fail1(t *testing.T) {
	dbMock, _ := pgxmock.NewPool()

	c := NewCrudOperation(dbMock)
	defer dbMock.Close()
	dbMock.ExpectBegin().WillReturnError(errors.New("some error"))
	_, err := c.Create(`INSERT INTO`, nil)
	assert.NotNil(t, err)
	if e := dbMock.ExpectationsWereMet(); e != nil {
		t.Errorf("there were unfulfilled expectations: %s", e)
	}
}

func Test_CRUDRepository_Create_Fail2(t *testing.T) {
	dbMock, _ := pgxmock.NewPool()

	c := NewCrudOperation(dbMock)
	defer dbMock.Close()
	dbMock.ExpectBegin()
	dbMock.ExpectQuery(`INSERT INTO`).
		WithArgs(1).
		WillReturnError(errors.New("some error"))
	dbMock.ExpectRollback()
	_, err := c.Create(`INSERT INTO`, 1)
	assert.NotNil(t, err)
	if e := dbMock.ExpectationsWereMet(); e != nil {
		t.Errorf("there were unfulfilled expectations: %s", e)
	}
}

func Test_CRUDRepository_Create_Fail3(t *testing.T) {
	dbMock, _ := pgxmock.NewPool()

	c := NewCrudOperation(dbMock)
	defer dbMock.Close()
	dbMock.ExpectBegin()
	dbMock.ExpectQuery(`INSERT INTO`).
		WithArgs(1).WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow(1))
	dbMock.ExpectCommit().WillReturnError(errors.New("some error"))
	dbMock.ExpectRollback()
	_, err := c.Create(`INSERT INTO`, 1)
	assert.NotNil(t, err)
	if e := dbMock.ExpectationsWereMet(); e != nil {
		t.Errorf("there were unfulfilled expectations: %s", e)
	}
}

func Test_CRUDRepository_Create_Fail4(t *testing.T) {
	dbMock, _ := pgxmock.NewPool()

	c := NewCrudOperation(dbMock)
	defer dbMock.Close()
	dbMock.ExpectBegin()
	dbMock.ExpectQuery(`INSERT INTO`).
		WithArgs(1).WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow(1))
	dbMock.ExpectCommit().WillReturnError(errors.New("some error"))
	dbMock.ExpectRollback().WillReturnError(errors.New("some error"))
	_, err := c.Create(`INSERT INTO`, 1)
	assert.NotNil(t, err)
	if e := dbMock.ExpectationsWereMet(); e != nil {
		t.Errorf("there were unfulfilled expectations: %s", e)
	}
}

func Test_CRUDRepository_Update_Fail1(t *testing.T) {
	dbMock, _ := pgxmock.NewPool()

	c := NewCrudOperation(dbMock)
	defer dbMock.Close()
	dbMock.ExpectBegin().WillReturnError(errors.New("some error"))
	err := c.Update(`Update`, nil)
	assert.NotNil(t, err)
	if e := dbMock.ExpectationsWereMet(); e != nil {
		t.Errorf("there were unfulfilled expectations: %s", e)
	}
}

func Test_CRUDRepository_Update_Fail2(t *testing.T) {
	dbMock, _ := pgxmock.NewPool()

	c := NewCrudOperation(dbMock)
	defer dbMock.Close()
	dbMock.ExpectBegin()
	dbMock.ExpectExec(`Update`).
		WithArgs(1).
		WillReturnError(errors.New("some error"))
	dbMock.ExpectRollback()
	err := c.Update(`Update`, 1)
	assert.NotNil(t, err)
	if e := dbMock.ExpectationsWereMet(); e != nil {
		t.Errorf("there were unfulfilled expectations: %s", e)
	}
}

func Test_CRUDRepository_Update_Fail3(t *testing.T) {
	dbMock, _ := pgxmock.NewPool()

	c := NewCrudOperation(dbMock)
	defer dbMock.Close()
	dbMock.ExpectBegin()
	dbMock.ExpectExec(`Update`).WithArgs(1).WillReturnResult(pgxmock.NewResult("Update", 0))
	err := c.Update(`Update`, 1)
	if e := dbMock.ExpectationsWereMet(); e != nil {
		t.Errorf("there were unfulfilled expectations: %s", e)
	}
	assert.NotNil(t, err)
}

func Test_CRUDRepository_Update_Fail4(t *testing.T) {
	dbMock, _ := pgxmock.NewPool()

	c := NewCrudOperation(dbMock)
	defer dbMock.Close()
	dbMock.ExpectBegin()
	dbMock.ExpectExec(`Update`).WithArgs(1).WillReturnResult(pgxmock.NewResult("Update", 1))
	dbMock.ExpectCommit().WillReturnError(errors.New("some error"))
	err := c.Update(`Update`, 1)
	if e := dbMock.ExpectationsWereMet(); e != nil {
		t.Errorf("there were unfulfilled expectations: %s", e)
	}
	assert.NotNil(t, err)
}
func Test_CRUDRepository_Update_Fail5(t *testing.T) {
	dbMock, _ := pgxmock.NewPool()

	c := NewCrudOperation(dbMock)
	defer dbMock.Close()
	dbMock.ExpectBegin()
	dbMock.ExpectExec(`Update`).
		WithArgs(1).
		WillReturnError(errors.New("some error"))
	dbMock.ExpectRollback().WillReturnError(errors.New("some error"))
	err := c.Update(`Update`, 1)
	assert.NotNil(t, err)
	if e := dbMock.ExpectationsWereMet(); e != nil {
		t.Errorf("there were unfulfilled expectations: %s", e)
	}
}

func Test_GetOne_Fail(t *testing.T) {
	dbMock, _ := pgxmock.NewPool()

	c := NewCrudOperation(dbMock)
	defer dbMock.Close()
	dbMock.ExpectQuery(`SELECT * `).
		WithArgs(1).
		WillReturnRows(pgxmock.NewRows([]string{"id"}))
	_, err := c.GetOne(`SELECT * `, testMapper, 1)
	assert.NotNil(t, err)
	if e := dbMock.ExpectationsWereMet(); e != nil {
		t.Errorf("there were unfulfilled expectations: %s", e)
	}
}

func Test_Get_Paginated_Failure1(t *testing.T) {
	dbMock, _ := pgxmock.NewPool()

	c := NewCrudOperation(dbMock)
	defer dbMock.Close()
	dbMock.ExpectQuery(`SELECT * `).
		WithArgs(1).
		WillReturnError(errors.New("some error"))
	_, err := c.GetWithPagination(`SELECT *`, testMapper, &Pagination{}, 1)
	assert.NotNil(t, err)
	if e := dbMock.ExpectationsWereMet(); e != nil {
		t.Errorf("there were unfulfilled expectations: %s", e)
	}
}

func Test_Get_Paginated_Failure2(t *testing.T) {
	dbMock, _ := pgxmock.NewPool()

	c := NewCrudOperation(dbMock)
	defer dbMock.Close()
	dbMock.ExpectQuery(`SELECT * `).
		WithArgs(1).
		WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow("a"))
	_, err := c.GetWithPagination(`SELECT *`, testMapper, &Pagination{}, 1)
	assert.NotNil(t, err)
	if e := dbMock.ExpectationsWereMet(); e != nil {
		t.Errorf("there were unfulfilled expectations: %s", e)
	}
}

func Test_Get_Paginated_Failure3(t *testing.T) {
	dbMock, _ := pgxmock.NewPool()

	c := NewCrudOperation(dbMock)
	defer dbMock.Close()
	dbMock.ExpectQuery(`SELECT * `).
		WithArgs(1).
		WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow(1))
	_, err := c.GetWithPagination(`SELECT *`, testMapper, &Pagination{}, 1)
	assert.Nil(t, err)
	if e := dbMock.ExpectationsWereMet(); e != nil {
		t.Errorf("there were unfulfilled expectations: %s", e)
	}
}

func Test_Get_Fail1(t *testing.T) {
	dbMock, _ := pgxmock.NewPool()

	c := NewCrudOperation(dbMock)
	defer dbMock.Close()
	dbMock.ExpectQuery(`SELECT * `).
		WithArgs(1).
		WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow("a"))
	_, err := c.Get(`SELECT * `, testMapper, 1)
	assert.NotNil(t, err)
	if e := dbMock.ExpectationsWereMet(); e != nil {
		t.Errorf("there were unfulfilled expectations: %s", e)
	}
}

func Test_Get_Fail2(t *testing.T) {
	dbMock, _ := pgxmock.NewPool()

	c := NewCrudOperation(dbMock)
	defer dbMock.Close()
	dbMock.ExpectQuery(`SELECT * `).
		WithArgs(1).
		WillReturnError(errors.New("some error"))
	_, err := c.Get(`SELECT * `, testMapper, 1)
	assert.NotNil(t, err)
	if e := dbMock.ExpectationsWereMet(); e != nil {
		t.Errorf("there were unfulfilled expectations: %s", e)
	}
}
