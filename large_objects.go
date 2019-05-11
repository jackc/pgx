package pgx

import (
	"context"
	"io"

	"github.com/jackc/pgtype"
	errors "golang.org/x/xerrors"
)

// LargeObjects is a structure used to access the large objects API. It is only
// valid within the transaction where it was created.
//
// For more details see: http://www.postgresql.org/docs/current/static/largeobjects.html
type LargeObjects struct {
	tx *Tx
}

const largeObjectFns = `select proname, oid from pg_catalog.pg_proc
where proname in (
'lo_open',
'lo_close',
'lo_create',
'lo_unlink',
'lo_lseek',
'lo_lseek64',
'lo_tell',
'lo_tell64',
'lo_truncate',
'lo_truncate64',
'loread',
'lowrite')
and pronamespace = (select oid from pg_catalog.pg_namespace where nspname = 'pg_catalog')`

// LargeObjects returns a LargeObjects instance for the transaction.
func (tx *Tx) LargeObjects() LargeObjects {
	return LargeObjects{tx: tx}
}

type LargeObjectMode int32

const (
	LargeObjectModeWrite LargeObjectMode = 0x20000
	LargeObjectModeRead  LargeObjectMode = 0x40000
)

// Create creates a new large object. If oid is zero, the server assigns an
// unused OID.
func (o *LargeObjects) Create(oid pgtype.OID) (pgtype.OID, error) {
	_, err := o.tx.Prepare(context.TODO(), "lo_create", "select lo_create($1)")
	if err != nil {
		return 0, err
	}

	err = o.tx.QueryRow(context.TODO(), "lo_create", oid).Scan(&oid)
	return oid, err
}

// Open opens an existing large object with the given mode.
func (o *LargeObjects) Open(oid pgtype.OID, mode LargeObjectMode) (*LargeObject, error) {
	_, err := o.tx.Prepare(context.TODO(), "lo_open", "select lo_open($1, $2)")
	if err != nil {
		return nil, err
	}

	var fd int32
	err = o.tx.QueryRow(context.TODO(), "lo_open", oid, mode).Scan(&fd)
	if err != nil {
		return nil, err
	}
	return &LargeObject{fd: fd, tx: o.tx}, nil
}

// Unlink removes a large object from the database.
func (o *LargeObjects) Unlink(oid pgtype.OID) error {
	_, err := o.tx.Prepare(context.TODO(), "lo_unlink", "select lo_unlink($1)")
	if err != nil {
		return err
	}

	var result int32
	err = o.tx.QueryRow(context.TODO(), "lo_unlink", oid).Scan(&result)
	if err != nil {
		return err
	}

	if result != 1 {
		return errors.New("failed to remove large object")
	}

	return nil
}

// A LargeObject is a large object stored on the server. It is only valid within
// the transaction that it was initialized in. It implements these interfaces:
//
//    io.Writer
//    io.Reader
//    io.Seeker
//    io.Closer
type LargeObject struct {
	fd int32
	tx *Tx
}

// Write writes p to the large object and returns the number of bytes written
// and an error if not all of p was written.
func (o *LargeObject) Write(p []byte) (int, error) {
	_, err := o.tx.Prepare(context.TODO(), "lowrite", "select lowrite($1, $2)")
	if err != nil {
		return 0, err
	}

	var n int
	err = o.tx.QueryRow(context.TODO(), "lowrite", o.fd, p).Scan(&n)
	if err != nil {
		return n, err
	}

	if n < 0 {
		return 0, errors.New("failed to write to large object")
	}

	return n, nil
}

// Read reads up to len(p) bytes into p returning the number of bytes read.
func (o *LargeObject) Read(p []byte) (int, error) {
	_, err := o.tx.Prepare(context.TODO(), "loread", "select loread($1, $2)")
	if err != nil {
		return 0, err
	}

	var res []byte
	err = o.tx.QueryRow(context.TODO(), "loread", o.fd, len(p)).Scan(&res)
	copy(p, res)
	if err != nil {
		return len(res), err
	}

	if len(res) < len(p) {
		err = io.EOF
	}
	return len(res), err
}

// Seek moves the current location pointer to the new location specified by offset.
func (o *LargeObject) Seek(offset int64, whence int) (n int64, err error) {
	_, err = o.tx.Prepare(context.TODO(), "lo_lseek64", "select lo_lseek64($1, $2, $3)")
	if err != nil {
		return 0, err
	}

	err = o.tx.QueryRow(context.TODO(), "lo_lseek64", o.fd, offset, whence).Scan(&n)
	return n, err
}

// Tell returns the current read or write location of the large object
// descriptor.
func (o *LargeObject) Tell() (n int64, err error) {
	_, err = o.tx.Prepare(context.TODO(), "lo_tell64", "select lo_tell64($1)")
	if err != nil {
		return 0, err
	}

	err = o.tx.QueryRow(context.TODO(), "lo_tell64", o.fd).Scan(&n)
	return n, err
}

// Trunctes the large object to size.
func (o *LargeObject) Truncate(size int64) (err error) {
	_, err = o.tx.Prepare(context.TODO(), "lo_truncate64", "select lo_truncate64($1, $2)")
	if err != nil {
		return err
	}

	_, err = o.tx.Exec(context.TODO(), "lo_truncate64", o.fd, size)
	return err
}

// Close closees the large object descriptor.
func (o *LargeObject) Close() error {
	_, err := o.tx.Prepare(context.TODO(), "lo_close", "select lo_close($1)")
	if err != nil {
		return err
	}

	_, err = o.tx.Exec(context.TODO(), "lo_close", o.fd)
	return err
}
