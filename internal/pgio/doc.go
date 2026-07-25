// Package pgio is a low-level toolkit for building and parsing messages in the
// PostgreSQL wire protocol.
/*
pgio provides functions for appending integers to a []byte while doing byte
order conversion, and a bounds-checked Reader for parsing binary values from
untrusted input.
*/
package pgio
