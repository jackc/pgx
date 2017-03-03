// Package pgio a extremely low-level IO toolkit for the PostgreSQL wire protocol.
/*
pgio provides functions for reading and writing integers from io.Reader and
io.Writer while doing byte order conversion. It publishes interfaces which
readers and writers may implement to decode and encode messages with the minimum
of memory allocations.
*/
package pgio
