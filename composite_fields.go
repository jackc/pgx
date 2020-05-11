package pgtype

import (
	errors "golang.org/x/xerrors"
)

// CompositeFields scans the fields of a composite type into the elements of the CompositeFields value. To scan a
// nullable value use a *CompositeFields. It will be set to nil in case of null.
type CompositeFields []interface{}

func (cf CompositeFields) DecodeBinary(ci *ConnInfo, src []byte) error {
	if len(cf) == 0 {
		return errors.Errorf("cannot decode into empty CompositeFields")
	}

	if src == nil {
		return errors.Errorf("cannot decode unexpected null into CompositeFields")
	}

	scanner, err := NewCompositeBinaryScanner(src)
	if err != nil {
		return err
	}
	if len(cf) != scanner.FieldCount() {
		return errors.Errorf("SQL composite can't be read, field count mismatch. expected %d , found %d", len(cf), scanner.FieldCount())
	}

	for i := 0; scanner.Scan(); i++ {
		err := ci.Scan(scanner.OID(), BinaryFormatCode, scanner.Bytes(), cf[i])
		if err != nil {
			return err
		}
	}

	if scanner.Err() != nil {
		return scanner.Err()
	}

	return nil
}

func (cf CompositeFields) DecodeText(ci *ConnInfo, src []byte) error {
	if len(cf) == 0 {
		return errors.Errorf("cannot decode into empty CompositeFields")
	}

	if src == nil {
		return errors.Errorf("cannot decode unexpected null into CompositeFields")
	}

	scanner, err := NewCompositeTextScanner(src)
	if err != nil {
		return err
	}

	fieldCount := 0

	for i := 0; scanner.Scan(); i++ {
		err := ci.Scan(0, TextFormatCode, scanner.Bytes(), cf[i])
		if err != nil {
			return err
		}

		fieldCount += 1
	}

	if scanner.Err() != nil {
		return scanner.Err()
	}

	if len(cf) != fieldCount {
		return errors.Errorf("SQL composite can't be read, field count mismatch. expected %d , found %d", len(cf), fieldCount)
	}

	return nil
}
