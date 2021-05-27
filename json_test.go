package pgproto3

import (
	"encoding/hex"
	"encoding/json"
	"reflect"
	"testing"
)

func TestJSONUnmarshalAuthenticationMD5Password(t *testing.T) {
	data := []byte(`{"Type":"AuthenticationMD5Password", "Salt":[97,98,99,100]}`)
	want := AuthenticationMD5Password{
		Salt: [4]byte{'a', 'b', 'c', 'd'},
	}

	var got AuthenticationMD5Password
	if err := json.Unmarshal(data, &got); err != nil {
		t.Errorf("cannot JSON unmarshal %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Error("unmarshaled AuthenticationMD5Password struct doesn't match expected value")
	}
}

func TestJSONUnmarshalAuthenticationSASL(t *testing.T) {
	data := []byte(`{"Type":"AuthenticationSASL","AuthMechanisms":["SCRAM-SHA-256"]}`)
	want := AuthenticationSASL{
		[]string{"SCRAM-SHA-256"},
	}

	var got AuthenticationSASL
	if err := json.Unmarshal(data, &got); err != nil {
		t.Errorf("cannot JSON unmarshal %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Error("unmarshaled AuthenticationSASL struct doesn't match expected value")
	}
}

func TestJSONUnmarshalAuthenticationSASLContinue(t *testing.T) {
	data := []byte(`{"Type":"AuthenticationSASLContinue", "Data":"1"}`)
	want := AuthenticationSASLContinue{
		Data: []byte{'1'},
	}

	var got AuthenticationSASLContinue
	if err := json.Unmarshal(data, &got); err != nil {
		t.Errorf("cannot JSON unmarshal %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Error("unmarshaled AuthenticationSASLContinue struct doesn't match expected value")
	}
}

func TestJSONUnmarshalAuthenticationSASLFinal(t *testing.T) {
	data := []byte(`{"Type":"AuthenticationSASLFinal", "Data":"1"}`)
	want := AuthenticationSASLFinal{
		Data: []byte{'1'},
	}

	var got AuthenticationSASLFinal
	if err := json.Unmarshal(data, &got); err != nil {
		t.Errorf("cannot JSON unmarshal %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Error("unmarshaled AuthenticationSASLFinal struct doesn't match expected value")
	}
}

func TestJSONUnmarshalBackendKeyData(t *testing.T) {
	data := []byte(`{"Type":"BackendKeyData","ProcessID":8864,"SecretKey":3641487067}`)
	want := BackendKeyData{
		ProcessID: 8864,
		SecretKey: 3641487067,
	}

	var got BackendKeyData
	if err := json.Unmarshal(data, &got); err != nil {
		t.Errorf("cannot JSON unmarshal %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Error("unmarshaled BackendKeyData struct doesn't match expected value")
	}
}

func TestJSONUnmarshalCommandComplete(t *testing.T) {
	data := []byte(`{"Type":"CommandComplete","CommandTag":"SELECT 1"}`)
	want := CommandComplete{
		CommandTag: []byte("SELECT 1"),
	}

	var got CommandComplete
	if err := json.Unmarshal(data, &got); err != nil {
		t.Errorf("cannot JSON unmarshal %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Error("unmarshaled CommandComplete struct doesn't match expected value")
	}
}

func TestJSONUnmarshalCopyBothResponse(t *testing.T) {
	data := []byte(`{"Type":"CopyBothResponse", "OverallFormat": "W"}`)
	want := CopyBothResponse{
		OverallFormat: 'W',
	}

	var got CopyBothResponse
	if err := json.Unmarshal(data, &got); err != nil {
		t.Errorf("cannot JSON unmarshal %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Error("unmarshaled CopyBothResponse struct doesn't match expected value")
	}
}

func TestJSONUnmarshalCopyData(t *testing.T) {
	data := []byte(`{"Type":"CopyData"}`)
	want := CopyData{
		Data: []byte{},
	}

	var got CopyData
	if err := json.Unmarshal(data, &got); err != nil {
		t.Errorf("cannot JSON unmarshal %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Error("unmarshaled CopyData struct doesn't match expected value")
	}
}

func TestJSONUnmarshalCopyInResponse(t *testing.T) {
	data := []byte(`{"Type":"CopyBothResponse", "OverallFormat": "W"}`)
	want := CopyBothResponse{
		OverallFormat: 'W',
	}

	var got CopyBothResponse
	if err := json.Unmarshal(data, &got); err != nil {
		t.Errorf("cannot JSON unmarshal %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Error("unmarshaled CopyBothResponse struct doesn't match expected value")
	}
}

func TestJSONUnmarshalCopyOutResponse(t *testing.T) {
	data := []byte(`{"Type":"CopyOutResponse", "OverallFormat": "W"}`)
	want := CopyOutResponse{
		OverallFormat: 'W',
	}

	var got CopyOutResponse
	if err := json.Unmarshal(data, &got); err != nil {
		t.Errorf("cannot JSON unmarshal %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Error("unmarshaled CopyOutResponse struct doesn't match expected value")
	}
}

func TestJSONUnmarshalDataRow(t *testing.T) {
	data := []byte(`{"Type":"DataRow","Values":[{"text":"abc"},{"text":"this is a test"},{"binary":"000263d3114d2e34"}]}`)
	want := DataRow{
		Values: [][]byte{
			[]byte("abc"),
			[]byte("this is a test"),
			{0, 2, 99, 211, 17, 77, 46, 52},
		},
	}

	var got DataRow
	if err := json.Unmarshal(data, &got); err != nil {
		t.Errorf("cannot JSON unmarshal %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Error("unmarshaled DataRow struct doesn't match expected value")
	}
}

func TestJSONUnmarshalErrorResponse(t *testing.T) {
	data := []byte(`{"Type":"ErrorResponse", "UnknownFields": {"97": "foo"}}`)
	want := ErrorResponse{
		UnknownFields: map[byte]string{
			'a': "foo",
		},
	}

	var got ErrorResponse
	if err := json.Unmarshal(data, &got); err != nil {
		t.Errorf("cannot JSON unmarshal %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Error("unmarshaled ErrorResponse struct doesn't match expected value")
	}
}

func TestJSONUnmarshalFunctionCallResponse(t *testing.T) {
	data := []byte(`{"Type":"FunctionCallResponse"}`)
	want := FunctionCallResponse{}

	var got FunctionCallResponse
	if err := json.Unmarshal(data, &got); err != nil {
		t.Errorf("cannot JSON unmarshal %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Error("unmarshaled FunctionCallResponse struct doesn't match expected value")
	}
}

func TestJSONUnmarshalNoticeResponse(t *testing.T) {
	data := []byte(`{"Type":"NoticeResponse", "UnknownFields": {"97": "foo"}}`)
	want := NoticeResponse{
		UnknownFields: map[byte]string{
			'a': "foo",
		},
	}

	var got NoticeResponse
	if err := json.Unmarshal(data, &got); err != nil {
		t.Errorf("cannot JSON unmarshal %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Error("unmarshaled NoticeResponse struct doesn't match expected value")
	}
}

func TestJSONUnmarshalNotificationResponse(t *testing.T) {
	data := []byte(`{"Type":"NotificationResponse"}`)
	want := NotificationResponse{}

	var got NotificationResponse
	if err := json.Unmarshal(data, &got); err != nil {
		t.Errorf("cannot JSON unmarshal %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Error("unmarshaled NotificationResponse struct doesn't match expected value")
	}
}

func TestJSONUnmarshalParameterDescription(t *testing.T) {
	data := []byte(`{"Type":"ParameterDescription", "ParameterOIDs": [25]}`)
	want := ParameterDescription{
		ParameterOIDs: []uint32{25},
	}

	var got ParameterDescription
	if err := json.Unmarshal(data, &got); err != nil {
		t.Errorf("cannot JSON unmarshal %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Error("unmarshaled ParameterDescription struct doesn't match expected value")
	}
}

func TestJSONUnmarshalParameterStatus(t *testing.T) {
	data := []byte(`{"Type":"ParameterStatus","Name":"TimeZone","Value":"Europe/Amsterdam"}`)
	want := ParameterStatus{
		Name:  "TimeZone",
		Value: "Europe/Amsterdam",
	}

	var got ParameterStatus
	if err := json.Unmarshal(data, &got); err != nil {
		t.Errorf("cannot JSON unmarshal %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Error("unmarshaled ParameterDescription struct doesn't match expected value")
	}
}

func TestJSONUnmarshalReadyForQuery(t *testing.T) {
	data := []byte(`{"Type":"ReadyForQuery","TxStatus":"I"}`)
	want := ReadyForQuery{
		TxStatus: 'I',
	}

	var got ReadyForQuery
	if err := json.Unmarshal(data, &got); err != nil {
		t.Errorf("cannot JSON unmarshal %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Error("unmarshaled ParameterDescription struct doesn't match expected value")
	}
}

func TestJSONUnmarshalRowDescription(t *testing.T) {
	data := []byte(`{"Type":"RowDescription","Fields":[{"Name":"generate_series","TableOID":0,"TableAttributeNumber":0,"DataTypeOID":23,"DataTypeSize":4,"TypeModifier":-1,"Format":0}]}`)
	want := RowDescription{
		Fields: []FieldDescription{
			{
				Name:         []byte("generate_series"),
				DataTypeOID:  23,
				DataTypeSize: 4,
				TypeModifier: -1,
			},
		},
	}

	var got RowDescription
	if err := json.Unmarshal(data, &got); err != nil {
		t.Errorf("cannot JSON unmarshal %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Error("unmarshaled RowDescription struct doesn't match expected value")
	}
}

func TestJSONUnmarshalBind(t *testing.T) {
	var testCases = []struct {
		desc string
		data []byte
	}{
		{
			"textual",
			[]byte(`{"Type":"Bind","DestinationPortal":"","PreparedStatement":"lrupsc_1_0","ParameterFormatCodes":[0],"Parameters":[{"text":"ABC-123"}],"ResultFormatCodes":[0,0,0,0,0,1,1]}`),
		},
		{
			"binary",
			[]byte(`{"Type":"Bind","DestinationPortal":"","PreparedStatement":"lrupsc_1_0","ParameterFormatCodes":[0],"Parameters":[{"binary":"` + hex.EncodeToString([]byte("ABC-123")) + `"}],"ResultFormatCodes":[0,0,0,0,0,1,1]}`),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			var want = Bind{
				PreparedStatement:    "lrupsc_1_0",
				ParameterFormatCodes: []int16{0},
				Parameters:           [][]byte{[]byte("ABC-123")},
				ResultFormatCodes:    []int16{0, 0, 0, 0, 0, 1, 1},
			}

			var got Bind
			if err := json.Unmarshal(tc.data, &got); err != nil {
				t.Errorf("cannot JSON unmarshal %v", err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Error("unmarshaled Bind struct doesn't match expected value")
			}
		})
	}
}

func TestJSONUnmarshalCancelRequest(t *testing.T) {
	data := []byte(`{"Type":"CancelRequest","ProcessID":8864,"SecretKey":3641487067}`)
	want := CancelRequest{
		ProcessID: 8864,
		SecretKey: 3641487067,
	}

	var got CancelRequest
	if err := json.Unmarshal(data, &got); err != nil {
		t.Errorf("cannot JSON unmarshal %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Error("unmarshaled CancelRequest struct doesn't match expected value")
	}
}

func TestJSONUnmarshalClose(t *testing.T) {
	data := []byte(`{"Type":"Close","ObjectType":"S","Name":"abc"}`)
	want := Close{
		ObjectType: 'S',
		Name:       "abc",
	}

	var got Close
	if err := json.Unmarshal(data, &got); err != nil {
		t.Errorf("cannot JSON unmarshal %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Error("unmarshaled Close struct doesn't match expected value")
	}
}

func TestJSONUnmarshalCopyFail(t *testing.T) {
	data := []byte(`{"Type":"CopyFail","Message":"abc"}`)
	want := CopyFail{
		Message: "abc",
	}

	var got CopyFail
	if err := json.Unmarshal(data, &got); err != nil {
		t.Errorf("cannot JSON unmarshal %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Error("unmarshaled CopyFail struct doesn't match expected value")
	}
}

func TestJSONUnmarshalDescribe(t *testing.T) {
	data := []byte(`{"Type":"Describe","ObjectType":"S","Name":"abc"}`)
	want := Describe{
		ObjectType: 'S',
		Name:       "abc",
	}

	var got Describe
	if err := json.Unmarshal(data, &got); err != nil {
		t.Errorf("cannot JSON unmarshal %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Error("unmarshaled Describe struct doesn't match expected value")
	}
}

func TestJSONUnmarshalExecute(t *testing.T) {
	data := []byte(`{"Type":"Execute","Portal":"","MaxRows":0}`)
	want := Execute{}

	var got Execute
	if err := json.Unmarshal(data, &got); err != nil {
		t.Errorf("cannot JSON unmarshal %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Error("unmarshaled Execute struct doesn't match expected value")
	}
}

func TestJSONUnmarshalParse(t *testing.T) {
	data := []byte(`{"Type":"Parse","Name":"lrupsc_1_0","Query":"SELECT id, name FROM t WHERE id = $1","ParameterOIDs":null}`)
	want := Parse{
		Name:  "lrupsc_1_0",
		Query: "SELECT id, name FROM t WHERE id = $1",
	}

	var got Parse
	if err := json.Unmarshal(data, &got); err != nil {
		t.Errorf("cannot JSON unmarshal %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Error("unmarshaled Parse struct doesn't match expected value")
	}
}

func TestJSONUnmarshalPasswordMessage(t *testing.T) {
	data := []byte(`{"Type":"PasswordMessage","Password":"abcdef"}`)
	want := PasswordMessage{
		Password: "abcdef",
	}

	var got PasswordMessage
	if err := json.Unmarshal(data, &got); err != nil {
		t.Errorf("cannot JSON unmarshal %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Error("unmarshaled PasswordMessage struct doesn't match expected value")
	}
}

func TestJSONUnmarshalQuery(t *testing.T) {
	data := []byte(`{"Type":"Query","String":"SELECT 1"}`)
	want := Query{
		String: "SELECT 1",
	}

	var got Query
	if err := json.Unmarshal(data, &got); err != nil {
		t.Errorf("cannot JSON unmarshal %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Error("unmarshaled Query struct doesn't match expected value")
	}
}

func TestJSONUnmarshalSASLInitialResponse(t *testing.T) {
	data := []byte(`{"Type":"SASLInitialResponse", "AuthMechanism":"SCRAM-SHA-256", "Data": "6D"}`)
	want := SASLInitialResponse{
		AuthMechanism: "SCRAM-SHA-256",
		Data:          []byte{109},
	}

	var got SASLInitialResponse
	if err := json.Unmarshal(data, &got); err != nil {
		t.Errorf("cannot JSON unmarshal %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Error("unmarshaled SASLInitialResponse struct doesn't match expected value")
	}
}

func TestJSONUnmarshalSASLResponse(t *testing.T) {
	data := []byte(`{"Type":"SASLResponse","Message":"abc"}`)
	want := SASLResponse{}

	var got SASLResponse
	if err := json.Unmarshal(data, &got); err != nil {
		t.Errorf("cannot JSON unmarshal %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Error("unmarshaled SASLResponse struct doesn't match expected value")
	}
}

func TestJSONUnmarshalStartupMessage(t *testing.T) {
	data := []byte(`{"Type":"StartupMessage","ProtocolVersion":196608,"Parameters":{"database":"testing","user":"postgres"}}`)
	want := StartupMessage{
		ProtocolVersion: 196608,
		Parameters: map[string]string{
			"database": "testing",
			"user":     "postgres",
		},
	}

	var got StartupMessage
	if err := json.Unmarshal(data, &got); err != nil {
		t.Errorf("cannot JSON unmarshal %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Error("unmarshaled StartupMessage struct doesn't match expected value")
	}
}

func TestAuthenticationOK(t *testing.T) {
	data := []byte(`{"Type":"AuthenticationOK"}`)
	want := AuthenticationOk{}

	var got AuthenticationOk
	if err := json.Unmarshal(data, &got); err != nil {
		t.Errorf("cannot JSON unmarshal %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Error("unmarshaled AuthenticationOK struct doesn't match expected value")
	}
}

func TestAuthenticationCleartextPassword(t *testing.T) {
	data := []byte(`{"Type":"AuthenticationCleartextPassword"}`)
	want := AuthenticationCleartextPassword{}

	var got AuthenticationCleartextPassword
	if err := json.Unmarshal(data, &got); err != nil {
		t.Errorf("cannot JSON unmarshal %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Error("unmarshaled AuthenticationCleartextPassword struct doesn't match expected value")
	}
}

func TestAuthenticationMD5Password(t *testing.T) {
	data := []byte(`{"Type":"AuthenticationMD5Password","Salt":[1,2,3,4]}`)
	want := AuthenticationMD5Password{
		Salt: [4]byte{1, 2, 3, 4},
	}

	var got AuthenticationMD5Password
	if err := json.Unmarshal(data, &got); err != nil {
		t.Errorf("cannot JSON unmarshal %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Error("unmarshaled AuthenticationMD5Password struct doesn't match expected value")
	}
}

func TestErrorResponse(t *testing.T) {
	data := []byte(`{"Type":"ErrorResponse","UnknownFields":{"112":"foo"},"Code": "Fail","Position":1,"Message":"this is an error"}`)
	want := ErrorResponse{
		UnknownFields: map[byte]string{
			'p': "foo",
		},
		Code:     "Fail",
		Position: 1,
		Message:  "this is an error",
	}

	var got ErrorResponse
	if err := json.Unmarshal(data, &got); err != nil {
		t.Errorf("cannot JSON unmarshal %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Error("unmarshaled ErrorResponse struct doesn't match expected value")
	}
}
