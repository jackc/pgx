package pgconn

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"regexp"
	"strings"
)

const (
	PgErrorSuccessfulCompletionCode                            = "00000"
	PgErrorWarningCode                                         = "01000"
	PgErrorWarningDynamicResultSetsReturnedCode                = "0100C"
	PgErrorWarningImplicitZeroBitPaddingCode                   = "01008"
	PgErrorWarningNullValueEliminatedInSetFunctionCode         = "01003"
	PgErrorWarningPrivilegeNotGrantedCode                      = "01007"
	PgErrorWarningPrivilegeNotRevokedCode                      = "01006"
	PgErrorWarningStringDataRightTruncationCode                = "01004"
	PgErrorWarningDeprecatedFeatureCode                        = "01P01"
	PgErrorNoDataCode                                          = "02000"
	PgErrorNoAdditionalDynamicResultSetsReturnedCode           = "02001"
	PgErrorSqlStatementNotYetCompleteCode                      = "03000"
	PgErrorConnectionExceptionCode                             = "08000"
	PgErrorConnectionDoesNotExistCode                          = "08003"
	PgErrorConnectionFailureCode                               = "08006"
	PgErrorSqlclientUnableToEstablishSqlconnectionCode         = "08001"
	PgErrorSqlserverRejectedEstablishmentOfSqlconnectionCode   = "08004"
	PgErrorTransactionResolutionUnknownCode                    = "08007"
	PgErrorProtocolViolationCode                               = "08P01"
	PgErrorTriggeredActionExceptionCode                        = "09000"
	PgErrorFeatureNotSupportedCode                             = "0A000"
	PgErrorInvalidTransactionInitiationCode                    = "0B000"
	PgErrorLocatorExceptionCode                                = "0F000"
	PgErrorLEInvalidSpecificationCode                          = "0F001"
	PgErrorInvalidGrantorCode                                  = "0L000"
	PgErrorInvalidGrantOperationCode                           = "0LP01"
	PgErrorInvalidRoleSpecificationCode                        = "0P000"
	PgErrorDiagnosticsExceptionCode                            = "0Z000"
	PgErrorStackedDiagnosticsAccessedWithoutActiveHandlerCode  = "0Z002"
	PgErrorCaseNotFoundCode                                    = "20000"
	PgErrorCardinalityViolationCode                            = "21000"
	PgErrorDataExceptionCode                                   = "22000"
	PgErrorArrayElementErrorCode                               = "2202E"
	PgErrorArraySubscriptErrorCode                             = "2202E"
	PgErrorCharacterNotInRepertoireCode                        = "22021"
	PgErrorDatetimeFieldOverflowCode                           = "22008"
	PgErrorDatetimeValueOutOfRangeCode                         = "22008"
	PgErrorDivisionByZeroCode                                  = "22012"
	PgErrorErrorInAssignmentCode                               = "22005"
	PgErrorEscapeCharacterConflictCode                         = "2200B"
	PgErrorIndicatorOverflowCode                               = "22022"
	PgErrorIntervalFieldOverflowCode                           = "22015"
	PgErrorInvalidArgumentForLogCode                           = "2201E"
	PgErrorInvalidArgumentForNtileCode                         = "22014"
	PgErrorInvalidArgumentForNthValueCode                      = "22016"
	PgErrorInvalidArgumentForPowerFunctionCode                 = "2201F"
	PgErrorInvalidArgumentForWidthBucketFunctionCode           = "2201G"
	PgErrorInvalidCharacterValueForCastCode                    = "22018"
	PgErrorInvalidDatetimeFormatCode                           = "22007"
	PgErrorInvalidEscapeCharacterCode                          = "22019"
	PgErrorInvalidEscapeOctetCode                              = "2200D"
	PgErrorInvalidEscapeSequenceCode                           = "22025"
	PgErrorNonstandardUseOfEscapeCharacterCode                 = "22P06"
	PgErrorInvalidIndicatorParameterValueCode                  = "22010"
	PgErrorInvalidParameterValueCode                           = "22023"
	PgErrorInvalidPrecedingOrFollowingSizeCode                 = "22013"
	PgErrorInvalidRegularExpressionCode                        = "2201B"
	PgErrorInvalidRowCountInLimitClauseCode                    = "2201W"
	PgErrorInvalidRowCountInResultOffsetClauseCode             = "2201X"
	PgErrorInvalidTablesampleArgumentCode                      = "2202H"
	PgErrorInvalidTablesampleRepeatCode                        = "2202G"
	PgErrorInvalidTimeZoneDisplacementValueCode                = "22009"
	PgErrorInvalidUseOfEscapeCharacterCode                     = "2200C"
	PgErrorMostSpecificTypeMismatchCode                        = "2200G"
	PgErrorNullValueNotAllowedCode                             = "22004"
	PgErrorNullValueNoIndicatorParameterCode                   = "22002"
	PgErrorNumericValueOutOfRangeCode                          = "22003"
	PgErrorSequenceGeneratorLimitExceededCode                  = "2200H"
	PgErrorStringDataLengthMismatchCode                        = "22026"
	PgErrorStringDataRightTruncationCode                       = "22001"
	PgErrorSubstringErrorCode                                  = "22011"
	PgErrorTrimErrorCode                                       = "22027"
	PgErrorUnterminatedCStringCode                             = "22024"
	PgErrorZeroLengthCharacterStringCode                       = "2200F"
	PgErrorFloatingPointExceptionCode                          = "22P01"
	PgErrorInvalidTextRepresentationCode                       = "22P02"
	PgErrorInvalidBinaryRepresentationCode                     = "22P03"
	PgErrorBadCopyFileFormatCode                               = "22P04"
	PgErrorUntranslatableCharacterCode                         = "22P05"
	PgErrorNotAnXmlDocumentCode                                = "2200L"
	PgErrorInvalidXmlDocumentCode                              = "2200M"
	PgErrorInvalidXmlContentCode                               = "2200N"
	PgErrorInvalidXmlCommentCode                               = "2200S"
	PgErrorInvalidXmlProcessingInstructionCode                 = "2200T"
	PgErrorDuplicateJsonObjectKeyValueCode                     = "22030"
	PgErrorInvalidArgumentForSqlJsonDatetimeFunctionCode       = "22031"
	PgErrorInvalidJsonTextCode                                 = "22032"
	PgErrorInvalidSqlJsonSubscriptCode                         = "22033"
	PgErrorMoreThanOneSqlJsonItemCode                          = "22034"
	PgErrorNoSqlJsonItemCode                                   = "22035"
	PgErrorNonNumericSqlJsonItemCode                           = "22036"
	PgErrorNonUniqueKeysInAJsonObjectCode                      = "22037"
	PgErrorSingletonSqlJsonItemRequiredCode                    = "22038"
	PgErrorSqlJsonArrayNotFoundCode                            = "22039"
	PgErrorSqlJsonMemberNotFoundCode                           = "2203A"
	PgErrorSqlJsonNumberNotFoundCode                           = "2203B"
	PgErrorSqlJsonObjectNotFoundCode                           = "2203C"
	PgErrorTooManyJsonArrayElementsCode                        = "2203D"
	PgErrorTooManyJsonObjectMembersCode                        = "2203E"
	PgErrorSqlJsonScalarRequiredCode                           = "2203F"
	PgErrorSqlJsonItemCannotBeCastToTargetTypeCode             = "2203G"
	PgErrorIntegrityConstraintViolationCode                    = "23000"
	PgErrorRestrictViolationCode                               = "23001"
	PgErrorNotNullViolationCode                                = "23502"
	PgErrorForeignKeyViolationCode                             = "23503"
	PgErrorUniqueViolationCode                                 = "23505"
	PgErrorCheckViolationCode                                  = "23514"
	PgErrorExclusionViolationCode                              = "23P01"
	PgErrorInvalidCursorStateCode                              = "24000"
	PgErrorInvalidTransactionStateCode                         = "25000"
	PgErrorActiveSqlTransactionCode                            = "25001"
	PgErrorBranchTransactionAlreadyActiveCode                  = "25002"
	PgErrorHeldCursorRequiresSameIsolationLevelCode            = "25008"
	PgErrorInappropriateAccessModeForBranchTransactionCode     = "25003"
	PgErrorInappropriateIsolationLevelForBranchTransactionCode = "25004"
	PgErrorNoActiveSqlTransactionForBranchTransactionCode      = "25005"
	PgErrorReadOnlySqlTransactionCode                          = "25006"
	PgErrorSchemaAndDataStatementMixingNotSupportedCode        = "25007"
	PgErrorNoActiveSqlTransactionCode                          = "25P01"
	PgErrorInFailedSqlTransactionCode                          = "25P02"
	PgErrorIdleInTransactionSessionTimeoutCode                 = "25P03"
	PgErrorInvalidSqlStatementNameCode                         = "26000"
	PgErrorTriggeredDataChangeViolationCode                    = "27000"
	PgErrorInvalidAuthorizationSpecificationCode               = "28000"
	PgErrorInvalidPasswordCode                                 = "28P01"
	PgErrorDependentPrivilegeDescriptorsStillExistCode         = "2B000"
	PgErrorDependentObjectsStillExistCode                      = "2BP01"
	PgErrorInvalidTransactionTerminationCode                   = "2D000"
	PgErrorSqlRoutineExceptionCode                             = "2F000"
	PgErrorSREFunctionExecutedNoReturnStatementCode            = "2F005"
	PgErrorSREModifyingSqlDataNotPermittedCode                 = "2F002"
	PgErrorSREProhibitedSqlStatementAttemptedCode              = "2F003"
	PgErrorSREReadingSqlDataNotPermittedCode                   = "2F004"
	PgErrorInvalidCursorNameCode                               = "34000"
	PgErrorExternalRoutineExceptionCode                        = "38000"
	PgErrorEREContainingSqlNotPermittedCode                    = "38001"
	PgErrorEREModifyingSqlDataNotPermittedCode                 = "38002"
	PgErrorEREProhibitedSqlStatementAttemptedCode              = "38003"
	PgErrorEREReadingSqlDataNotPermittedCode                   = "38004"
	PgErrorExternalRoutineInvocationExceptionCode              = "39000"
	PgErrorERIEInvalidSqlstateReturnedCode                     = "39001"
	PgErrorERIENullValueNotAllowedCode                         = "39004"
	PgErrorERIETriggerProtocolViolatedCode                     = "39P01"
	PgErrorERIESrfProtocolViolatedCode                         = "39P02"
	PgErrorERIEEventTriggerProtocolViolatedCode                = "39P03"
	PgErrorSavepointExceptionCode                              = "3B000"
	PgErrorSEInvalidSpecificationCode                          = "3B001"
	PgErrorInvalidCatalogNameCode                              = "3D000"
	PgErrorInvalidSchemaNameCode                               = "3F000"
	PgErrorTransactionRollbackCode                             = "40000"
	PgErrorTRIntegrityConstraintViolationCode                  = "40002"
	PgErrorTRSerializationFailureCode                          = "40001"
	PgErrorTRStatementCompletionUnknownCode                    = "40003"
	PgErrorTRDeadlockDetectedCode                              = "40P01"
	PgErrorSyntaxErrorOrAccessRuleViolationCode                = "42000"
	PgErrorSyntaxErrorCode                                     = "42601"
	PgErrorInsufficientPrivilegeCode                           = "42501"
	PgErrorCannotCoerceCode                                    = "42846"
	PgErrorGroupingErrorCode                                   = "42803"
	PgErrorWindowingErrorCode                                  = "42P20"
	PgErrorInvalidRecursionCode                                = "42P19"
	PgErrorInvalidForeignKeyCode                               = "42830"
	PgErrorInvalidNameCode                                     = "42602"
	PgErrorNameTooLongCode                                     = "42622"
	PgErrorReservedNameCode                                    = "42939"
	PgErrorDatatypeMismatchCode                                = "42804"
	PgErrorIndeterminateDatatypeCode                           = "42P18"
	PgErrorCollationMismatchCode                               = "42P21"
	PgErrorIndeterminateCollationCode                          = "42P22"
	PgErrorWrongObjectTypeCode                                 = "42809"
	PgErrorGeneratedAlwaysCode                                 = "428C9"
	PgErrorUndefinedColumnCode                                 = "42703"
	PgErrorUndefinedCursorCode                                 = "34000"
	PgErrorUndefinedDatabaseCode                               = "3D000"
	PgErrorUndefinedFunctionCode                               = "42883"
	PgErrorUndefinedPstatementCode                             = "26000"
	PgErrorUndefinedSchemaCode                                 = "3F000"
	PgErrorUndefinedTableCode                                  = "42P01"
	PgErrorUndefinedParameterCode                              = "42P02"
	PgErrorUndefinedObjectCode                                 = "42704"
	PgErrorDuplicateColumnCode                                 = "42701"
	PgErrorDuplicateCursorCode                                 = "42P03"
	PgErrorDuplicateDatabaseCode                               = "42P04"
	PgErrorDuplicateFunctionCode                               = "42723"
	PgErrorDuplicatePstatementCode                             = "42P05"
	PgErrorDuplicateSchemaCode                                 = "42P06"
	PgErrorDuplicateTableCode                                  = "42P07"
	PgErrorDuplicateAliasCode                                  = "42712"
	PgErrorDuplicateObjectCode                                 = "42710"
	PgErrorAmbiguousColumnCode                                 = "42702"
	PgErrorAmbiguousFunctionCode                               = "42725"
	PgErrorAmbiguousParameterCode                              = "42P08"
	PgErrorAmbiguousAliasCode                                  = "42P09"
	PgErrorInvalidColumnReferenceCode                          = "42P10"
	PgErrorInvalidColumnDefinitionCode                         = "42611"
	PgErrorInvalidCursorDefinitionCode                         = "42P11"
	PgErrorInvalidDatabaseDefinitionCode                       = "42P12"
	PgErrorInvalidFunctionDefinitionCode                       = "42P13"
	PgErrorInvalidPstatementDefinitionCode                     = "42P14"
	PgErrorInvalidSchemaDefinitionCode                         = "42P15"
	PgErrorInvalidTableDefinitionCode                          = "42P16"
	PgErrorInvalidObjectDefinitionCode                         = "42P17"
	PgErrorWithCheckOptionViolationCode                        = "44000"
	PgErrorInsufficientResourcesCode                           = "53000"
	PgErrorDiskFullCode                                        = "53100"
	PgErrorOutOfMemoryCode                                     = "53200"
	PgErrorTooManyConnectionsCode                              = "53300"
	PgErrorConfigurationLimitExceededCode                      = "53400"
	PgErrorProgramLimitExceededCode                            = "54000"
	PgErrorStatementTooComplexCode                             = "54001"
	PgErrorTooManyColumnsCode                                  = "54011"
	PgErrorTooManyArgumentsCode                                = "54023"
	PgErrorObjectNotInPrerequisiteStateCode                    = "55000"
	PgErrorObjectInUseCode                                     = "55006"
	PgErrorCantChangeRuntimeParamCode                          = "55P02"
	PgErrorLockNotAvailableCode                                = "55P03"
	PgErrorUnsafeNewEnumValueUsageCode                         = "55P04"
	PgErrorOperatorInterventionCode                            = "57000"
	PgErrorQueryCanceledCode                                   = "57014"
	PgErrorAdminShutdownCode                                   = "57P01"
	PgErrorCrashShutdownCode                                   = "57P02"
	PgErrorCannotConnectNowCode                                = "57P03"
	PgErrorDatabaseDroppedCode                                 = "57P04"
	PgErrorIdleSessionTimeoutCode                              = "57P05"
	PgErrorSystemErrorCode                                     = "58000"
	PgErrorIoErrorCode                                         = "58030"
	PgErrorUndefinedFileCode                                   = "58P01"
	PgErrorDuplicateFileCode                                   = "58P02"
	PgErrorSnapshotTooOldCode                                  = "72000"
	PgErrorConfigFileErrorCode                                 = "F0000"
	PgErrorLockFileExistsCode                                  = "F0001"
	PgErrorFdwErrorCode                                        = "HV000"
	PgErrorFdwColumnNameNotFoundCode                           = "HV005"
	PgErrorFdwDynamicParameterValueNeededCode                  = "HV002"
	PgErrorFdwFunctionSequenceErrorCode                        = "HV010"
	PgErrorFdwInconsistentDescriptorInformationCode            = "HV021"
	PgErrorFdwInvalidAttributeValueCode                        = "HV024"
	PgErrorFdwInvalidColumnNameCode                            = "HV007"
	PgErrorFdwInvalidColumnNumberCode                          = "HV008"
	PgErrorFdwInvalidDataTypeCode                              = "HV004"
	PgErrorFdwInvalidDataTypeDescriptorsCode                   = "HV006"
	PgErrorFdwInvalidDescriptorFieldIdentifierCode             = "HV091"
	PgErrorFdwInvalidHandleCode                                = "HV00B"
	PgErrorFdwInvalidOptionIndexCode                           = "HV00C"
	PgErrorFdwInvalidOptionNameCode                            = "HV00D"
	PgErrorFdwInvalidStringLengthOrBufferLengthCode            = "HV090"
	PgErrorFdwInvalidStringFormatCode                          = "HV00A"
	PgErrorFdwInvalidUseOfNullPointerCode                      = "HV009"
	PgErrorFdwTooManyHandlesCode                               = "HV014"
	PgErrorFdwOutOfMemoryCode                                  = "HV001"
	PgErrorFdwNoSchemasCode                                    = "HV00P"
	PgErrorFdwOptionNameNotFoundCode                           = "HV00J"
	PgErrorFdwReplyHandleCode                                  = "HV00K"
	PgErrorFdwSchemaNotFoundCode                               = "HV00Q"
	PgErrorFdwTableNotFoundCode                                = "HV00R"
	PgErrorFdwUnableToCreateExecutionCode                      = "HV00L"
	PgErrorFdwUnableToCreateReplyCode                          = "HV00M"
	PgErrorFdwUnableToEstablishConnectionCode                  = "HV00N"
	PgErrorPlpgsqlErrorCode                                    = "P0000"
	PgErrorRaiseExceptionCode                                  = "P0001"
	PgErrorNoDataFoundCode                                     = "P0002"
	PgErrorTooManyRowsCode                                     = "P0003"
	PgErrorAssertFailureCode                                   = "P0004"
	PgErrorInternalErrorCode                                   = "XX000"
	PgErrorDataCorruptedCode                                   = "XX001"
	PgErrorIndexCorruptedCode                                  = "XX002"
)

// SafeToRetry checks if the err is guaranteed to have occurred before sending any data to the server.
func SafeToRetry(err error) bool {
	if e, ok := err.(interface{ SafeToRetry() bool }); ok {
		return e.SafeToRetry()
	}
	return false
}

// Timeout checks if err was was caused by a timeout. To be specific, it is true if err was caused within pgconn by a
// context.DeadlineExceeded or an implementer of net.Error where Timeout() is true.
func Timeout(err error) bool {
	var timeoutErr *errTimeout
	return errors.As(err, &timeoutErr)
}

// PgError represents an error reported by the PostgreSQL server. See
// http://www.postgresql.org/docs/11/static/protocol-error-fields.html for
// detailed field description.
type PgError struct {
	Severity         string
	Code             string
	Message          string
	Detail           string
	Hint             string
	Position         int32
	InternalPosition int32
	InternalQuery    string
	Where            string
	SchemaName       string
	TableName        string
	ColumnName       string
	DataTypeName     string
	ConstraintName   string
	File             string
	Line             int32
	Routine          string
}

func (pe *PgError) Error() string {
	return pe.Severity + ": " + pe.Message + " (SQLSTATE " + pe.Code + ")"
}

// SQLState returns the SQLState of the error.
func (pe *PgError) SQLState() string {
	return pe.Code
}

type connectError struct {
	config *Config
	msg    string
	err    error
}

func (e *connectError) Error() string {
	sb := &strings.Builder{}
	fmt.Fprintf(sb, "failed to connect to `host=%s user=%s database=%s`: %s", e.config.Host, e.config.User, e.config.Database, e.msg)
	if e.err != nil {
		fmt.Fprintf(sb, " (%s)", e.err.Error())
	}
	return sb.String()
}

func (e *connectError) Unwrap() error {
	return e.err
}

type connLockError struct {
	status string
}

func (e *connLockError) SafeToRetry() bool {
	return true // a lock failure by definition happens before the connection is used.
}

func (e *connLockError) Error() string {
	return e.status
}

type parseConfigError struct {
	connString string
	msg        string
	err        error
}

func (e *parseConfigError) Error() string {
	connString := redactPW(e.connString)
	if e.err == nil {
		return fmt.Sprintf("cannot parse `%s`: %s", connString, e.msg)
	}
	return fmt.Sprintf("cannot parse `%s`: %s (%s)", connString, e.msg, e.err.Error())
}

func (e *parseConfigError) Unwrap() error {
	return e.err
}

func normalizeTimeoutError(ctx context.Context, err error) error {
	if err, ok := err.(net.Error); ok && err.Timeout() {
		if ctx.Err() == context.Canceled {
			// Since the timeout was caused by a context cancellation, the actual error is context.Canceled not the timeout error.
			return context.Canceled
		} else if ctx.Err() == context.DeadlineExceeded {
			return &errTimeout{err: ctx.Err()}
		} else {
			return &errTimeout{err: err}
		}
	}
	return err
}

type pgconnError struct {
	msg         string
	err         error
	safeToRetry bool
}

func (e *pgconnError) Error() string {
	if e.msg == "" {
		return e.err.Error()
	}
	if e.err == nil {
		return e.msg
	}
	return fmt.Sprintf("%s: %s", e.msg, e.err.Error())
}

func (e *pgconnError) SafeToRetry() bool {
	return e.safeToRetry
}

func (e *pgconnError) Unwrap() error {
	return e.err
}

// errTimeout occurs when an error was caused by a timeout. Specifically, it wraps an error which is
// context.Canceled, context.DeadlineExceeded, or an implementer of net.Error where Timeout() is true.
type errTimeout struct {
	err error
}

func (e *errTimeout) Error() string {
	return fmt.Sprintf("timeout: %s", e.err.Error())
}

func (e *errTimeout) SafeToRetry() bool {
	return SafeToRetry(e.err)
}

func (e *errTimeout) Unwrap() error {
	return e.err
}

type contextAlreadyDoneError struct {
	err error
}

func (e *contextAlreadyDoneError) Error() string {
	return fmt.Sprintf("context already done: %s", e.err.Error())
}

func (e *contextAlreadyDoneError) SafeToRetry() bool {
	return true
}

func (e *contextAlreadyDoneError) Unwrap() error {
	return e.err
}

// newContextAlreadyDoneError double-wraps a context error in `contextAlreadyDoneError` and `errTimeout`.
func newContextAlreadyDoneError(ctx context.Context) (err error) {
	return &errTimeout{&contextAlreadyDoneError{err: ctx.Err()}}
}

func redactPW(connString string) string {
	if strings.HasPrefix(connString, "postgres://") || strings.HasPrefix(connString, "postgresql://") {
		if u, err := url.Parse(connString); err == nil {
			return redactURL(u)
		}
	}
	quotedDSN := regexp.MustCompile(`password='[^']*'`)
	connString = quotedDSN.ReplaceAllLiteralString(connString, "password=xxxxx")
	plainDSN := regexp.MustCompile(`password=[^ ]*`)
	connString = plainDSN.ReplaceAllLiteralString(connString, "password=xxxxx")
	brokenURL := regexp.MustCompile(`:[^:@]+?@`)
	connString = brokenURL.ReplaceAllLiteralString(connString, ":xxxxxx@")
	return connString
}

func redactURL(u *url.URL) string {
	if u == nil {
		return ""
	}
	if _, pwSet := u.User.Password(); pwSet {
		u.User = url.UserPassword(u.User.Username(), "xxxxx")
	}
	return u.String()
}

type NotPreferredError struct {
	err         error
	safeToRetry bool
}

func (e *NotPreferredError) Error() string {
	return fmt.Sprintf("standby server not found: %s", e.err.Error())
}

func (e *NotPreferredError) SafeToRetry() bool {
	return e.safeToRetry
}

func (e *NotPreferredError) Unwrap() error {
	return e.err
}
