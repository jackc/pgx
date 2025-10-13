package pgconn

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgproto3"
)

func (c *PgConn) oauthAuth(ctx context.Context) error {
	if c.config.OAuthTokenProvider == nil {
		return errors.New("OAuth authentication required but no token provider configured")
	}

	token, err := c.config.OAuthTokenProvider(ctx)
	if err != nil {
		return fmt.Errorf("failed to obtain OAuth token: %w", err)
	}

	// https://www.rfc-editor.org/rfc/rfc7628.html#section-3.1
	initialResponse := []byte("n,,\x01auth=Bearer " + token + "\x01\x01")

	saslInitialResponse := &pgproto3.SASLInitialResponse{
		AuthMechanism: "OAUTHBEARER",
		Data:          initialResponse,
	}
	c.frontend.Send(saslInitialResponse)
	err = c.flushWithPotentialWriteReadDeadlock()
	if err != nil {
		return err
	}

	msg, err := c.receiveMessage()
	if err != nil {
		return err
	}

	switch m := msg.(type) {
	case *pgproto3.AuthenticationOk:
		return nil
	case *pgproto3.AuthenticationSASLContinue:
		// Server sent error response in SASL continue
		// https://www.rfc-editor.org/rfc/rfc7628.html#section-3.2.2
		// https://www.rfc-editor.org/rfc/rfc7628.html#section-3.2.3
		errResponse := struct {
			Status              string `json:"status"`
			Scope               string `json:"scope"`
			OpenIDConfiguration string `json:"openid-configuration"`
		}{}
		err := json.Unmarshal(m.Data, &errResponse)
		if err != nil {
			return fmt.Errorf("invalid OAuth error response from server: %w", err)
		}

		// Per RFC 7628 section 3.2.3, we should send a SASLResponse which only contains \x01.
		// However, since the connection will be closed anyway, we can skip this
		return fmt.Errorf("OAuth authentication failed: %s", errResponse.Status)

	case *pgproto3.ErrorResponse:
		return ErrorResponseToPgError(m)

	default:
		return fmt.Errorf("unexpected message type during OAuth auth: %T", msg)
	}
}
