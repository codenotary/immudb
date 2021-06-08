package paseto

import (
	"encoding/json"
	"fmt"
	"time"
)

// JSONToken defines standard token payload claims and allows for additional
// claims to be added. All of the standard claims are optional.
type JSONToken struct {
	// Audience identifies the intended recipients of the token.
	// It should be a string or a URI and is case sensitive.
	Audience string
	// Issuer identifies the entity which issued the token.
	// It should be a string or a URI and is case sensitive.
	Issuer string
	// JTI is a globally unique identifier for the token. It must be created in
	// such a way as to ensure that there is negligible probability that the same
	// value will be used in another token.
	Jti string
	// Subject identifies the principal entity that is the subject of the token.
	// For example, for an authentication token, the subject might be the user ID
	// of a person.
	Subject string
	// Expiration is a time on or after which the token must not be accepted for processing.
	Expiration time.Time
	// IssuedAt is the time at which the token was issued.
	IssuedAt time.Time
	// NotBefore is a time on or before which the token must not be accepted for
	// processing.
	NotBefore time.Time
	claims    map[string]string
}

// Get returns the value of a custom claim, as a string.
// If there is no such claim, an empty string is returned.
func (t *JSONToken) Get(key string) string {
	return t.claims[key]
}

// Set sets the value of a custom claim to the string value provided.
func (t *JSONToken) Set(key string, value string) {
	if t.claims == nil {
		t.claims = make(map[string]string)
	}
	t.claims[key] = value
}

// MarshalJSON implements json.Marshaler interface
func (t JSONToken) MarshalJSON() ([]byte, error) {
	if t.claims == nil {
		t.claims = make(map[string]string)
	}
	if t.Audience != "" {
		t.claims["aud"] = t.Audience
	}
	if t.Issuer != "" {
		t.claims["iss"] = t.Issuer
	}
	if t.Jti != "" {
		t.claims["jti"] = t.Jti
	}
	if t.Subject != "" {
		t.claims["sub"] = t.Subject
	}
	if !t.Expiration.IsZero() {
		t.claims["exp"] = t.Expiration.Format(time.RFC3339)
	}
	if !t.IssuedAt.IsZero() {
		t.claims["iat"] = t.IssuedAt.Format(time.RFC3339)
	}
	if !t.NotBefore.IsZero() {
		t.claims["nbf"] = t.NotBefore.Format(time.RFC3339)
	}

	return json.Marshal(t.claims)
}

// UnmarshalJSON implements json.Unmarshaler interface
func (t *JSONToken) UnmarshalJSON(data []byte) error {
	var err error
	if err := json.Unmarshal(data, &t.claims); err != nil {
		return err
	}

	t.Audience = t.claims["aud"]
	t.Issuer = t.claims["iss"]
	t.Jti = t.claims["jti"]
	t.Subject = t.claims["sub"]

	if timeStr, ok := t.claims["exp"]; ok {
		t.Expiration, err = time.Parse(time.RFC3339, timeStr)
		if err != nil {
			return fmt.Errorf(`incorrect time format for Expiration field "%s". It should be RFC3339`, timeStr)
		}
	}

	if timeStr, ok := t.claims["iat"]; ok {
		t.IssuedAt, err = time.Parse(time.RFC3339, timeStr)
		if err != nil {
			return fmt.Errorf(`incorrect time format for IssuedAt field "%s". It should be RFC3339`, timeStr)
		}
	}

	if timeStr, ok := t.claims["nbf"]; ok {
		t.NotBefore, err = time.Parse(time.RFC3339, timeStr)
		if err != nil {
			return fmt.Errorf(`incorrect time format for NotBefore field "%s". It should be RFC3339`, timeStr)
		}
	}

	return nil
}

// Validate validates a token with the given validators. If no validators are
// specified, then by default it validates the token with ValidAt(time.Now()),
// which checks IssuedAt, NotBefore and Expiration fields against the current
// time.
func (t *JSONToken) Validate(validators ...Validator) error {
	var err error
	if len(validators) == 0 {
		validators = append(validators, ValidAt(time.Now()))
	}
	for _, validator := range validators {
		if err = validator(t); err != nil {
			return err
		}
	}
	return nil
}
