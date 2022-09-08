package api

import (
	"os"
	"sync"
)

var (
	once sync.Once
	key  *APIKey
)

const (
	EnvApiKeyID     = "APCA_API_KEY_ID"
	EnvApiSecretKey = "APCA_API_SECRET_KEY"
	EnvAuthMethod   = "APCA_API_AUTH_METHOD"
	EnvApiOAuth     = "APCA_API_OAUTH"
	EnvPolygonKeyID = "POLY_API_KEY_ID"
)

type APIKey struct {
	ID           string
	Secret       string
	OAuth        string
	PolygonKeyID string
	AuthMethod   AuthMethod
}

// Credentials returns the user's Alpaca API key ID
// and secret for use through the SDK.
func Credentials() *APIKey {
	var polygonKeyID string
	if s := os.Getenv(EnvPolygonKeyID); s != "" {
		polygonKeyID = s
	} else {
		polygonKeyID = os.Getenv(EnvApiKeyID)
	}
	apiKey := &APIKey{
		ID:           os.Getenv(EnvApiKeyID),
		PolygonKeyID: polygonKeyID,
		Secret:       os.Getenv(EnvApiSecretKey),
		OAuth:        os.Getenv(EnvApiOAuth),
	}
	if am := os.Getenv(EnvAuthMethod); am != "" {
		apiKey.AuthMethod = AuthMethodFromString(am)
	}
	return apiKey
}

func AuthMethodFromString(s string) AuthMethod {
	switch s {
	case "basic":
		return BasicAuth
	case "header":
		return HeaderAuth
	default:
		return BasicAuth
	}
}
