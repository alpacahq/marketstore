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
	EnvApiOAuth     = "APCA_API_OAUTH"
	EnvPolygonKeyID = "POLY_API_KEY_ID"
)

type APIKey struct {
	ID           string
	Secret       string
	OAuth        string
	PolygonKeyID string
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
	return &APIKey{
		ID:           os.Getenv(EnvApiKeyID),
		PolygonKeyID: polygonKeyID,
		Secret:       os.Getenv(EnvApiSecretKey),
		OAuth:        os.Getenv(EnvApiOAuth),
	}
}
