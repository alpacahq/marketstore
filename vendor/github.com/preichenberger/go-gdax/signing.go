package gdax

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"strconv"
	"time"
)

func generateSig(message, secret string) (string, error) {
	key, err := base64.StdEncoding.DecodeString(secret)
	if err != nil {
		return "", err
	}

	signature := hmac.New(sha256.New, key)
	_, err = signature.Write([]byte(message))
	if err != nil {
		return "", err
	}

	return base64.StdEncoding.EncodeToString(signature.Sum(nil)), nil
}

func (m Message) Sign(secret, key, passphrase string) (SignedMessage, error) {

	method := "GET"
	url := "/users/self/verify"
	timestamp := strconv.FormatInt(time.Now().Unix(), 10)
	sig, err := generateSig(fmt.Sprintf("%s%s%s", timestamp, method, url), secret)

	return SignedMessage{
		Message:    m,
		Key:        key,
		Passphrase: passphrase,
		Timestamp:  strconv.FormatInt(time.Now().Unix(), 10),
		Signature:  sig,
	}, err
}
