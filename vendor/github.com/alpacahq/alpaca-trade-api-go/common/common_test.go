package common

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type CommonTestSuite struct {
	suite.Suite
}

var _ = setEnv()

func setEnv() (s struct{}) {
	os.Setenv(EnvApiKeyID, "KEY_ID")
	os.Setenv(EnvApiSecretKey, "SECRET_KEY")
	return
}

func TestCommonTestSuite(t *testing.T) {
	suite.Run(t, new(CommonTestSuite))
}

func (s *CommonTestSuite) TestCredentials() {
	assert.Equal(s.T(), "KEY_ID", Credentials().ID)
	assert.Equal(s.T(), "SECRET_KEY", Credentials().Secret)
}
