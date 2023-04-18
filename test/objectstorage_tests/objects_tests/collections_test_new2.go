import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type MySuite struct {
	suite.Suite
	MyObject *MyObject
}

func (s *MySuite) SetupSuite() {
	// Set up your object here
	s.MyObject = httpexpect.Default(t, baseURL)
}

func (s *MySuite) TestMyFirstTest() {
	// Use your object here
	assert.Equal(s.T(), s.MyObject.Foo(), "foo should be equal")
}

func (s *MySuite) TestMySecondTest() {
	// Use your object here
	assert.Equal(s.T(), s.MyObject.Bar(), "bar should be equal")
}

func TestSuite(t *testing.T) {
	suite.Run(t, new(MySuite))
}
