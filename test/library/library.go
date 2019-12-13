package library

import (
	"crypto/rand"
	"fmt"
	"math"
	"math/big"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"k8s.io/apimachinery/pkg/util/wait"
)

var (
	WaitPollInterval = time.Second
	WaitPollTimeout  = 10 * time.Minute
)

// GenerateNameForTest generates a name of the form `prefix + test name + random string` that
// can be used as a resource name. Convert the result to lowercase to use as a dns label.
func GenerateNameForTest(t *testing.T, prefix string) string {
	n, err := rand.Int(rand.Reader, big.NewInt(math.MaxInt64))
	require.NoError(t, err)
	name := []byte(fmt.Sprintf("%s%s-%016x", prefix, t.Name(), n.Int64()))
	// make the name (almost) suitable for use as a dns label
	// only a-z, 0-9, and '-' allowed
	name = regexp.MustCompile("[^a-zA-Z0-9]+").ReplaceAll(name, []byte("-"))
	// collapse multiple `-`
	name = regexp.MustCompile("-+").ReplaceAll(name, []byte("-"))
	// ensure no `-` at beginning or end
	return strings.Trim(string(name), "-")
}

// Poll calls the given function f every given interval
// until it returns no error or the given timeout occurs.
// If a timeout occurs, the last observed error is returned
// or wait.ErrWaitTimeout if no error occurred.
func Poll(interval, timeout time.Duration, f func() error) error {
	var lastErr error

	err := wait.Poll(interval, timeout, func() (bool, error) {
		lastErr = f()
		if lastErr != nil {
			return false, nil
		}
		return true, nil
	})

	if err != nil {
		if err == wait.ErrWaitTimeout && lastErr != nil {
			err = fmt.Errorf("%v: %v", err, lastErr)
		}
	}

	return err
}
