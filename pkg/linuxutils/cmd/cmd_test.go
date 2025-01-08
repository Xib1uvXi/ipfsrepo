package cmd

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNewExecutor(t *testing.T) {
	exec := NewExecutor()

	stdOut, stdErr, err := exec.ExecuteCmd("ls")
	require.NoError(t, err)

	t.Logf("stdout: %s", stdOut)
	t.Logf("stderr: %s", stdErr)

	stdOut, stdErr, err = exec.ExecuteCmd("ls -l")
	require.NoError(t, err)
	t.Logf("stdout: %s", stdOut)
	t.Logf("stderr: %s", stdErr)
}
