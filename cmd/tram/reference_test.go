// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The command reference is generated from the help of the commands and has to
// follow every change of a command.
func TestCommandReferenceIsCurrent(t *testing.T) {
	t.Parallel()

	var b strings.Builder
	require.NoError(t, writeReference(&b))
	committed, err := os.ReadFile(filepath.Join("..", "..", referencePath))
	require.NoError(t, err)
	assert.Equal(t, string(committed), b.String(),
		"%s is out of date: run task generate", referencePath)
}
