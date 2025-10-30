/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package utils

import (
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"

	"github.com/hyperledger/fabric-x-orderer/testutil/tlsgen"

	"github.com/stretchr/testify/require"
)

func TestReadPemFile(t *testing.T) {
	// reading a file with an empty path
	_, err := ReadPem("")
	require.Error(t, err)

	// reading an existing file which is not a PEM file
	data, err := ReadPem("/dev/null")
	require.NoError(t, err)
	pbl, _ := pem.Decode(data)
	require.Nil(t, pbl)

	// reading a valid pem
	dir := t.TempDir()

	serverCA, err := tlsgen.NewCA()
	require.NoError(t, err)
	serverKeyPair, err := serverCA.NewServerCertKeyPair("127.0.0.1")
	require.NoError(t, err)

	err = os.WriteFile(filepath.Join(dir, "cert.pem"), serverKeyPair.Cert, 0o640)
	require.NoError(t, err)
	cert, err := ReadPem(filepath.Join(dir, "cert.pem"))
	require.NoError(t, err)
	require.NotNil(t, cert)
}
