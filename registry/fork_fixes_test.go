package registry_test

// Regression tests for defects found in iqhive/avro after the fork from
// hamba/avro and iskorotkov/avro.

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iqhive/avro/v2/registry"
)

// A subject was joined into the request path unescaped, and the path was
// then cleaned, so a subject holding "/", "?", "#" or ".." reached another
// endpoint: DeleteSubject of "../config" sent a DELETE for the registry's
// global configuration.
func TestFix_SubjectIsEscapedInTheRequestPath(t *testing.T) {
	var method, path, query string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method, path, query = r.Method, r.URL.EscapedPath(), r.URL.RawQuery
		_, _ = w.Write([]byte(`[]`))
	}))
	defer srv.Close()
	client, err := registry.NewClient(srv.URL)
	require.NoError(t, err)

	for subject, want := range map[string]string{
		"../config": "/subjects/..%2Fconfig",
		"a/b?x=1#f": "/subjects/a%2Fb%3Fx=1%23f",
		"..":        "/subjects/%2E%2E",
	} {
		_, err := client.DeleteSubject(t.Context(), subject)
		require.NoError(t, err)
		assert.Equal(t, http.MethodDelete, method)
		assert.Equal(t, want, path, "the path for subject %q", subject)
		assert.Empty(t, query, "the query for subject %q", subject)

		_, err = client.GetVersions(t.Context(), subject)
		require.NoError(t, err)
		assert.Equal(t, want+"/versions", path, "the path for subject %q", subject)
		assert.Empty(t, query, "the query for subject %q", subject)
	}
}

// A message whose payload encodes to no bytes, such as an empty record, was
// refused as too short: the decoder wanted a byte after the 5-byte header.
func TestFix_DecodeEmptyPayload(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"schema":"{\"type\":\"record\",\"name\":\"Empty\",\"fields\":[]}"}`))
	}))
	defer srv.Close()
	client, err := registry.NewClient(srv.URL)
	require.NoError(t, err)

	var v struct{}
	assert.NoError(t, registry.NewDecoder(client).Decode(t.Context(), []byte{0, 0, 0, 0, 1}, &v))
}
