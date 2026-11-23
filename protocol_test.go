package avro_test

import (
	"crypto/md5" //nolint:gosec // MD5 is required by the Avro protocol specification.
	"encoding/hex"
	"encoding/json"
	"testing"

	"github.com/iqhive/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMustParseProtocol(t *testing.T) {
	proto := avro.MustParseProtocol(`{"protocol":"test", "namespace": "org.hamba.avro", "doc": "docs"}`)

	assert.IsType(t, &avro.Protocol{}, proto)
}

func TestMustParseProtocol_PanicsOnError(t *testing.T) {
	assert.Panics(t, func() {
		avro.MustParseProtocol("123")
	})
}

func TestNewProtocol_ValidatesName(t *testing.T) {
	_, err := avro.NewProtocol("0test", "", nil, nil)

	assert.Error(t, err)
}

func TestNewMessage(t *testing.T) {
	field, _ := avro.NewField("test", avro.NewPrimitiveSchema(avro.String, nil))
	fields := []*avro.Field{field}
	req, _ := avro.NewRecordSchema("test", "", fields)
	resp := avro.NewPrimitiveSchema(avro.String, nil)
	types := []avro.Schema{avro.NewPrimitiveSchema(avro.String, nil)}
	errs, _ := avro.NewUnionSchema(types)

	msg := avro.NewMessage(req, resp, errs, false)

	assert.Equal(t, req, msg.Request())
	assert.Equal(t, resp, msg.Response())
	assert.Equal(t, errs, msg.Errors())
	assert.False(t, msg.OneWay())
}

func TestParseProtocol(t *testing.T) {
	tests := []struct {
		name    string
		schema  string
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name:    "Valid",
			schema:  `{"protocol":"test", "namespace": "org.hamba.avro", "doc": "docs"}`,
			wantErr: assert.NoError,
		},
		{
			name:    "Empty Namespace",
			schema:  `{"protocol":"test", "namespace": ""}`,
			wantErr: assert.NoError,
		},
		{
			name:    "Invalid Json",
			schema:  `{`,
			wantErr: assert.Error,
		},
		{
			name:    "Invalid Name First Char",
			schema:  `{"protocol":"0test", "namespace": "org.hamba.avro"}`,
			wantErr: assert.Error,
		},
		{
			name:    "Invalid Name Other Char",
			schema:  `{"protocol":"test+", "namespace": "org.hamba.avro"}`,
			wantErr: assert.Error,
		},
		{
			name:    "Empty Name",
			schema:  `{"protocol":"", "namespace": "org.hamba.avro"}`,
			wantErr: assert.Error,
		},
		{
			name:    "No Name",
			schema:  `{"namespace": "org.hamba.avro"}`,
			wantErr: assert.Error,
		},
		{
			name:    "Invalid Namespace",
			schema:  `{"protocol":"test", "namespace": "org.hamba.avro+"}`,
			wantErr: assert.Error,
		},
		{
			name:    "Invalid Type Schema",
			schema:  `{"protocol":"test", "namespace": "org.hamba.avro", "types":["test"]}`,
			wantErr: assert.Error,
		},
		{
			name:    "Type Not Named Schema",
			schema:  `{"protocol":"test", "namespace": "org.hamba.avro", "types":["string"]}`,
			wantErr: assert.Error,
		},
		{
			name:    "Message Not Object",
			schema:  `{"protocol":"test", "namespace": "org.hamba.avro", "messages":{"test":["test"]}}`,
			wantErr: assert.Error,
		},
		{
			name:    "Message Request Invalid Request Json",
			schema:  `{"protocol":"test", "namespace": "org.hamba.avro", "messages":{"test":{"request": "test"}}}`,
			wantErr: assert.Error,
		},
		{
			name:    "Message Request Invalid Field",
			schema:  `{"protocol":"test", "namespace": "org.hamba.avro", "messages":{"test":{"request": [{"name": "foobar"}]}}}`,
			wantErr: assert.Error,
		},
		{
			name:    "Message Response Invalid Schema",
			schema:  `{"protocol":"test", "namespace": "org.hamba.avro", "messages":{"test":{"request": [{"name": "foobar", "type": "string"}], "response": "test"}}}`,
			wantErr: assert.Error,
		},
		{
			name:    "Message Errors Invalid Schema",
			schema:  `{"protocol":"test", "namespace": "org.hamba.avro", "messages":{"test":{"request": [{"name": "foobar", "type": "string"}], "errors": ["test"]}}}`,
			wantErr: assert.Error,
		},
		{
			name:    "Message Errors Record Not Error Schema",
			schema:  `{"protocol":"test", "namespace": "org.hamba.avro", "messages":{"test":{"request": [{"name": "foobar", "type": "string"}], "errors": [{"type":"record", "name":"test", "fields":[{"name": "field", "type": "int"}]}]}}}`,
			wantErr: assert.Error,
		},
		{
			name:    "Message Errors Duplicate Schema",
			schema:  `{"protocol":"test", "namespace": "org.hamba.avro", "messages":{"test":{"request": [{"name": "foobar", "type": "string"}], "errors": ["string"]}}}`,
			wantErr: assert.Error,
		},
		{
			name:    "Message One Way Invalid",
			schema:  `{"protocol":"test", "namespace": "org.hamba.avro", "messages":{"test":{"request": [{"name": "foobar", "type": "string"}], "errors": ["int"], "one-way": true}}}`,
			wantErr: assert.Error,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := avro.ParseProtocol(test.schema)

			test.wantErr(t, err)
		})
	}
}

func TestParseProtocol_DeterminesOneWayMessage(t *testing.T) {
	schema := `{"protocol":"test", "namespace": "org.hamba.avro", "messages":{"test":{"request": [{"name": "foobar", "type": "string"}]}}}`

	proto, err := avro.ParseProtocol(schema)

	require.NoError(t, err)

	msg := proto.Message("test")
	require.NotNil(t, msg)
	assert.True(t, msg.OneWay())
}

func TestParseProtocol_Docs(t *testing.T) {
	schema := `{"protocol":"test", "doc": "foo", "messages":{"test":{"request": [{"name": "foobar", "type": "string"}], "doc": "bar"}}}`

	proto, err := avro.ParseProtocol(schema)
	require.NoError(t, err)

	assert.Equal(t, "foo", proto.Doc())

	msg := proto.Message("test")
	require.NotNil(t, msg)
	assert.Equal(t, "bar", msg.Doc())
}

func TestParseProtocolFile(t *testing.T) {
	protocol, err := avro.ParseProtocolFile("testdata/echo.avpr")

	want := `{"protocol":"Echo","namespace":"org.hamba.avro","types":[{"name":"org.hamba.avro.Ping","type":"record","fields":[{"name":"timestamp","type":"long"},{"name":"text","type":"string"}]},{"name":"org.hamba.avro.Pong","type":"record","fields":[{"name":"timestamp","type":"long"},{"name":"ping","type":"org.hamba.avro.Ping"}]},{"name":"org.hamba.avro.PongError","type":"error","fields":[{"name":"timestamp","type":"long"},{"name":"reason","type":"string"}]}],"messages":{"ping":{"request":[{"name":"ping","type":"org.hamba.avro.Ping"}],"response":"org.hamba.avro.Pong","errors":["org.hamba.avro.PongError"]}}}`
	wantMD5 := "5bc594ae86fc8c209f553ce3bc4291a5"
	require.NoError(t, err)
	assert.Equal(t, want, protocol.String())
	assert.Equal(t, wantMD5, protocol.Hash())
}

func TestParseProtocolFile_InvalidPath(t *testing.T) {
	_, err := avro.ParseProtocolFile("test.avpr")

	assert.Error(t, err)
}

func TestParseProtocol_Types(t *testing.T) {
	protocol, err := avro.ParseProtocolFile("testdata/echo.avpr")

	wantPing := `{"name":"org.hamba.avro.Ping","type":"record","fields":[{"name":"timestamp","type":"long"},{"name":"text","type":"string"}]}`
	wantPong := `{"name":"org.hamba.avro.Pong","type":"record","fields":[{"name":"timestamp","type":"long"},{"name":"ping","type":"org.hamba.avro.Ping"}]}`
	wantPongError := `{"name":"org.hamba.avro.PongError","type":"error","fields":[{"name":"timestamp","type":"long"},{"name":"reason","type":"string"}]}`
	wantLen := 3
	require.NoError(t, err)
	assert.Equal(t, wantLen, len(protocol.Types()))
	assert.Equal(t, wantPing, protocol.Types()[0].String())
	assert.Equal(t, wantPong, protocol.Types()[1].String())
	assert.Equal(t, wantPongError, protocol.Types()[2].String())
}

func TestProtocol_StringStableAndHashConsistent(t *testing.T) {
	protocol := avro.MustParseProtocol(`{"protocol":"test","namespace":"org.hamba.avro","messages":{"z":{"request":[]},"a":{"request":[]},"m":{"request":[]}}}`)
	want := `{"protocol":"test","namespace":"org.hamba.avro","types":[],"messages":{"a":{"request":[]},"m":{"request":[]},"z":{"request":[]}}}`

	for range 100 {
		got := protocol.String()
		require.Equal(t, want, got)
		hash := md5.Sum([]byte(got)) //nolint:gosec // Testing Avro's specified MD5 protocol hash.
		require.Equal(t, protocol.Hash(), hex.EncodeToString(hash[:]))
	}
}

func TestProtocol_StringEscapesMessageNames(t *testing.T) {
	protocol, err := avro.ParseProtocol(`{"protocol":"test","messages":{"bad\"key":{"request":[]},"line\nkey":{"request":[]},"slash\\key":{"request":[]}}}`)
	require.NoError(t, err)

	got := protocol.String()
	want := `{"protocol":"test","namespace":"","types":[],"messages":{"bad\"key":{"request":[]},"line\nkey":{"request":[]},"slash\\key":{"request":[]}}}`
	require.Equal(t, want, got)
	require.True(t, json.Valid([]byte(got)))

	var decoded struct {
		Messages map[string]json.RawMessage `json:"messages"`
	}
	require.NoError(t, json.Unmarshal([]byte(got), &decoded))
	require.Len(t, decoded.Messages, 3)
	for _, name := range []string{"bad\"key", "line\nkey", "slash\\key"} {
		require.Contains(t, decoded.Messages, name)
	}
}

func TestParseProtocol_RejectsInvalidMessageDeclarations(t *testing.T) {
	tests := []struct {
		name   string
		schema string
	}{
		{
			name:   "primitive error",
			schema: `{"protocol":"test","messages":{"m":{"request":[],"response":"string","errors":["int"]}}}`,
		},
		{
			name:   "regular record reference error",
			schema: `{"protocol":"test","types":[{"type":"record","name":"R","fields":[]}],"messages":{"m":{"request":[],"response":"string","errors":["R"]}}}`,
		},
		{
			name:   "duplicate request field",
			schema: `{"protocol":"test","messages":{"m":{"request":[{"name":"x","type":"int"},{"name":"x","type":"string"}],"response":"string"}}}`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := avro.ParseProtocol(test.schema)
			require.Error(t, err)
		})
	}
}

func TestParseProtocol_AcceptsErrorRecords(t *testing.T) {
	tests := []struct {
		name   string
		schema string
	}{
		{
			name:   "definition",
			schema: `{"protocol":"test","messages":{"m":{"request":[],"response":"string","errors":[{"type":"error","name":"E","fields":[]}]}}}`,
		},
		{
			name:   "reference",
			schema: `{"protocol":"test","types":[{"type":"error","name":"E","fields":[]}],"messages":{"m":{"request":[],"response":"string","errors":["E"]}}}`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			protocol, err := avro.ParseProtocol(test.schema)
			require.NoError(t, err)
			require.NotNil(t, protocol.Message("m"))
			require.Len(t, protocol.Message("m").Errors().Types(), 2)
		})
	}
}
