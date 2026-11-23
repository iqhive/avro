package avro_test

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/iqhive/avro/v2"
	"github.com/modern-go/reflect2"
)

type TestInterface interface {
	SomeFunc() int
}

type TestRecord struct {
	A int64  `avro:"a"`
	B string `avro:"b"`
}

func (*TestRecord) SomeFunc() int {
	return 0
}

type UnionRecordParent struct {
	UnionRecord *UnionRecord `avro:"union"`
}

type UnionRecord struct {
	Int  *int
	Test *TestRecord
}

func (u *UnionRecord) ToAny() (any, error) {
	if u.Int != nil {
		return u.Int, nil
	} else if u.Test != nil {
		return u.Test, nil
	}

	return nil, errors.New("no value to encode")
}

func (u *UnionRecord) FromAny(payload any) error {
	switch t := payload.(type) {
	case int:
		u.Int = &t
	case TestRecord:
		u.Test = &t
	default:
		return errors.New("unknown type during decode of union")
	}

	return nil
}

type TestPartialRecord struct {
	B string `avro:"b"`
}

type TestNestedRecord struct {
	A TestRecord `avro:"a"`
	B TestRecord `avro:"b"`
}

type TestUnion struct {
	A any `avro:"a"`
}

type TestEmbeddedRecord struct {
	C string `avro:"c"`

	TestEmbed // tests not-first position
}

type TestEmbeddedPtrRecord struct {
	C string `avro:"c"`

	*TestEmbed // tests not-first position
}

type TestEmbed struct {
	A int64  `avro:"a"`
	B string `avro:"b"`
}

type TestEmbedInt int

type TestEmbeddedIntRecord struct {
	B string `avro:"b"`

	TestEmbedInt // tests not-first position
}

type TestUnexportedRecord struct {
	A int64  `avro:"a"`
	b string `avro:"b"`
}

func TestTypeResolverRegisterConcurrent(t *testing.T) {
	const registrations = 1000

	resolver := avro.NewTypeResolver()
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(registrations)

	for i := range registrations {
		go func() {
			defer wg.Done()
			<-start
			resolver.Register(fmt.Sprintf("test.%d", i), TestRecord{})
		}()
	}

	close(start)
	wg.Wait()

	names, err := resolver.Name(reflect2.TypeOf(TestRecord{}))
	if err != nil {
		t.Fatalf("resolve registered type: %v", err)
	}
	if len(names) != registrations {
		t.Fatalf("got %d names, want %d", len(names), registrations)
	}

	seen := make(map[string]struct{}, registrations)
	for _, name := range names {
		if _, ok := seen[name]; ok {
			t.Fatalf("duplicate name %q", name)
		}
		seen[name] = struct{}{}
	}
	for i := range registrations {
		name := fmt.Sprintf("test.%d", i)
		if _, ok := seen[name]; !ok {
			t.Errorf("missing name %q", name)
		}
	}
}

func TestTypeResolverNameReturnsClone(t *testing.T) {
	resolver := avro.NewTypeResolver()
	resolver.Register("test", TestRecord{})
	typ := reflect2.TypeOf(TestRecord{})

	names, err := resolver.Name(typ)
	if err != nil {
		t.Fatalf("resolve registered type: %v", err)
	}
	names[0] = "mutated"

	names, err = resolver.Name(typ)
	if err != nil {
		t.Fatalf("resolve registered type after caller mutation: %v", err)
	}
	if names[0] != "test" {
		t.Fatalf("caller mutation changed stored name to %q", names[0])
	}
}
