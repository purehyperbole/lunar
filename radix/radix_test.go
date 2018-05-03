package radix

import (
	"os"
	"testing"

	"github.com/purehyperbole/lunar/table"
	"github.com/stretchr/testify/assert"
)

type testvalue struct {
	Key    string
	Value  string
	Prefix string
}

func TestRadixInsert(t *testing.T) {
	cases := []struct {
		Name          string
		ExpectedNodes int
		Existing      []testvalue
		Creates       []testvalue
		Updates       []testvalue
	}{
		{"single-insert", 2, nil, []testvalue{{"test", "1234", "est"}}, []testvalue{{"test", "4567", "est"}}},
	}

	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			fl := table.NewFreeList(1 << 30)

			tbl, err := table.New("test-insert.index")
			assert.Nil(t, err)
			assert.NotNil(t, tbl)

			// clean file
			defer func() {
				tbl.Close()
				os.Remove("test-insert.index")
			}()

			r := New(tbl)
			assert.NotNil(t, r)

			for _, kv := range tc.Existing {
				sz := int64(len(kv.Value))
				off, _ := fl.Reserve(sz)

				_, err := r.Insert([]byte(kv.Key), sz, off)
				assert.Nil(t, err)
			}

			for _, kv := range tc.Creates {
				sz := int64(len(kv.Value))
				off, _ := fl.Reserve(sz)

				_, err := r.Insert([]byte(kv.Key), sz, off)
				assert.Nil(t, err)

				n, err := r.Lookup([]byte(kv.Key))
				assert.Nil(t, err)
				assert.NotNil(t, n)
				assert.Equal(t, off, n.Offset())
				assert.Equal(t, sz, n.Size())
			}

			for _, kv := range tc.Updates {
				sz := int64(len(kv.Value))
				off, _ := fl.Reserve(sz)

				_, err := r.Insert([]byte(kv.Key), sz, off)
				assert.Nil(t, err)

				n, err := r.Lookup([]byte(kv.Key))
				assert.Nil(t, err)
				assert.NotNil(t, n)
				assert.Equal(t, off, n.Offset())
				assert.Equal(t, sz, n.Size())
			}
		})
	}

}

func TestRadixLookup(t *testing.T) {
	cases := []struct {
		Name          string
		ExpectedNodes int
		Existing      []testvalue
		Lookups       []testvalue
	}{
		{"simple", 2, []testvalue{{"test", "1234", "est"}}, []testvalue{{"test", "1234", "est"}}},
		{"derivative", 3, []testvalue{{"test", "1234", "est"}, {"test1234", "bacon", "est"}}, []testvalue{{"test1234", "bacon", "234"}}},
		{"split", 3, []testvalue{{"test1234", "bacon", "234"}, {"test", "1234", "est"}}, []testvalue{{"test1234", "bacon", "234"}}},
		{"split-single-shared-character", 5, []testvalue{{"test", "1234", "est"}, {"test1234", "bacon", "est"}, {"test1000", "egg", "est"}}, []testvalue{{"test", "1234", "est"}, {"test1234", "bacon", "34"}, {"test1000", "egg", "00"}}},
		{"complex", 13, []testvalue{{"test", "1234", "st"}, {"test1234", "bacon", "234"}, {"tomato", "egg", "ato"}, {"tamale", "hash browns", "male"}, {"todo", "beans", ""}, {"todos", "mushrooms", "s"}, {"abalienate", "toast", ""}, {"abalienated", "onions", ""}, {"abalienating", "sausage", "ng"}}, []testvalue{{"test", "1234", "st"}, {"test1234", "bacon", "234"}, {"tomato", "egg", "ato"}, {"tamale", "hash browns", "male"}, {"todo", "beans", "o"}, {"todos", "mushrooms", ""}, {"abalienate", "toast", ""}, {"abalienated", "onions", ""}, {"abalienating", "sausage", "ng"}}},
		{"single-character", 3, []testvalue{{"todo", "toast", "odo"}, {"todos", "bacon", ""}}, []testvalue{{"todo", "toast", "odo"}, {"todos", "bacon", ""}}},
	}

	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			fl := table.NewFreeList(1 << 30)

			tbl, err := table.New("test-lookup.index")
			assert.Nil(t, err)
			assert.NotNil(t, tbl)

			// clean file
			defer func() {
				tbl.Close()
				os.Remove("test-lookup.index")
			}()

			r := New(tbl)
			assert.NotNil(t, r)

			for _, kv := range tc.Existing {
				sz := int64(len(kv.Value))
				off, _ := fl.Reserve(sz)

				_, err := r.Insert([]byte(kv.Key), sz, off)
				assert.Nil(t, err)
			}

			assert.Equal(t, tc.ExpectedNodes, r.nodes)

			for _, kv := range tc.Lookups {
				n, err := r.Lookup([]byte(kv.Key))
				assert.Nil(t, err)
				assert.NotNil(t, n)
				assert.Equal(t, kv.Prefix, string(n.Prefix()))
			}
		})
	}

}
