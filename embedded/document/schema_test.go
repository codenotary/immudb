package document

import (
	"reflect"
	"testing"

	"github.com/codenotary/immudb/embedded/sql"
)

func TestDefaultOptions(t *testing.T) {
	tests := []struct {
		name string
		want *sql.Options
	}{
		{
			name: "default options",
			want: sql.DefaultOptions().WithPrefix(ObjectPrefix),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := DefaultOptions(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DefaultOptions() = %v, want %v", got, tt.want)
			}
		})
	}
}
