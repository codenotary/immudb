package immuc_test

import (
	"strings"
	"testing"
)

func TestServerInfo(t *testing.T) {
	ic := setupTest(t)

	if msg, err := ic.Imc.ServerInfo(nil); err != nil {
		t.Fatal("ServerInfo fail", err)
	} else if !strings.Contains(msg, "version") {
		t.Fatalf("ServerInfo failed: %s", msg)
	}
}
