package mtglib

import "testing"

func TestDedupeSecretsKeepsFirstIDDeterministically(t *testing.T) {
	var k1, k2 [SecretKeyLength]byte
	k1[0] = 1
	k2[0] = 2

	in := map[string]Secret{
		"zed":   {Key: k1, Host: "a"},
		"alpha": {Key: k1, Host: "a"},
		"mid":   {Key: k2, Host: "a"},
	}

	unique, dropped := dedupeSecrets(in)

	if len(unique) != 2 {
		t.Fatalf("unique=%d want 2", len(unique))
	}
	if _, ok := unique["alpha"]; !ok {
		t.Fatal("alpha (first by sorted id) must be kept")
	}
	if _, ok := unique["mid"]; !ok {
		t.Fatal("mid must be kept")
	}
	if len(dropped) != 1 || dropped[0] != "zed" {
		t.Fatalf("dropped=%v want [zed]", dropped)
	}
}
