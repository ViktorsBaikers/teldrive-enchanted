package crypt

import "testing"

func TestStoredSaltNormalization(t *testing.T) {
	raw := "salt-123"
	stored := StoredSalt(raw)

	if stored == raw {
		t.Fatalf("expected stored salt to differ from raw salt, got %q", stored)
	}
	if !HasStoredSaltPrefix(stored) {
		t.Fatalf("expected stored salt prefix, got %q", stored)
	}
	if NormalizeSalt(stored) != raw {
		t.Fatalf("expected normalized salt %q, got %q", raw, NormalizeSalt(stored))
	}
	if NormalizeSalt(raw) != raw {
		t.Fatalf("expected raw salt to remain unchanged, got %q", NormalizeSalt(raw))
	}
}

func TestCipherKeyUsesNormalizedSalt(t *testing.T) {
	rawCipher, err := NewCipher("password", "salt-123")
	if err != nil {
		t.Fatalf("raw cipher: %v", err)
	}
	storedCipher, err := NewCipher("password", StoredSalt("salt-123"))
	if err != nil {
		t.Fatalf("stored cipher: %v", err)
	}

	if rawCipher.dataKey != storedCipher.dataKey {
		t.Fatal("expected identical data keys for raw and stored salts")
	}
	if rawCipher.nameKey != storedCipher.nameKey {
		t.Fatal("expected identical name keys for raw and stored salts")
	}
	if rawCipher.nameTweak != storedCipher.nameTweak {
		t.Fatal("expected identical name tweaks for raw and stored salts")
	}
}
