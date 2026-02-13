package tgc

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"testing"
)

func TestCDNDecrypt(t *testing.T) {
	// AES-256 key (32 bytes)
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}

	// IV (16 bytes = AES block size)
	iv := make([]byte, aes.BlockSize)
	for i := range iv {
		iv[i] = byte(0xAA)
	}

	// Create plaintext
	plaintext := []byte("hello from CDN edge node data!!!")

	// Encrypt with same algorithm cdnDecrypt uses
	offset := int64(1024)
	modifiedIV := make([]byte, aes.BlockSize)
	copy(modifiedIV, iv)
	binary.BigEndian.PutUint32(modifiedIV[12:], uint32(offset/16))

	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatal(err)
	}
	ciphertext := make([]byte, len(plaintext))
	cipher.NewCTR(block, modifiedIV).XORKeyStream(ciphertext, plaintext)

	// Decrypt
	got, err := cdnDecrypt(ciphertext, key, iv, offset)
	if err != nil {
		t.Fatalf("cdnDecrypt error: %v", err)
	}

	if string(got) != string(plaintext) {
		t.Fatalf("cdnDecrypt: got %q, want %q", got, plaintext)
	}
}

func TestCDNDecrypt_OffsetZero(t *testing.T) {
	key := make([]byte, 32)
	iv := make([]byte, aes.BlockSize)

	plaintext := []byte("test data at offset zero")

	// At offset 0, IV last 4 bytes become 0 (same as original if IV ends in 0)
	modifiedIV := make([]byte, aes.BlockSize)
	copy(modifiedIV, iv)
	binary.BigEndian.PutUint32(modifiedIV[12:], 0)

	block, _ := aes.NewCipher(key)
	ciphertext := make([]byte, len(plaintext))
	cipher.NewCTR(block, modifiedIV).XORKeyStream(ciphertext, plaintext)

	got, err := cdnDecrypt(ciphertext, key, iv, 0)
	if err != nil {
		t.Fatalf("cdnDecrypt error: %v", err)
	}

	if string(got) != string(plaintext) {
		t.Fatalf("got %q, want %q", got, plaintext)
	}
}

func TestCDNDecrypt_InvalidKeyLength(t *testing.T) {
	_, err := cdnDecrypt([]byte("data"), []byte("short"), make([]byte, 16), 0)
	if err == nil {
		t.Fatal("expected error for invalid key length")
	}
}

func TestCDNDecrypt_InvalidIVLength(t *testing.T) {
	key := make([]byte, 32)
	_, err := cdnDecrypt([]byte("data"), key, []byte("short"), 0)
	if err == nil {
		t.Fatal("expected error for IV/block size mismatch")
	}
}

func TestCDNRedirect_Error(t *testing.T) {
	err := &CDNRedirect{Info: nil}
	// Should not panic on nil Info
	_ = err.Error()
}
