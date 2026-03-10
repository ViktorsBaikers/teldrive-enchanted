package services

import (
	"errors"
	"testing"

	"github.com/ViktorsBaikers/teldrive/internal/crypt"
	"github.com/ViktorsBaikers/teldrive/pkg/models"
)

func TestValidateUploadBackedFileReturnsLogicalSize(t *testing.T) {
	encryptedSize1 := crypt.EncryptedSize(5)
	encryptedSize2 := crypt.EncryptedSize(7)
	uploads := []models.Upload{
		{PartNo: 1, PartId: 11, Size: encryptedSize1, Encrypted: true, Salt: crypt.StoredSalt("salt-a")},
		{PartNo: 2, PartId: 12, Size: encryptedSize2, Encrypted: true, Salt: crypt.StoredSalt("salt-b")},
	}

	size, err := validateUploadBackedFile(uploads, 12, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if size != 12 {
		t.Fatalf("expected logical size 12, got %d", size)
	}
}

func TestValidateUploadBackedFileRejectsEncryptionMismatch(t *testing.T) {
	uploads := []models.Upload{
		{PartNo: 1, PartId: 11, Size: crypt.EncryptedSize(5), Encrypted: true, Salt: crypt.StoredSalt("salt-a")},
	}

	_, err := validateUploadBackedFile(uploads, 5, false)
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, ErrUploadEncryptionMismatch) {
		t.Fatalf("expected ErrUploadEncryptionMismatch, got %v", err)
	}
}

func TestValidateUploadBackedFileRejectsZeroPartID(t *testing.T) {
	_, err := validateUploadBackedFile([]models.Upload{{PartNo: 1, PartId: 0, Size: 5}}, 5, false)
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, ErrInvalidUploadPart) {
		t.Fatalf("expected ErrInvalidUploadPart, got %v", err)
	}
}

func TestValidateUploadBackedFileRejectsSizeMismatch(t *testing.T) {
	_, err := validateUploadBackedFile([]models.Upload{{PartNo: 1, PartId: 11, Size: 5}}, 6, false)
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, ErrUploadedPartsSizeMismatch) {
		t.Fatalf("expected ErrUploadedPartsSizeMismatch, got %v", err)
	}
}

func TestValidateUploadBackedFileReturnsAmbiguousForLegacyEncryptedPlaintextSizes(t *testing.T) {
	uploads := []models.Upload{
		{PartNo: 1, PartId: 11, Size: 100, Encrypted: true},
		{PartNo: 2, PartId: 12, Size: 7, Encrypted: true},
	}

	_, err := validateUploadBackedFile(uploads, 107, true)
	if !errors.Is(err, ErrAmbiguousUploadPartSize) {
		t.Fatalf("expected ErrAmbiguousUploadPartSize, got %v", err)
	}
}

func TestValidateUploadBackedFileFallsBackToLegacyPlaintextWhenEncryptedSizeInvalid(t *testing.T) {
	uploads := []models.Upload{
		{PartNo: 1, PartId: 11, Size: 5, Encrypted: true},
	}

	size, err := validateUploadBackedFile(uploads, 0, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if size != 5 {
		t.Fatalf("expected logical size 5, got %d", size)
	}
}

func TestValidateUploadBackedFileUsesDecryptedSizeWhenEncryptedSizeOmittedForCurrentUploads(t *testing.T) {
	uploads := []models.Upload{
		{PartNo: 1, PartId: 11, Size: crypt.EncryptedSize(5), Encrypted: true, Salt: crypt.StoredSalt("salt-a")},
		{PartNo: 2, PartId: 12, Size: crypt.EncryptedSize(7), Encrypted: true, Salt: crypt.StoredSalt("salt-b")},
	}

	size, err := validateUploadBackedFile(uploads, 0, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if size != 12 {
		t.Fatalf("expected logical size 12, got %d", size)
	}
}

func TestValidateUploadBackedFileKeepsCurrentEncryptedZeroByteSizeWhenOmitted(t *testing.T) {
	uploads := []models.Upload{
		{PartNo: 1, PartId: 11, Size: crypt.EncryptedSize(0), Encrypted: true, Salt: crypt.StoredSalt("salt-a")},
	}

	size, err := validateUploadBackedFile(uploads, 0, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if size != 0 {
		t.Fatalf("expected logical size 0, got %d", size)
	}
}

func TestValidateUploadBackedFileFallsBackToLegacyPlaintextWhenEncryptedSizeLooksValid(t *testing.T) {
	uploads := []models.Upload{
		{PartNo: 1, PartId: 11, Size: 100, Encrypted: true, Salt: "salt-a"},
		{PartNo: 2, PartId: 12, Size: 80, Encrypted: true, Salt: "salt-b"},
	}

	_, err := validateUploadBackedFile(uploads, 0, true)
	if !errors.Is(err, ErrAmbiguousUploadPartSize) {
		t.Fatalf("expected ErrAmbiguousUploadPartSize, got %v", err)
	}
}

func TestValidateUploadBackedFileUsesStoredSaltForCurrentEncryptedUploadsWithoutHashes(t *testing.T) {
	uploads := []models.Upload{
		{PartNo: 1, PartId: 11, Size: crypt.EncryptedSize(5), Encrypted: true, Salt: crypt.StoredSalt("salt-a")},
		{PartNo: 2, PartId: 12, Size: crypt.EncryptedSize(7), Encrypted: true, Salt: crypt.StoredSalt("salt-b")},
	}

	size, err := validateUploadBackedFile(uploads, 0, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if size != 12 {
		t.Fatalf("expected logical size 12, got %d", size)
	}
}

func TestValidateUploadBackedFileRejectsCiphertextDeclaredSizeForCurrentEncryptedUploads(t *testing.T) {
	uploads := []models.Upload{
		{PartNo: 1, PartId: 11, Size: crypt.EncryptedSize(5), Encrypted: true, Salt: crypt.StoredSalt("salt-a")},
		{PartNo: 2, PartId: 12, Size: crypt.EncryptedSize(7), Encrypted: true, Salt: crypt.StoredSalt("salt-b")},
	}

	_, err := validateUploadBackedFile(uploads, crypt.EncryptedSize(5)+crypt.EncryptedSize(7), true)
	if !errors.Is(err, ErrUploadedPartsSizeMismatch) {
		t.Fatalf("expected ErrUploadedPartsSizeMismatch, got %v", err)
	}
}

func TestValidateUploadBackedFileReturnsAmbiguousForMixedLegacyAndCurrentEncryptedParts(t *testing.T) {
	uploads := []models.Upload{
		{PartNo: 1, PartId: 11, Size: 100, Encrypted: true, Salt: "legacy-raw-salt"},
		{PartNo: 2, PartId: 12, Size: crypt.EncryptedSize(7), Encrypted: true, Salt: crypt.StoredSalt("current-salt")},
	}

	_, err := validateUploadBackedFile(uploads, 0, true)
	if !errors.Is(err, ErrAmbiguousUploadPartSize) {
		t.Fatalf("expected ErrAmbiguousUploadPartSize, got %v", err)
	}
}

func TestInferUploadBackedEncryptionUsesUploadsWhenFieldOmitted(t *testing.T) {
	uploads := []models.Upload{{PartNo: 1, PartId: 11, Encrypted: true}}

	if got := inferUploadBackedEncryption(uploads, false, false); !got {
		t.Fatal("expected omitted field to infer encrypted=true from uploads")
	}
}

func TestInferUploadBackedEncryptionPreservesExplicitField(t *testing.T) {
	uploads := []models.Upload{{PartNo: 1, PartId: 11, Encrypted: true}}

	if got := inferUploadBackedEncryption(uploads, false, true); got {
		t.Fatal("expected explicit encrypted=false to be preserved")
	}
}
