package services

import (
	"errors"
	"fmt"

	"github.com/ViktorsBaikers/teldrive/internal/crypt"
	"github.com/ViktorsBaikers/teldrive/pkg/models"
)

var (
	ErrInvalidUploadPart         = errors.New("invalid upload part")
	ErrAmbiguousUploadPartSize   = errors.New("ambiguous upload part size")
	ErrUploadEncryptionMismatch  = errors.New("upload encryption mismatch")
	ErrUploadedPartsSizeMismatch = errors.New("uploaded parts size mismatch")
)

func validateUploadBackedFile(uploads []models.Upload, declaredSize int64, declaredEncrypted bool) (int64, error) {
	var (
		totalSize                 int64
		hasAmbiguousEncryptedPart bool
	)

	for _, upload := range uploads {
		if upload.PartId == 0 {
			return 0, fmt.Errorf("%w: part %d has zero part_id", ErrInvalidUploadPart, upload.PartNo)
		}
		if upload.Encrypted != declaredEncrypted {
			return 0, fmt.Errorf(
				"%w: part %d upload_encrypted=%t file_encrypted=%t",
				ErrUploadEncryptionMismatch,
				upload.PartNo,
				upload.Encrypted,
				declaredEncrypted,
			)
		}

		partSize := upload.Size
		if declaredEncrypted {
			if crypt.HasStoredSaltPrefix(upload.Salt) {
				decryptedSize, err := crypt.DecryptedSize(upload.Size)
				if err != nil {
					return 0, fmt.Errorf("%w: part %d encrypted_size=%d", ErrInvalidUploadPart, upload.PartNo, upload.Size)
				}
				partSize = decryptedSize
			} else if decryptedSize, err := crypt.DecryptedSize(upload.Size); err == nil {
				hasAmbiguousEncryptedPart = true
				partSize = decryptedSize
			}
		}
		totalSize += partSize
	}

	if !declaredEncrypted {
		if declaredSize != 0 && totalSize != declaredSize {
			return 0, fmt.Errorf("%w: declared=%d uploaded=%d", ErrUploadedPartsSizeMismatch, declaredSize, totalSize)
		}
		return totalSize, nil
	}

	if hasAmbiguousEncryptedPart {
		return 0, ErrAmbiguousUploadPartSize
	}

	if declaredSize != 0 && totalSize != declaredSize {
		return 0, fmt.Errorf("%w: declared=%d uploaded=%d", ErrUploadedPartsSizeMismatch, declaredSize, totalSize)
	}

	return totalSize, nil
}

func inferUploadBackedEncryption(uploads []models.Upload, declaredEncrypted bool, encryptedSet bool) bool {
	if encryptedSet || len(uploads) == 0 {
		return declaredEncrypted
	}
	return uploads[0].Encrypted
}
