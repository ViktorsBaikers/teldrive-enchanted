package services

import (
	"errors"
	"strings"
	"testing"

	"github.com/gotd/td/tg"
)

func TestValidateUploadedDocumentSizeAcceptsExactMatch(t *testing.T) {
	if err := validateUploadedDocumentSize(&tg.Document{Size: 10}, 10); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidateUploadedDocumentSizeAcceptsZeroByteMatch(t *testing.T) {
	if err := validateUploadedDocumentSize(&tg.Document{Size: 0}, 0); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidateUploadedDocumentSizeRejectsMismatch(t *testing.T) {
	err := validateUploadedDocumentSize(&tg.Document{Size: 11}, 10)
	if !errors.Is(err, ErrUploadFailed) {
		t.Fatalf("expected ErrUploadFailed, got %v", err)
	}
	if !strings.Contains(err.Error(), "expected=10 actual=11") {
		t.Fatalf("expected mismatch context in error, got %v", err)
	}
}

func TestValidateUploadedDocumentSizeRejectsUnexpectedZeroSize(t *testing.T) {
	err := validateUploadedDocumentSize(&tg.Document{Size: 0}, 10)
	if !errors.Is(err, ErrUploadFailed) {
		t.Fatalf("expected ErrUploadFailed, got %v", err)
	}
	if !strings.Contains(err.Error(), "expected=10 actual=0") {
		t.Fatalf("expected mismatch context in error, got %v", err)
	}
}

func TestValidateUploadedDocumentSizeRejectsMissingDocument(t *testing.T) {
	err := validateUploadedDocumentSize(nil, 10)
	if !errors.Is(err, ErrUploadFailed) {
		t.Fatalf("expected ErrUploadFailed, got %v", err)
	}
	if !strings.Contains(err.Error(), "missing uploaded document metadata") {
		t.Fatalf("expected missing-document context in error, got %v", err)
	}
}
