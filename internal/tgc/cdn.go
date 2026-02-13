package tgc

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/go-faster/errors"
	"github.com/gotd/td/session"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"

	"github.com/ViktorsBaikers/teldrive/internal/config"
)

// CDNRedirect is returned by GetChunk when Telegram redirects to a CDN DC.
type CDNRedirect struct {
	Info *tg.UploadFileCDNRedirect
}

func (e *CDNRedirect) Error() string {
	if e.Info == nil {
		return "CDN redirect"
	}
	return fmt.Sprintf("CDN redirect to DC %d", e.Info.DCID)
}

// CDNFetcher fetches and decrypts file chunks from a Telegram CDN DC.
// It manages the CDN DC connection lifecycle and handles AES-256-CTR decryption.
type CDNFetcher struct {
	cdnClient  *tg.Client
	stopCDN    func() error
	mainClient *tg.Client
	redirect   *tg.UploadFileCDNRedirect
	mu         sync.Mutex
	closed     bool
}

// NewCDNFetcher creates a CDN fetcher by connecting to the CDN DC specified in the redirect.
// The connection uses noUpdatesMode (no Self() call) since CDN DCs only accept
// upload.getCdnFile and upload.getCdnFileHashes.
func NewCDNFetcher(ctx context.Context, mainClient *tg.Client, redirect *tg.UploadFileCDNRedirect, tgConfig *config.TGConfig) (*CDNFetcher, error) {
	// Get DC list to find CDN DC address
	cfg, err := mainClient.HelpGetConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("cdn: get config: %w", err)
	}

	// Find CDN DC address (prefer IPv4, non-media-only)
	var cdnAddr string
	for _, dc := range cfg.DCOptions {
		if dc.ID == redirect.DCID && !dc.Ipv6 {
			cdnAddr = fmt.Sprintf("%s:%d", dc.IPAddress, dc.Port)
			break
		}
	}
	if cdnAddr == "" {
		return nil, fmt.Errorf("cdn: no address found for DC %d", redirect.DCID)
	}

	// Create session pointing to CDN DC
	storage := new(session.StorageMemory)
	loader := session.Loader{Storage: storage}
	if err := loader.Save(ctx, &session.Data{
		DC:   redirect.DCID,
		Addr: cdnAddr,
	}); err != nil {
		return nil, fmt.Errorf("cdn: save session: %w", err)
	}

	// Create CDN client — no UpdateHandler means noUpdatesMode, which skips Self()
	cdnTelegramClient := telegram.NewClient(tgConfig.AppId, tgConfig.AppHash, telegram.Options{
		SessionStorage: storage,
	})

	ready, stop, err := cdnTelegramClient.RunBackground(ctx)
	if err != nil {
		return nil, fmt.Errorf("cdn: connect to DC %d: %w", redirect.DCID, err)
	}

	select {
	case <-ready:
	case <-ctx.Done():
		stop()
		return nil, ctx.Err()
	}

	return &CDNFetcher{
		cdnClient:  cdnTelegramClient.API(),
		stopCDN:    stop,
		mainClient: mainClient,
		redirect:   redirect,
	}, nil
}

// Chunk fetches a chunk from the CDN DC and decrypts it.
// Handles UploadCDNFileReuploadNeeded by requesting the main DC to re-upload,
// then retrying the CDN fetch.
func (f *CDNFetcher) Chunk(ctx context.Context, offset int64, limit int64) ([]byte, error) {
	data, err := f.fetchCDN(ctx, offset, int(limit))
	if err != nil {
		var reuploadErr *reuploadNeededError
		if errors.As(err, &reuploadErr) {
			// Token expired — ask main DC to re-upload, then retry
			if _, err := f.mainClient.UploadReuploadCDNFile(ctx, &tg.UploadReuploadCDNFileRequest{
				FileToken:    f.redirect.FileToken,
				RequestToken: reuploadErr.RequestToken,
			}); err != nil {
				return nil, fmt.Errorf("cdn: reupload request: %w", err)
			}
			return f.fetchCDN(ctx, offset, int(limit))
		}
		return nil, err
	}
	return data, nil
}

// Close shuts down the CDN DC connection.
func (f *CDNFetcher) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return nil
	}
	f.closed = true
	if f.stopCDN != nil {
		return f.stopCDN()
	}
	return nil
}

type reuploadNeededError struct {
	RequestToken []byte
}

func (e *reuploadNeededError) Error() string {
	return "CDN file reupload needed"
}

func (f *CDNFetcher) fetchCDN(ctx context.Context, offset int64, limit int) ([]byte, error) {
	r, err := f.cdnClient.UploadGetCDNFile(ctx, &tg.UploadGetCDNFileRequest{
		FileToken: f.redirect.FileToken,
		Offset:    offset,
		Limit:     limit,
	})
	if err != nil {
		return nil, fmt.Errorf("cdn: get file: %w", err)
	}

	switch result := r.(type) {
	case *tg.UploadCDNFile:
		return cdnDecrypt(result.Bytes, f.redirect.EncryptionKey, f.redirect.EncryptionIv, offset)
	case *tg.UploadCDNFileReuploadNeeded:
		return nil, &reuploadNeededError{RequestToken: result.RequestToken}
	default:
		return nil, fmt.Errorf("cdn: unexpected type %T", r)
	}
}

// cdnDecrypt decrypts a CDN file chunk using AES-256-CTR.
// Per Telegram spec, the IV is modified: last 4 bytes are replaced with offset/16 in big-endian.
// See https://core.telegram.org/cdn#decrypting-files.
func cdnDecrypt(data, key, iv []byte, offset int64) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("cdn decrypt: create cipher: %w", err)
	}

	if block.BlockSize() != len(iv) {
		return nil, fmt.Errorf("cdn decrypt: IV length %d != block size %d", len(iv), block.BlockSize())
	}

	// Copy IV and modify last 4 bytes with offset/16 in big-endian
	modifiedIV := make([]byte, len(iv))
	copy(modifiedIV, iv)
	binary.BigEndian.PutUint32(modifiedIV[len(modifiedIV)-4:], uint32(offset/16))

	dst := make([]byte, len(data))
	cipher.NewCTR(block, modifiedIV).XORKeyStream(dst, data)
	return dst, nil
}
