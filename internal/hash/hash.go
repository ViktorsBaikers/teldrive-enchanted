package hash

import (
	"encoding/hex"
	"runtime"
	"sync"

	"github.com/zeebo/blake3"
)

// BlockSize is the fixed block size for tree hashing (16MB)
const BlockSize = 16 * 1024 * 1024

// Type represents the hash algorithm type
type Type string

const (
	// TypeBlake3 is the only supported hash algorithm (fastest)
	TypeBlake3 Type = "blake3"
)

var blake3DigestSize = blake3.New().Size()

type blockJob struct {
	index int
	data  []byte
}

type blockResult struct {
	index int
	sum   []byte
}

// BlockHasher processes data in fixed-size blocks and accumulates block hashes
type BlockHasher struct {
	blockSize int64

	jobs    chan blockJob
	results chan blockResult
	wg      sync.WaitGroup

	currentBlock []byte
	blockHashes  [][]byte
	outOfOrder   map[int][]byte

	nextDispatchIndex int
	nextCommitIndex   int
	pending           int
	closed            bool
}

// NewBlockHasher creates a new BlockHasher (always BLAKE3)
func NewBlockHasher() *BlockHasher {
	h := &BlockHasher{
		blockSize:  BlockSize,
		outOfOrder: make(map[int][]byte),
	}
	h.initWorkers()
	return h
}

func (h *BlockHasher) initWorkers() {
	workerCount := runtime.GOMAXPROCS(0)
	if workerCount < 1 {
		workerCount = 1
	}
	queueSize := workerCount * 2
	if queueSize < 2 {
		queueSize = 2
	}
	h.jobs = make(chan blockJob, queueSize)
	h.results = make(chan blockResult, queueSize)

	h.wg.Add(workerCount)
	for i := 0; i < workerCount; i++ {
		go func() {
			defer h.wg.Done()
			for job := range h.jobs {
				sum := blake3.Sum256(job.data)
				h.results <- blockResult{
					index: job.index,
					sum:   sum[:],
				}
			}
		}()
	}

	go func() {
		h.wg.Wait()
		close(h.results)
	}()
}

func (h *BlockHasher) dispatchBlock(block []byte) {
	blockCopy := append([]byte(nil), block...)
	h.jobs <- blockJob{
		index: h.nextDispatchIndex,
		data:  blockCopy,
	}
	h.nextDispatchIndex++
	h.pending++
}

func (h *BlockHasher) processResult(res blockResult) {
	h.pending--
	h.outOfOrder[res.index] = res.sum
	for {
		hashBytes, ok := h.outOfOrder[h.nextCommitIndex]
		if !ok {
			return
		}
		h.blockHashes = append(h.blockHashes, hashBytes)
		delete(h.outOfOrder, h.nextCommitIndex)
		h.nextCommitIndex++
	}
}

func (h *BlockHasher) drainResultsNonBlocking() {
	for {
		select {
		case res, ok := <-h.results:
			if !ok {
				return
			}
			h.processResult(res)
		default:
			return
		}
	}
}

func (h *BlockHasher) closeJobs() {
	if h.closed {
		return
	}
	close(h.jobs)
	h.closed = true
}

// Close releases worker resources for hasher instances that won't be finalized with Sum.
// It is safe to call multiple times.
func (h *BlockHasher) Close() {
	h.closeJobs()
	for range h.results {
	}
}

// Write implements io.Writer - processes data in BlockSize chunks
func (h *BlockHasher) Write(p []byte) (n int, err error) {
	n = len(p)
	for len(p) > 0 {
		remaining := int(h.blockSize) - len(h.currentBlock)
		if remaining > len(p) {
			remaining = len(p)
		}
		h.currentBlock = append(h.currentBlock, p[:remaining]...)
		p = p[remaining:]

		// Block is complete
		if len(h.currentBlock) == int(h.blockSize) {
			h.dispatchBlock(h.currentBlock)
			h.currentBlock = h.currentBlock[:0]
			h.drainResultsNonBlocking()
		}
	}
	return n, nil
}

// Sum returns concatenated block hashes
func (h *BlockHasher) Sum() []byte {
	// Handle partial block at end
	if len(h.currentBlock) > 0 {
		h.dispatchBlock(h.currentBlock)
		h.currentBlock = h.currentBlock[:0]
	}

	h.closeJobs()
	for h.pending > 0 {
		res, ok := <-h.results
		if !ok {
			break
		}
		h.processResult(res)
	}

	// Concatenate all block hashes
	result := make([]byte, 0, len(h.blockHashes)*blake3DigestSize)
	for _, bh := range h.blockHashes {
		result = append(result, bh...)
	}
	return result
}

// GetBlockCount returns the number of complete blocks processed
func (h *BlockHasher) GetBlockCount() int {
	return h.nextDispatchIndex
}

// Reset resets the hasher for a new stream
func (h *BlockHasher) Reset() {
	h.Close()

	h.blockHashes = nil
	h.currentBlock = nil
	h.outOfOrder = make(map[int][]byte)
	h.nextDispatchIndex = 0
	h.nextCommitIndex = 0
	h.pending = 0
	h.closed = false
	h.initWorkers()
}

// ComputeTreeHash computes the final tree hash from concatenated block hashes
func ComputeTreeHash(concatenatedBlockHashes []byte) []byte {
	h := blake3.New()
	h.Write(concatenatedBlockHashes)
	return h.Sum(nil)
}

// SumToHex converts bytes to hex string
func SumToHex(data []byte) string {
	return hex.EncodeToString(data)
}
