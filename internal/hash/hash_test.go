package hash

import (
	"bytes"
	"encoding/hex"
	"math/rand"
	"testing"
	"time"

	"github.com/zeebo/blake3"
)

func serialBlockHashes(data []byte) []byte {
	if len(data) == 0 {
		return nil
	}
	out := make([]byte, 0, ((len(data)+BlockSize-1)/BlockSize)*blake3DigestSize)
	for start := 0; start < len(data); start += BlockSize {
		end := start + BlockSize
		if end > len(data) {
			end = len(data)
		}
		sum := blake3.Sum256(data[start:end])
		out = append(out, sum[:]...)
	}
	return out
}

func TestBlockHasherMatchesSerial(t *testing.T) {
	sizes := []int{
		0,
		1,
		17,
		1024,
		BlockSize - 1,
		BlockSize,
		BlockSize + 123,
		2*BlockSize + 7,
		3*BlockSize + 4096,
	}

	for _, size := range sizes {
		t.Run(hex.EncodeToString([]byte{byte(size % 251)}), func(t *testing.T) {
			data := bytes.Repeat([]byte{0xAB}, size)
			h := NewBlockHasher()

			if size > 0 {
				rng := rand.New(rand.NewSource(int64(size)))
				offset := 0
				for offset < size {
					chunk := rng.Intn(1<<20) + 1
					if offset+chunk > size {
						chunk = size - offset
					}
					_, err := h.Write(data[offset : offset+chunk])
					if err != nil {
						t.Fatalf("write failed: %v", err)
					}
					offset += chunk
				}
			}

			got := h.Sum()
			want := serialBlockHashes(data)
			if !bytes.Equal(got, want) {
				t.Fatalf("block hash mismatch for size=%d", size)
			}

			treeGot := SumToHex(ComputeTreeHash(got))
			treeWant := SumToHex(ComputeTreeHash(want))
			if treeGot != treeWant {
				t.Fatalf("tree hash mismatch for size=%d", size)
			}
		})
	}
}

func TestBlockHasherReset(t *testing.T) {
	h := NewBlockHasher()
	_, _ = h.Write([]byte("first"))
	first := h.Sum()
	if len(first) == 0 {
		t.Fatalf("expected non-empty block hash for first input")
	}

	h.Reset()
	_, _ = h.Write([]byte("second"))
	second := h.Sum()
	if len(second) == 0 {
		t.Fatalf("expected non-empty block hash for second input")
	}
	if bytes.Equal(first, second) {
		t.Fatalf("reset did not clear previous state")
	}
}

func TestBlockHasherCloseWithoutSum(t *testing.T) {
	h := NewBlockHasher()
	_, _ = h.Write(bytes.Repeat([]byte{0x01}, BlockSize+123))

	done := make(chan struct{})
	go func() {
		h.Close()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatalf("Close timed out")
	}
}
