package knowledge

import (
	"encoding/binary"
	"math"
	"strings"
	"unicode"

	"golang.org/x/crypto/blake2b"
	"tree-eclass/internal/domain/identity"
)

const LocalEmbeddingModel = "hash-384-v1"
const EmbeddingDimensions = 384

func Embed(text string) []float64 {
	vector := make([]float64, EmbeddingDimensions)
	tokens := strings.FieldsFunc(
		identity.Search(text),
		func(r rune) bool { return !(unicode.IsLetter(r) || unicode.IsNumber(r) || r == '_') },
	)
	add := func(value string, weight float64) {
		hash, _ := blake2b.New(8, nil)
		_, _ = hash.Write([]byte(value))
		digest := hash.Sum(nil)
		bucket := binary.BigEndian.Uint32(digest[:4]) % EmbeddingDimensions
		if digest[4]&1 == 0 {
			weight = -weight
		}
		vector[bucket] += weight
	}
	for _, token := range tokens {
		add("w:"+token, 1)
		padded := []rune("^" + token + "$")
		for i := 0; i+2 < len(padded); i++ {
			add("c:"+string(padded[i:i+3]), 0.35)
		}
	}
	norm := 0.0
	for _, value := range vector {
		norm += value * value
	}
	if norm > 0 {
		norm = math.Sqrt(norm)
		for i := range vector {
			vector[i] /= norm
		}
	}
	return vector
}
func Pack(vector []float64) []byte {
	data := make([]byte, len(vector)*4)
	for i, value := range vector {
		binary.LittleEndian.PutUint32(data[i*4:], math.Float32bits(float32(value)))
	}
	return data
}
func CosinePacked(query []float64, packed []byte) float64 {
	if len(packed) != len(query)*4 {
		return 0
	}
	value := 0.0
	for i, q := range query {
		value += q * float64(math.Float32frombits(binary.LittleEndian.Uint32(packed[i*4:])))
	}
	return value
}
