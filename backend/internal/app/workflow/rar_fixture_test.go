package workflow

import (
	"encoding/binary"
	"hash/crc32"
	"slices"
)

// Self-authored stored RAR5 fixture: https://www.rarlab.com/technote.htm.
// No proprietary compressor, downloaded archive or user course data is needed.
func fixtureRAR(files map[string][]byte) []byte {
	out := []byte{'R', 'a', 'r', '!', 0x1a, 7, 1, 0}
	block := func(header []byte) {
		body := binary.AppendUvarint(nil, uint64(len(header)))
		body = append(body, header...)
		out = binary.LittleEndian.AppendUint32(out, crc32.ChecksumIEEE(body))
		out = append(out, body...)
	}
	block([]byte{1, 0, 0})
	names := []string{}
	for name := range files {
		names = append(names, name)
	}
	slices.Sort(names)
	for _, name := range names {
		data := files[name]
		header := []byte{2, 2}
		header = binary.AppendUvarint(header, uint64(len(data)))
		header = append(header, 4)
		header = binary.AppendUvarint(header, uint64(len(data)))
		header = binary.AppendUvarint(header, 0100644)
		header = binary.LittleEndian.AppendUint32(header, crc32.ChecksumIEEE(data))
		header = append(header, 0, 1)
		header = binary.AppendUvarint(header, uint64(len(name)))
		header = append(header, name...)
		block(header)
		out = append(out, data...)
	}
	block([]byte{5, 0, 0})
	return out
}
