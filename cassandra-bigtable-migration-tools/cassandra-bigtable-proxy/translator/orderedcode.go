/*
 * Copyright (C) 2025 Google LLC
 *
 * Licensed under the Apache License, ProtocolVersion 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

/*
This file is a modified copy of https://github.com/google/orderedcode.

We've modified it to be compatible with Bigtable Struct OrderedCode encoding
which requires all encoded bytes (not just strings) to have their null bytes,
`\x00`, escaped with `\xff`. We also removed support for floats because we only
need string and int encoding at this time.
*/

package translator

import (
	"fmt"
)

func invert(b []byte) {
	for i := range b {
		b[i] ^= 0xff
	}
}

// Append appends the encoded representations of items to buf. Items can have
// different underlying types, but each item must have type T or be the value
// Decr(somethingOfTypeT), for T in the set: string or int64.
func Append(buf []byte, items ...interface{}) ([]byte, error) {
	for _, item := range items {
		switch x := item.(type) {
		case string:
			buf = appendString(buf, x)
		case int64:
			buf = appendInt64(buf, x)
		default:
			return nil, fmt.Errorf("orderedcode: cannot append an item of type %T", item)
		}
	}
	return buf, nil
}

// The wire format for strings is:
//   - \x00\x01 terminates the string.
//   - \x00 bytes are escaped as \x00\xff.
//   - All other bytes are literals.
const (
	term  = "\x00\x01"
	lit00 = "\x00\xff"
)

func appendString(s []byte, x string) []byte {
	s = escapeNullByte(s, x)
	s = append(s, term...)
	return s
}

func escapeNullByte(s []byte, x string) []byte {
	last := 0
	for i := 0; i < len(x); i++ {
		switch x[i] {
		case 0x00:
			s = append(s, x[last:i]...)
			s = append(s, lit00...)
			last = i + 1
		}
	}
	s = append(s, x[last:]...)
	return s
}

// The wire format for an int64 value x is, for non-negative x, n leading 1
// bits, followed by a 0 bit, followed by n-1 bytes. That entire slice, after
// masking off the leading 1 bits, is the big-endian representation of x.
// n is the smallest positive integer that can represent x this way.
//
// The encoding of a negative x is the inversion of the encoding for ^x.
// Thus, the encoded form's leading bit is a sign bit: it is 0 for negative x
// and 1 for non-negative x.
//
// For example:
//   - 0x23   encodes as 10 100011
//     n=0, the remainder after masking is 0x23.
//   - 0x10e  encodes as 110 00001  00001110
//     n=1, the remainder after masking is 0x10e.
//   - -0x10f encodes as 001 11110  11110001
//     This is the inverse of the encoding of 0x10e.
// There are many more examples in orderedcode_test.go.

func encodeInt64(x int64) []byte {
	// Fast-path those values of x that encode to a single byte.
	if x >= -64 && x < 64 {
		return []byte{uint8(x) ^ 0x80}
	}
	// If x is negative, invert it, and correct for this at the end.
	neg := x < 0
	if neg {
		x = ^x
	}
	// x is now non-negative, and so its encoding starts with a 1: the sign bit.
	n := 1
	// buf is 8 bytes for x's big-endian representation plus 2 bytes for leading 1 bits.
	var buf [10]byte
	// Fill buf from back-to-front.
	i := 9
	for x > 0 {
		buf[i] = byte(x)
		n, i, x = n+1, i-1, x>>8
	}
	// Check if we need a full byte of leading 1 bits. 7 is 8 - 1; the 8 is the
	// number of bits in a byte, and the 1 is because lengthening the encoding
	// by one byte requires incrementing n.
	leadingFFByte := n > 7
	if leadingFFByte {
		n -= 7
	}
	// If we can squash the leading 1 bits together with x's most significant byte,
	// then we can save one byte.
	//
	// We need to adjust 8-n by -1 for the separating 0 bit, but also by
	// +1 because we are trying to get away with one fewer leading 1 bits.
	// The two adjustments cancel each other out.
	if buf[i+1] < 1<<uint(8-n) {
		n--
		i++
	}
	// Or in the leading 1 bits, invert if necessary, and return.
	buf[i] |= msb[n]
	if leadingFFByte {
		i--
		buf[i] = 0xff
	}
	if neg {
		invert(buf[i:])
	}
	return buf[i:]
}

func appendInt64(buff []byte, x int64) []byte {
	// normally encoded int64 bytes would *not* be escaped, but they need to be escaped for Bigtable Struct OrderedCode encoding
	return escapeNullByte(buff, string(encodeInt64(x)))
}

// msb[i] is a byte whose first i bits (in most significant bit order) are 1
// and all other bits are 0.
var msb = [8]byte{
	0x00, 0x80, 0xc0, 0xe0, 0xf0, 0xf8, 0xfc, 0xfe,
}
