package avro

import "math"

// SkipNBytes skips the given number of bytes in the reader.
func (r *Reader) SkipNBytesInt64(n int64) {
	if n < 0 {
		r.ReportError("skip n bytes", "n is negative")
		return
	}
	if n > math.MaxInt {
		r.ReportError("skip n bytes", "n exceeds max int")
		return
	}
	r.SkipNBytes(int(n))
}

// SkipNBytes skips the given number of bytes in the reader.
func (r *Reader) SkipNBytes(n int) {
	read := 0
	for read < n {
		if r.head == r.tail {
			if !r.loadMore() {
				r.Error = cutShort(r.Error)
				return
			}
		}

		if read+r.tail-r.head < n {
			read += r.tail - r.head
			r.head = r.tail
			continue
		}

		r.head += n - read
		read += n - read
	}
}

// SkipBool skips a Bool in the reader.
func (r *Reader) SkipBool() {
	_ = r.readByte()
}

// SkipInt skips an Int in the reader.
func (r *Reader) SkipInt() {
	if r.Error != nil {
		return
	}
	for i := range maxIntBufSize {
		b := r.readByte()
		if r.Error != nil {
			return
		}
		if b&0x80 == 0 {
			if i == maxIntBufSize-1 && b > lastIntByte {
				break
			}
			return
		}
	}
	// Reading would refuse it too: an int takes at most maxIntBufSize bytes.
	r.ReportError("SkipInt", "int overflow")
}

// SkipLong skips a Long in the reader.
func (r *Reader) SkipLong() {
	if r.Error != nil {
		return
	}
	for i := range maxLongBufSize {
		b := r.readByte()
		if r.Error != nil {
			return
		}
		if b&0x80 == 0 {
			if i == maxLongBufSize-1 && b > lastLongByte {
				break
			}
			return
		}
	}
	// Reading would refuse it too: a long takes at most maxLongBufSize bytes.
	r.ReportError("SkipLong", "long overflow")
}

// SkipFloat skips a Float in the reader.
func (r *Reader) SkipFloat() {
	r.SkipNBytes(4)
}

// SkipDouble skips a Double in the reader.
func (r *Reader) SkipDouble() {
	r.SkipNBytes(8)
}

// SkipString skips a String in the reader.
func (r *Reader) SkipString() {
	size := r.ReadLong()
	r.SkipNBytesInt64(size)
}

// SkipBytes skips Bytes in the reader.
func (r *Reader) SkipBytes() {
	size := r.ReadLong()
	r.SkipNBytesInt64(size)
}
