use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::fmt;
use std::str;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors that can occur during RESP protocol parsing or serialization.
#[derive(Debug)]
pub enum ProtocolError {
    /// Not enough data in the buffer to complete parsing.
    Incomplete,
    /// The server type byte was not recognised.
    InvalidType(u8),
    /// A simple-string or error payload contained a `\r` or `\n` before the
    /// terminator.
    UnexpectedCharacter(u8),
    /// An integer field could not be parsed.
    InvalidInteger(String),
    /// A bulk-string length could not be parsed.
    InvalidBulkLength(String),
    /// A bulk-string body was shorter / longer than the declared length.
    BulkLengthMismatch { expected: usize, actual: usize },
    /// The array length could not be parsed.
    InvalidArrayLength(String),
    /// An I/O error occurred during serialisation.
    Io(std::io::Error),
    /// The input was valid RESP but contained a RESP3 hint that this parser
    /// does not (yet) support.
    UnsupportedResp3Type(u8),
    /// A RESP3 map / set / attribute / push was encountered (valid RESP3 but
    /// not representable in the `RespValue` enum).
    UnsupportedFeature(String),
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Incomplete => write!(f, "incomplete frame"),
            Self::InvalidType(b) => write!(f, "invalid type byte: {}", b),
            Self::UnexpectedCharacter(b) => {
                write!(f, "unexpected character in inline data: {}", b)
            }
            Self::InvalidInteger(s) => write!(f, "invalid integer: {}", s),
            Self::InvalidBulkLength(s) => write!(f, "invalid bulk length: {}", s),
            Self::BulkLengthMismatch { expected, actual } => {
                write!(
                    f,
                    "bulk string length mismatch: expected {} got {}",
                    expected, actual
                )
            }
            Self::InvalidArrayLength(s) => write!(f, "invalid array length: {}", s),
            Self::Io(e) => write!(f, "io error: {}", e),
            Self::UnsupportedResp3Type(b) => {
                write!(f, "unsupported RESP3 type byte: {}", b)
            }
            Self::UnsupportedFeature(s) => write!(f, "unsupported feature: {}", s),
        }
    }
}

impl std::error::Error for ProtocolError {}

impl From<std::io::Error> for ProtocolError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

// ---------------------------------------------------------------------------
// RespValue
// ---------------------------------------------------------------------------

/// All possible RESP2 types (plus `Null` for the RESP2 null-bulk / null-array).
///
/// RESP3 hint types that map cleanly onto existing variants (e.g. RESP3 double
/// → `BulkString` with the text representation) are accepted during parsing but
/// the `RespValue` enum stays RESP2-flavoured for simplicity.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RespValue {
    /// `+OK\r\n` – a status / simple string.
    SimpleString(String),
    /// `-ERR message\r\n` – an error.
    Error(String),
    /// `:1000\r\n` – a 64-bit signed integer.
    Integer(i64),
    /// `$6\r\nfoobar\r\n` – a bulk string (binary-safe).
    BulkString(Vec<u8>),
    /// `$-1\r\n` – RESP2 null (no value).
    Null,
    /// `*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n` – an ordered array of `RespValue`.
    Array(Vec<RespValue>),
}

impl RespValue {
    /// Convenience constructor for a simple UTF-8 string.
    pub fn simple(s: impl Into<String>) -> Self {
        Self::SimpleString(s.into())
    }

    /// Convenience constructor for an error.
    pub fn err(s: impl Into<String>) -> Self {
        Self::Error(s.into())
    }

    /// Convenience constructor for a bulk string from bytes.
    pub fn bulk(data: impl Into<Vec<u8>>) -> Self {
        Self::BulkString(data.into())
    }

    /// Convenience constructor for a bulk string from a `&str`.
    pub fn bulk_str(s: &str) -> Self {
        Self::BulkString(s.as_bytes().to_vec())
    }

    /// Convenience constructor for an integer.
    pub fn int(n: i64) -> Self {
        Self::Integer(n)
    }

    /// Convenience constructor for an array.
    pub fn array(items: Vec<RespValue>) -> Self {
        Self::Array(items)
    }

    /// Returns `true` if the value is `Null`.
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    /// Try to interpret the value as a `&str` (only for `SimpleString`, `Error`
    /// or `BulkString` that happens to be valid UTF-8).
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::SimpleString(s) | Self::Error(s) => Some(s.as_str()),
            Self::BulkString(b) => str::from_utf8(b).ok(),
            _ => None,
        }
    }

    /// Try to interpret the value as a byte slice.
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Self::SimpleString(s) => Some(s.as_bytes()),
            Self::Error(s) => Some(s.as_bytes()),
            Self::BulkString(b) => Some(b.as_slice()),
            _ => None,
        }
    }

    /// Try to interpret the value as an `i64`.
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Integer(n) => Some(*n),
            Self::BulkString(b) => str::from_utf8(b).ok()?.parse::<i64>().ok(),
            Self::SimpleString(s) => s.parse::<i64>().ok(),
            _ => None,
        }
    }

    /// Serialize this value into `dst` using the RESP2 wire format.
    pub fn write_to(&self, dst: &mut BytesMut) {
        match self {
            Self::SimpleString(s) => {
                dst.put_u8(b'+');
                dst.extend_from_slice(s.as_bytes());
                dst.extend_from_slice(b"\r\n");
            }
            Self::Error(s) => {
                dst.put_u8(b'-');
                dst.extend_from_slice(s.as_bytes());
                dst.extend_from_slice(b"\r\n");
            }
            Self::Integer(n) => {
                dst.put_u8(b':');
                dst.extend_from_slice(n.to_string().as_bytes());
                dst.extend_from_slice(b"\r\n");
            }
            Self::BulkString(b) => {
                dst.put_u8(b'$');
                dst.extend_from_slice(b.len().to_string().as_bytes());
                dst.extend_from_slice(b"\r\n");
                dst.extend_from_slice(b);
                dst.extend_from_slice(b"\r\n");
            }
            Self::Null => {
                dst.extend_from_slice(b"$-1\r\n");
            }
            Self::Array(items) => {
                dst.put_u8(b'*');
                dst.extend_from_slice(items.len().to_string().as_bytes());
                dst.extend_from_slice(b"\r\n");
                for item in items {
                    item.write_to(dst);
                }
            }
        }
    }

    /// Serialize this value into a new `BytesMut`.
    pub fn to_bytes(&self) -> BytesMut {
        let mut buf = BytesMut::with_capacity(256);
        self.write_to(&mut buf);
        buf
    }
}

// ---------------------------------------------------------------------------
// From implementations
// ---------------------------------------------------------------------------

impl From<String> for RespValue {
    fn from(s: String) -> Self {
        Self::BulkString(s.into_bytes())
    }
}

impl From<&str> for RespValue {
    fn from(s: &str) -> Self {
        Self::BulkString(s.as_bytes().to_vec())
    }
}

impl From<Vec<u8>> for RespValue {
    fn from(v: Vec<u8>) -> Self {
        Self::BulkString(v)
    }
}

impl From<&[u8]> for RespValue {
    fn from(v: &[u8]) -> Self {
        Self::BulkString(v.to_vec())
    }
}

impl From<i64> for RespValue {
    fn from(n: i64) -> Self {
        Self::Integer(n)
    }
}

impl From<i32> for RespValue {
    fn from(n: i32) -> Self {
        Self::Integer(n as i64)
    }
}

impl From<usize> for RespValue {
    fn from(n: usize) -> Self {
        Self::Integer(n as i64)
    }
}

impl From<bool> for RespValue {
    fn from(b: bool) -> Self {
        Self::Integer(i64::from(b))
    }
}

impl From<()> for RespValue {
    fn from(_: ()) -> Self {
        Self::Null
    }
}

impl<T: Into<RespValue>> From<Vec<T>> for RespValue {
    fn from(v: Vec<T>) -> Self {
        Self::Array(v.into_iter().map(Into::into).collect())
    }
}

// ---------------------------------------------------------------------------
// RespParser – streaming / incremental
// ---------------------------------------------------------------------------

/// A streaming RESP2 parser that can handle partial reads.
///
/// Call [`feed`] with incoming data and then [`parse_frame`] to extract the
/// next complete frame.  `parse_frame` returns `Ok(None)` when more data is
/// needed.
pub struct RespParser {
    buf: BytesMut,
}

impl RespParser {
    pub fn new() -> Self {
        Self {
            buf: BytesMut::with_capacity(4096),
        }
    }

    /// Create a parser with a pre-allocated buffer of the given capacity.
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            buf: BytesMut::with_capacity(cap),
        }
    }

    /// Append incoming data to the internal buffer.
    pub fn feed(&mut self, data: &[u8]) {
        self.buf.extend_from_slice(data);
    }

    /// Get a reference to the internal buffer.
    pub fn buffer(&self) -> &BytesMut {
        &self.buf
    }

    /// Attempt to parse one complete RESP frame from the buffer.
    ///
    /// * `Ok(Some(value))` – a complete frame was consumed.
    /// * `Ok(None)`        – more data is needed.
    /// * `Err(..)`         – protocol violation.
    pub fn parse_frame(&mut self) -> Result<Option<RespValue>, ProtocolError> {
        if self.buf.is_empty() {
            return Ok(None);
        }

        let type_byte = self.buf[0];

        // RESP3 hint types that map cleanly to RESP2 equivalents.
        //
        //  '_' → Null      (RESP3 null)
        //  '%' → Array      (RESP3 map – flattened to array of pairs)
        //  '~' → Array      (RESP3 set – same as array on the wire)
        //  '>' → Array      (RESP3 push)
        //  '.' / ',' / '=' / '(' → BulkString (RESP3 double / boolean /
        //                       big-number / verbatim-string as bulk)
        match type_byte {
            b'+' => self.parse_simple_string(),
            b'-' => self.parse_error(),
            b':' => self.parse_integer(),
            b'$' => self.parse_bulk_string(),
            b'*' => self.parse_array(),
            // RESP3 null
            b'_' => self.parse_resp3_null(),
            // RESP3 map – serialise as alternating-key/value array
            b'%' => self.parse_resp3_map(),
            // RESP3 set – serialise as array
            b'~' => self.parse_resp3_set(),
            // RESP3 push – serialise as array
            b'>' => self.parse_resp3_push(),
            // RESP3 double / big-number / verbatim-string – return as bulk string
            b'.' | b',' | b'=' | b'(' => self.parse_resp3_bulk_equivalent(type_byte),
            _ => Err(ProtocolError::InvalidType(type_byte)),
        }
    }

    // -- internal helpers ---------------------------------------------------

    /// Consume `n` bytes from the front of the buffer.
    fn consume(&mut self, n: usize) {
        self.buf.advance(n);
    }

    /// Search for `\r\n` starting at `offset`.  Returns `Some(pos)` where the
    /// `\r` is at `pos`, or `None` if the terminator hasn't been received yet.
    fn find_crlf(&self, offset: usize) -> Option<usize> {
        let data = &self.buf[offset..];
        for i in 0..data.len().saturating_sub(1) {
            if data[i] == b'\r' && data[i + 1] == b'\n' {
                return Some(offset + i);
            }
        }
        None
    }

    /// Read a CRLF-terminated line from the buffer (including the type byte at
    /// position 0).  Returns `Ok(Some(line_bytes))` where `line_bytes` does
    /// NOT include the type byte or the `\r\n`.
    fn read_line(&self) -> Result<Option<Bytes>, ProtocolError> {
        // Start at offset 1 (skip type byte).
        match self.find_crlf(1) {
            None => Ok(None),
            Some(pos) => {
                // Validate that the line doesn't contain stray `\r` or `\n`.
                let line = &self.buf[1..pos];
                if line.iter().any(|&b| b == b'\r' || b == b'\n') {
                    return Err(ProtocolError::UnexpectedCharacter(b'\r'));
                }
                Ok(Some(Bytes::copy_from_slice(line)))
            }
        }
    }

    // -- RESP2 type parsers -------------------------------------------------

    fn parse_simple_string(&mut self) -> Result<Option<RespValue>, ProtocolError> {
        let line = match self.read_line()? {
            Some(l) => l,
            None => return Ok(None),
        };
        let crlf = 1 + line.len() + 2; // type + line + \r\n
        self.consume(crlf);
        let s = String::from_utf8(line.to_vec())
            .map_err(|e| ProtocolError::InvalidInteger(e.to_string()))?;
        Ok(Some(RespValue::SimpleString(s)))
    }

    fn parse_error(&mut self) -> Result<Option<RespValue>, ProtocolError> {
        let line = match self.read_line()? {
            Some(l) => l,
            None => return Ok(None),
        };
        let crlf = 1 + line.len() + 2;
        self.consume(crlf);
        let s = String::from_utf8(line.to_vec())
            .map_err(|e| ProtocolError::InvalidInteger(e.to_string()))?;
        Ok(Some(RespValue::Error(s)))
    }

    fn parse_integer(&mut self) -> Result<Option<RespValue>, ProtocolError> {
        let line = match self.read_line()? {
            Some(l) => l,
            None => return Ok(None),
        };
        let crlf = 1 + line.len() + 2;
        self.consume(crlf);
        let s = str::from_utf8(&line)
            .map_err(|_| ProtocolError::InvalidInteger(String::from_utf8_lossy(&line).into_owned()))?;
        let n: i64 = s
            .parse()
            .map_err(|_| ProtocolError::InvalidInteger(s.to_owned()))?;
        Ok(Some(RespValue::Integer(n)))
    }

    fn parse_bulk_string(&mut self) -> Result<Option<RespValue>, ProtocolError> {
        let line = match self.read_line()? {
            Some(l) => l,
            None => return Ok(None),
        };
        let line_str = str::from_utf8(&line)
            .map_err(|_| ProtocolError::InvalidBulkLength(String::from_utf8_lossy(&line).into_owned()))?;
        let len: i64 = line_str
            .parse()
            .map_err(|_| ProtocolError::InvalidBulkLength(line_str.to_owned()))?;

        // Null bulk string: $-1\r\n
        if len < 0 {
            let total = 1 + line.len() + 2;
            self.consume(total);
            return Ok(Some(RespValue::Null));
        }
        let len = len as usize;

        // We need: type-byte + length-line + \r\n + payload + \r\n
        let header_len = 1 + line.len() + 2;
        let total_needed = header_len + len + 2;

        if self.buf.len() < total_needed {
            return Ok(None);
        }

        // Validate trailing CRLF.
        if self.buf[header_len + len] != b'\r' || self.buf[header_len + len + 1] != b'\n' {
            return Err(ProtocolError::BulkLengthMismatch {
                expected: len,
                actual: 0, // signal: bad terminator
            });
        }

        let data = self.buf[header_len..header_len + len].to_vec();
        self.consume(total_needed);
        Ok(Some(RespValue::BulkString(data)))
    }

    fn parse_array(&mut self) -> Result<Option<RespValue>, ProtocolError> {
        let line = match self.read_line()? {
            Some(l) => l,
            None => return Ok(None),
        };
        let line_str = str::from_utf8(&line)
            .map_err(|_| ProtocolError::InvalidArrayLength(String::from_utf8_lossy(&line).into_owned()))?;
        let len: i64 = line_str
            .parse()
            .map_err(|_| ProtocolError::InvalidArrayLength(line_str.to_owned()))?;

        // Null array: *-1\r\n
        if len < 0 {
            let total = 1 + line.len() + 2;
            self.consume(total);
            return Ok(Some(RespValue::Null));
        }
        let len = len as usize;

        let header_len = 1 + line.len() + 2;
        self.consume(header_len);

        let mut items = Vec::with_capacity(len);
        for _ in 0..len {
            match self.parse_frame()? {
                Some(v) => items.push(v),
                None => return Ok(None), // incomplete – don't consume header
            }
        }
        Ok(Some(RespValue::Array(items)))
    }

    // -- RESP3 helpers ------------------------------------------------------

    /// RESP3 null (`_\r\n`).
    fn parse_resp3_null(&mut self) -> Result<Option<RespValue>, ProtocolError> {
        match self.find_crlf(1) {
            None => Ok(None),
            Some(pos) => {
                self.consume(pos + 2);
                Ok(Some(RespValue::Null))
            }
        }
    }

    /// RESP3 map (`%<count>\r\n<k1>\r\n<v1>\r\n…`) – represented as a flat
    /// array of alternating key/value pairs.
    fn parse_resp3_map(&mut self) -> Result<Option<RespValue>, ProtocolError> {
        let line = match self.read_line()? {
            Some(l) => l,
            None => return Ok(None),
        };
        let line_str = str::from_utf8(&line)
            .map_err(|_| ProtocolError::InvalidArrayLength(String::from_utf8_lossy(&line).into_owned()))?;
        let count: usize = line_str
            .parse()
            .map_err(|_| ProtocolError::InvalidArrayLength(line_str.to_owned()))?;

        let header_len = 1 + line.len() + 2;
        self.consume(header_len);

        let mut items = Vec::with_capacity(count * 2);
        for _ in 0..count * 2 {
            match self.parse_frame()? {
                Some(v) => items.push(v),
                None => return Ok(None),
            }
        }
        Ok(Some(RespValue::Array(items)))
    }

    /// RESP3 set (`~<count>\r\n…`) – same layout as array.
    fn parse_resp3_set(&mut self) -> Result<Option<RespValue>, ProtocolError> {
        let line = match self.read_line()? {
            Some(l) => l,
            None => return Ok(None),
        };
        let line_str = str::from_utf8(&line)
            .map_err(|_| ProtocolError::InvalidArrayLength(String::from_utf8_lossy(&line).into_owned()))?;
        let count: usize = line_str
            .parse()
            .map_err(|_| ProtocolError::InvalidArrayLength(line_str.to_owned()))?;

        let header_len = 1 + line.len() + 2;
        self.consume(header_len);

        let mut items = Vec::with_capacity(count);
        for _ in 0..count {
            match self.parse_frame()? {
                Some(v) => items.push(v),
                None => return Ok(None),
            }
        }
        Ok(Some(RespValue::Array(items)))
    }

    /// RESP3 push (`><count>\r\n…`) – same layout as array.
    fn parse_resp3_push(&mut self) -> Result<Option<RespValue>, ProtocolError> {
        let line = match self.read_line()? {
            Some(l) => l,
            None => return Ok(None),
        };
        let line_str = str::from_utf8(&line)
            .map_err(|_| ProtocolError::InvalidArrayLength(String::from_utf8_lossy(&line).into_owned()))?;
        let count: usize = line_str
            .parse()
            .map_err(|_| ProtocolError::InvalidArrayLength(line_str.to_owned()))?;

        let header_len = 1 + line.len() + 2;
        self.consume(header_len);

        let mut items = Vec::with_capacity(count);
        for _ in 0..count {
            match self.parse_frame()? {
                Some(v) => items.push(v),
                None => return Ok(None),
            }
        }
        Ok(Some(RespValue::Array(items)))
    }

    /// RESP3 double / big-number / verbatim-string – stored as a bulk string
    /// containing the raw text.
    fn parse_resp3_bulk_equivalent(
        &mut self,
        _type_byte: u8,
    ) -> Result<Option<RespValue>, ProtocolError> {
        // These are wire-compatible with RESP2 bulk strings, so delegate.
        self.parse_bulk_string()
    }
}

impl Default for RespParser {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// RespCodec – tokio-util codec integration
// ---------------------------------------------------------------------------

/// A `tokio_util::codec` implementation for framing RESP over a byte stream.
pub struct RespCodec;

impl RespCodec {
    pub fn new() -> Self {
        Self
    }
}

impl Default for RespCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl tokio_util::codec::Decoder for RespCodec {
    type Item = Result<RespValue, ProtocolError>;
    type Error = ProtocolError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.is_empty() {
            return Ok(None);
        }

        let mut parser = RespParser::new();
        parser.feed(src);

        match parser.parse_frame() {
            Ok(Some(value)) => {
                // Advance the source buffer by however many bytes the parser consumed.
                let consumed = src.len() - parser.buffer().len();
                src.advance(consumed);
                Ok(Some(Ok(value)))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

impl tokio_util::codec::Encoder<RespValue> for RespCodec {
    type Error = ProtocolError;

    fn encode(&mut self, item: RespValue, dst: &mut BytesMut) -> Result<(), Self::Error> {
        item.write_to(dst);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Convenience helper: parse a complete buffer in one shot
// ---------------------------------------------------------------------------

/// Parse a single complete RESP frame from `data`.  Returns the frame and the
/// number of bytes consumed.
pub fn parse_frame(data: &[u8]) -> Result<Option<(usize, RespValue)>, ProtocolError> {
    let mut parser = RespParser::new();
    parser.feed(data);
    match parser.parse_frame()? {
        Some(v) => {
            let consumed = data.len() - parser.buffer().len();
            Ok(Some((consumed, v)))
        }
        None => Ok(None),
    }
}

/// Parse all complete frames in `data`, returning them and the number of bytes
/// consumed (leftover bytes are for the caller to re-feed).
pub fn parse_frames(data: &[u8]) -> Result<(Vec<RespValue>, usize), ProtocolError> {
    let mut parser = RespParser::new();
    parser.feed(data);
    let mut frames = Vec::new();
    while let Some(v) = parser.parse_frame()? {
        frames.push(v);
    }
    let consumed = data.len() - parser.buffer().len();
    Ok((frames, consumed))
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // -- RespValue serialization round-trips ---------------------------------

    #[test]
    fn simple_string_roundtrip() {
        let v = RespValue::SimpleString("OK".to_string());
        let bytes = v.to_bytes();
        assert_eq!(&bytes[..], b"+OK\r\n");
    }

    #[test]
    fn error_roundtrip() {
        let v = RespValue::Error("ERR unknown command".to_string());
        let bytes = v.to_bytes();
        assert_eq!(&bytes[..], b"-ERR unknown command\r\n");
    }

    #[test]
    fn integer_roundtrip() {
        let v = RespValue::Integer(42);
        let bytes = v.to_bytes();
        assert_eq!(&bytes[..], b":42\r\n");
    }

    #[test]
    fn negative_integer_roundtrip() {
        let v = RespValue::Integer(-100);
        let bytes = v.to_bytes();
        assert_eq!(&bytes[..], b":-100\r\n");
        // re-parse
        let (consumed, parsed) = parse_frame(&bytes).unwrap().unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(parsed, RespValue::Integer(-100));
    }

    #[test]
    fn bulk_string_roundtrip() {
        let v = RespValue::BulkString(b"hello world".to_vec());
        let bytes = v.to_bytes();
        assert_eq!(&bytes[..], b"$11\r\nhello world\r\n");
    }

    #[test]
    fn null_roundtrip() {
        let v = RespValue::Null;
        let bytes = v.to_bytes();
        assert_eq!(&bytes[..], b"$-1\r\n");
    }

    #[test]
    fn array_roundtrip() {
        let v = RespValue::Array(vec![
            RespValue::BulkString(b"foo".to_vec()),
            RespValue::BulkString(b"bar".to_vec()),
        ]);
        let bytes = v.to_bytes();
        assert_eq!(&bytes[..], b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
    }

    #[test]
    fn empty_array_roundtrip() {
        let v = RespValue::Array(vec![]);
        let bytes = v.to_bytes();
        assert_eq!(&bytes[..], b"*0\r\n");
    }

    #[test]
    fn nested_array_roundtrip() {
        let v = RespValue::Array(vec![
            RespValue::Integer(1),
            RespValue::Array(vec![
                RespValue::BulkString(b"two".to_vec()),
                RespValue::Null,
            ]),
        ]);
        let bytes = v.to_bytes();
        let (consumed, parsed) = parse_frame(&bytes).unwrap().unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(parsed, v);
    }

    // -- From implementations -----------------------------------------------

    #[test]
    fn from_string_is_bulk() {
        let v: RespValue = "hello".to_string().into();
        assert_eq!(v, RespValue::BulkString(b"hello".to_vec()));
    }

    #[test]
    fn from_str_is_bulk() {
        let v: RespValue = "hello".into();
        assert_eq!(v, RespValue::BulkString(b"hello".to_vec()));
    }

    #[test]
    fn from_vec_u8_is_bulk() {
        let v: RespValue = vec![1u8, 2, 3].into();
        assert_eq!(v, RespValue::BulkString(vec![1, 2, 3]));
    }

    #[test]
    fn from_i64_is_integer() {
        let v: RespValue = 99i64.into();
        assert_eq!(v, RespValue::Integer(99));
    }

    #[test]
    fn from_i32_is_integer() {
        let v: RespValue = 42i32.into();
        assert_eq!(v, RespValue::Integer(42));
    }

    #[test]
    fn from_bool_is_integer() {
        let v: RespValue = true.into();
        assert_eq!(v, RespValue::Integer(1));
        let v: RespValue = false.into();
        assert_eq!(v, RespValue::Integer(0));
    }

    #[test]
    fn from_unit_is_null() {
        let v: RespValue = ().into();
        assert_eq!(v, RespValue::Null);
    }

    #[test]
    fn from_vec_respvalue_is_array() {
        let v: RespValue = vec![
            RespValue::Integer(1),
            RespValue::SimpleString("ok".to_string()),
        ]
        .into();
        assert_eq!(
            v,
            RespValue::Array(vec![
                RespValue::Integer(1),
                RespValue::SimpleString("ok".to_string()),
            ])
        );
    }

    // -- RespParser streaming tests -----------------------------------------

    #[test]
    fn parse_simple_string() {
        let mut p = RespParser::new();
        p.feed(b"+OK\r\n");
        let v = p.parse_frame().unwrap().unwrap();
        assert_eq!(v, RespValue::SimpleString("OK".to_string()));
    }

    #[test]
    fn parse_error() {
        let mut p = RespParser::new();
        p.feed(b"-ERR bad\r\n");
        let v = p.parse_frame().unwrap().unwrap();
        assert_eq!(v, RespValue::Error("ERR bad".to_string()));
    }

    #[test]
    fn parse_integer() {
        let mut p = RespParser::new();
        p.feed(b":1000\r\n");
        let v = p.parse_frame().unwrap().unwrap();
        assert_eq!(v, RespValue::Integer(1000));
    }

    #[test]
    fn parse_negative_integer() {
        let mut p = RespParser::new();
        p.feed(b":-100\r\n");
        let v = p.parse_frame().unwrap().unwrap();
        assert_eq!(v, RespValue::Integer(-100));
    }

    #[test]
    fn parse_bulk_string() {
        let mut p = RespParser::new();
        p.feed(b"$5\r\nhello\r\n");
        let v = p.parse_frame().unwrap().unwrap();
        assert_eq!(v, RespValue::BulkString(b"hello".to_vec()));
    }

    #[test]
    fn parse_empty_bulk_string() {
        let mut p = RespParser::new();
        p.feed(b"$0\r\n\r\n");
        let v = p.parse_frame().unwrap().unwrap();
        assert_eq!(v, RespValue::BulkString(b"".to_vec()));
    }

    #[test]
    fn parse_null_bulk_string() {
        let mut p = RespParser::new();
        p.feed(b"$-1\r\n");
        let v = p.parse_frame().unwrap().unwrap();
        assert_eq!(v, RespValue::Null);
    }

    #[test]
    fn parse_null_array() {
        let mut p = RespParser::new();
        p.feed(b"*-1\r\n");
        let v = p.parse_frame().unwrap().unwrap();
        assert_eq!(v, RespValue::Null);
    }

    #[test]
    fn parse_array() {
        let mut p = RespParser::new();
        p.feed(b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
        let v = p.parse_frame().unwrap().unwrap();
        assert_eq!(
            v,
            RespValue::Array(vec![
                RespValue::BulkString(b"foo".to_vec()),
                RespValue::BulkString(b"bar".to_vec()),
            ])
        );
    }

    #[test]
    fn parse_empty_array() {
        let mut p = RespParser::new();
        p.feed(b"*0\r\n");
        let v = p.parse_frame().unwrap().unwrap();
        assert_eq!(v, RespValue::Array(vec![]));
    }

    #[test]
    fn parse_nested_array() {
        let mut p = RespParser::new();
        p.feed(b"*2\r\n*2\r\n:1\r\n:2\r\n*1\r\n$3\r\nabc\r\n");
        let v = p.parse_frame().unwrap().unwrap();
        assert_eq!(
            v,
            RespValue::Array(vec![
                RespValue::Array(vec![RespValue::Integer(1), RespValue::Integer(2)]),
                RespValue::Array(vec![RespValue::BulkString(b"abc".to_vec())]),
            ])
        );
    }

    // -- Partial / streaming tests ------------------------------------------

    #[test]
    fn partial_simple_string() {
        let mut p = RespParser::new();
        p.feed(b"+OK");
        assert!(p.parse_frame().unwrap().is_none());
        p.feed(b"\r\n");
        let v = p.parse_frame().unwrap().unwrap();
        assert_eq!(v, RespValue::SimpleString("OK".to_string()));
    }

    #[test]
    fn partial_bulk_string() {
        let mut p = RespParser::new();
        p.feed(b"$11\r\nhello wo");
        assert!(p.parse_frame().unwrap().is_none());
        p.feed(b"rld\r\n");
        let v = p.parse_frame().unwrap().unwrap();
        assert_eq!(v, RespValue::BulkString(b"hello world".to_vec()));
    }

    #[test]
    fn byte_at_a_time() {
        let data = b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
        let mut p = RespParser::new();
        for &byte in data {
            p.feed(&[byte]);
        }
        let v = p.parse_frame().unwrap().unwrap();
        assert_eq!(
            v,
            RespValue::Array(vec![
                RespValue::BulkString(b"foo".to_vec()),
                RespValue::BulkString(b"bar".to_vec()),
            ])
        );
    }

    #[test]
    fn multiple_frames_in_one_feed() {
        let mut p = RespParser::new();
        p.feed(b"+OK\r\n:42\r\n$5\r\nhello\r\n");
        let f1 = p.parse_frame().unwrap().unwrap();
        let f2 = p.parse_frame().unwrap().unwrap();
        let f3 = p.parse_frame().unwrap().unwrap();
        assert_eq!(f1, RespValue::SimpleString("OK".to_string()));
        assert_eq!(f2, RespValue::Integer(42));
        assert_eq!(f3, RespValue::BulkString(b"hello".to_vec()));
    }

    #[test]
    fn empty_buffer_returns_none() {
        let mut p = RespParser::new();
        assert!(p.parse_frame().unwrap().is_none());
    }

    // -- parse_frame / parse_frames helpers ----------------------------------

    #[test]
    fn parse_frame_helper() {
        let data = b":100\r\n";
        let (consumed, v) = parse_frame(data).unwrap().unwrap();
        assert_eq!(consumed, 6);
        assert_eq!(v, RespValue::Integer(100));
    }

    #[test]
    fn parse_frames_helper() {
        let data = b"+OK\r\n:42\r\n$3\r\nfoo\r\n";
        let (frames, consumed) = parse_frames(data).unwrap();
        assert_eq!(consumed, data.len());
        assert_eq!(frames.len(), 3);
        assert_eq!(frames[0], RespValue::SimpleString("OK".to_string()));
        assert_eq!(frames[1], RespValue::Integer(42));
        assert_eq!(frames[2], RespValue::BulkString(b"foo".to_vec()));
    }

    #[test]
    fn parse_frames_helper_with_leftover() {
        let data = b"+OK\r\n:42";
        let (frames, consumed) = parse_frames(data).unwrap();
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0], RespValue::SimpleString("OK".to_string()));
        assert_eq!(consumed, 5);
    }

    // -- RESP3 compat tests -------------------------------------------------

    #[test]
    fn resp3_null() {
        let mut p = RespParser::new();
        p.feed(b"_\r\n");
        let v = p.parse_frame().unwrap().unwrap();
        assert_eq!(v, RespValue::Null);
    }

    #[test]
    fn resp3_map_as_array() {
        let mut p = RespParser::new();
        // %2\r\n$3\r\nfoo\r\n:1\r\n$3\r\nbar\r\n:2\r\n
        p.feed(b"%2\r\n$3\r\nfoo\r\n:1\r\n$3\r\nbar\r\n:2\r\n");
        let v = p.parse_frame().unwrap().unwrap();
        assert_eq!(
            v,
            RespValue::Array(vec![
                RespValue::BulkString(b"foo".to_vec()),
                RespValue::Integer(1),
                RespValue::BulkString(b"bar".to_vec()),
                RespValue::Integer(2),
            ])
        );
    }

    #[test]
    fn resp3_set_as_array() {
        let mut p = RespParser::new();
        p.feed(b"~2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
        let v = p.parse_frame().unwrap().unwrap();
        assert_eq!(
            v,
            RespValue::Array(vec![
                RespValue::BulkString(b"foo".to_vec()),
                RespValue::BulkString(b"bar".to_vec()),
            ])
        );
    }

    // -- Error cases --------------------------------------------------------

    #[test]
    fn invalid_type_byte() {
        let mut p = RespParser::new();
        p.feed(b"!bad\r\n");
        let err = p.parse_frame().unwrap_err();
        assert!(matches!(err, ProtocolError::InvalidType(b'!')));
    }

    #[test]
    fn invalid_integer() {
        let mut p = RespParser::new();
        p.feed(b":notanumber\r\n");
        let err = p.parse_frame().unwrap_err();
        assert!(matches!(err, ProtocolError::InvalidInteger(_)));
    }

    #[test]
    fn invalid_bulk_length() {
        let mut p = RespParser::new();
        p.feed(b"$abc\r\n");
        let err = p.parse_frame().unwrap_err();
        assert!(matches!(err, ProtocolError::InvalidBulkLength(_)));
    }

    #[test]
    fn invalid_array_length() {
        let mut p = RespParser::new();
        p.feed(b"*xyz\r\n");
        let err = p.parse_frame().unwrap_err();
        assert!(matches!(err, ProtocolError::InvalidArrayLength(_)));
    }

    // -- RespValue accessor tests -------------------------------------------

    #[test]
    fn is_null() {
        assert!(RespValue::Null.is_null());
        assert!(!RespValue::Integer(0).is_null());
    }

    #[test]
    fn as_str() {
        assert_eq!(
            RespValue::SimpleString("ok".into()).as_str(),
            Some("ok")
        );
        assert_eq!(RespValue::Error("err".into()).as_str(), Some("err"));
        assert_eq!(
            RespValue::BulkString(b"hello".to_vec()).as_str(),
            Some("hello")
        );
        assert_eq!(
            RespValue::BulkString(vec![0xff]).as_str(),
            None // invalid UTF-8
        );
        assert_eq!(RespValue::Integer(1).as_str(), None);
    }

    #[test]
    fn as_bytes() {
        assert_eq!(
            RespValue::BulkString(b"data".to_vec()).as_bytes(),
            Some(&b"data"[..])
        );
        assert_eq!(RespValue::Integer(1).as_bytes(), None);
    }

    #[test]
    fn as_i64() {
        assert_eq!(RespValue::Integer(42).as_i64(), Some(42));
        assert_eq!(
            RespValue::BulkString(b"123".to_vec()).as_i64(),
            Some(123)
        );
        assert_eq!(
            RespValue::BulkString(b"abc".to_vec()).as_i64(),
            None
        );
        assert_eq!(
            RespValue::SimpleString("456".into()).as_i64(),
            Some(456)
        );
    }

    // -- Codec tests --------------------------------------------------------

    #[test]
    fn codec_roundtrip() {
        use tokio_util::codec::{Decoder, Encoder};

        let mut codec = RespCodec::new();
        let original = RespValue::Array(vec![
            RespValue::BulkString(b"SET".to_vec()),
            RespValue::BulkString(b"key".to_vec()),
            RespValue::BulkString(b"value".to_vec()),
        ]);

        let mut buf = BytesMut::new();
        codec.encode(original.clone(), &mut buf).unwrap();

        let decoded = codec.decode(&mut buf).unwrap().unwrap().unwrap();
        assert_eq!(decoded, original);
        assert!(buf.is_empty());
    }

    #[test]
    fn codec_partial_decode_returns_none() {
        use tokio_util::codec::Decoder;

        let mut codec = RespCodec::new();
        let mut buf = BytesMut::from(&b"+OK"[..]);
        assert!(codec.decode(&mut buf).unwrap().is_none());
    }

    #[test]
    fn codec_multiple_frames() {
        use tokio_util::codec::Decoder;

        let mut codec = RespCodec::new();
        let mut buf = BytesMut::from(&b"+OK\r\n:42\r\n"[..]);

        let f1 = codec.decode(&mut buf).unwrap().unwrap().unwrap();
        let f2 = codec.decode(&mut buf).unwrap().unwrap().unwrap();
        assert_eq!(f1, RespValue::SimpleString("OK".to_string()));
        assert_eq!(f2, RespValue::Integer(42));
        assert!(buf.is_empty());
    }

    // -- Display impl -------------------------------------------------------

    #[test]
    fn error_display() {
        let e = ProtocolError::Incomplete;
        assert_eq!(format!("{}", e), "incomplete frame");
        let e = ProtocolError::InvalidType(b'!');
        assert_eq!(format!("{}", e), "invalid type byte: 33");
    }

    // -- Stress: large bulk string ------------------------------------------

    #[test]
    fn large_bulk_string() {
        let payload = vec![b'x'; 100_000];
        let mut header = format!("${}\r\n", payload.len());
        header.push_str(&String::from_utf8_lossy(&payload));
        header.push_str("\r\n");

        let mut p = RespParser::new();
        p.feed(header.as_bytes());
        let v = p.parse_frame().unwrap().unwrap();
        match v {
            RespValue::BulkString(data) => assert_eq!(data.len(), 100_000),
            _ => panic!("expected BulkString"),
        }
    }
}
