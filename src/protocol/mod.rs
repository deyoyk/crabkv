use bytes::{Buf, BytesMut};

#[derive(Debug, Clone, PartialEq)]
pub enum RespValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Vec<u8>),
    Array(Vec<RespValue>),
    Null,
    NullArray,
}

pub struct RespParser {
    buffer: BytesMut,
}

impl RespParser {
    pub fn new() -> Self {
        Self {
            buffer: BytesMut::with_capacity(4096),
        }
    }

    pub fn feed(&mut self, data: &[u8]) {
        self.buffer.extend_from_slice(data);
    }

    pub fn parse_frame(&mut self) -> Result<Option<RespValue>, String> {
        if self.buffer.is_empty() {
            return Ok(None);
        }

        match self.buffer[0] {
            b'+' => self.parse_simple_string(),
            b'-' => self.parse_error(),
            b':' => self.parse_integer(),
            b'$' => self.parse_bulk_string(),
            b'*' => self.parse_array(),
            other => Err(format!("Invalid frame type: {}", other as char)),
        }
    }

    fn parse_simple_string(&mut self) -> Result<Option<RespValue>, String> {
        if let Some(pos) = find_crlf(&self.buffer) {
            let line = String::from_utf8(self.buffer[1..pos].to_vec())
                .map_err(|e| e.to_string())?;
            self.buffer.advance(pos + 2);
            Ok(Some(RespValue::SimpleString(line)))
        } else {
            Ok(None)
        }
    }

    fn parse_error(&mut self) -> Result<Option<RespValue>, String> {
        if let Some(pos) = find_crlf(&self.buffer) {
            let line = String::from_utf8(self.buffer[1..pos].to_vec())
                .map_err(|e| e.to_string())?;
            self.buffer.advance(pos + 2);
            Ok(Some(RespValue::Error(line)))
        } else {
            Ok(None)
        }
    }

    fn parse_integer(&mut self) -> Result<Option<RespValue>, String> {
        if let Some(pos) = find_crlf(&self.buffer) {
            let line = std::str::from_utf8(&self.buffer[1..pos])
                .map_err(|e| e.to_string())?;
            let n: i64 = line.parse().map_err(|e: std::num::ParseIntError| e.to_string())?;
            self.buffer.advance(pos + 2);
            Ok(Some(RespValue::Integer(n)))
        } else {
            Ok(None)
        }
    }

    fn parse_bulk_string(&mut self) -> Result<Option<RespValue>, String> {
        if let Some(pos) = find_crlf(&self.buffer) {
            let len_str = std::str::from_utf8(&self.buffer[1..pos])
                .map_err(|e| e.to_string())?;
            let len: isize = len_str.parse().map_err(|e: std::num::ParseIntError| e.to_string())?;

            if len < 0 {
                self.buffer.advance(pos + 2);
                return Ok(Some(RespValue::Null));
            }

            let len = len as usize;
            let total = pos + 2 + len + 2; // header + \r\n + data + \r\n
            if self.buffer.len() < total {
                return Ok(None);
            }

            let start = pos + 2;
            let data = self.buffer[start..start + len].to_vec();
            self.buffer.advance(total);
            Ok(Some(RespValue::BulkString(data)))
        } else {
            Ok(None)
        }
    }

    fn parse_array(&mut self) -> Result<Option<RespValue>, String> {
        if let Some(pos) = find_crlf(&self.buffer) {
            let len_str = std::str::from_utf8(&self.buffer[1..pos])
                .map_err(|e| e.to_string())?;
            let len: isize = len_str.parse().map_err(|e: std::num::ParseIntError| e.to_string())?;

            if len < 0 {
                self.buffer.advance(pos + 2);
                return Ok(Some(RespValue::NullArray));
            }

            let len = len as usize;
            self.buffer.advance(pos + 2);

            let mut items = Vec::with_capacity(len);
            for _ in 0..len {
                match self.parse_frame()? {
                    Some(v) => items.push(v),
                    None => return Ok(None),
                }
            }
            Ok(Some(RespValue::Array(items)))
        } else {
            Ok(None)
        }
    }
}

impl Default for RespParser {
    fn default() -> Self {
        Self::new()
    }
}

fn find_crlf(buf: &[u8]) -> Option<usize> {
    buf.windows(2).position(|w| w == b"\r\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_simple_string() {
        let mut parser = RespParser::new();
        parser.feed(b"+OK\r\n");
        let frame = parser.parse_frame().unwrap().unwrap();
        assert_eq!(frame, RespValue::SimpleString("OK".to_string()));
    }

    #[test]
    fn parse_error() {
        let mut parser = RespParser::new();
        parser.feed(b"-ERR unknown command\r\n");
        let frame = parser.parse_frame().unwrap().unwrap();
        assert_eq!(frame, RespValue::Error("ERR unknown command".to_string()));
    }

    #[test]
    fn parse_integer() {
        let mut parser = RespParser::new();
        parser.feed(b":42\r\n");
        let frame = parser.parse_frame().unwrap().unwrap();
        assert_eq!(frame, RespValue::Integer(42));
    }

    #[test]
    fn parse_negative_integer() {
        let mut parser = RespParser::new();
        parser.feed(b":-100\r\n");
        let frame = parser.parse_frame().unwrap().unwrap();
        assert_eq!(frame, RespValue::Integer(-100));
    }

    #[test]
    fn parse_bulk_string() {
        let mut parser = RespParser::new();
        parser.feed(b"$5\r\nhello\r\n");
        let frame = parser.parse_frame().unwrap().unwrap();
        assert_eq!(frame, RespValue::BulkString(b"hello".to_vec()));
    }

    #[test]
    fn parse_null_bulk_string() {
        let mut parser = RespParser::new();
        parser.feed(b"$-1\r\n");
        let frame = parser.parse_frame().unwrap().unwrap();
        assert_eq!(frame, RespValue::Null);
    }

    #[test]
    fn parse_empty_bulk_string() {
        let mut parser = RespParser::new();
        parser.feed(b"$0\r\n\r\n");
        let frame = parser.parse_frame().unwrap().unwrap();
        assert_eq!(frame, RespValue::BulkString(Vec::new()));
    }

    #[test]
    fn parse_array() {
        let mut parser = RespParser::new();
        parser.feed(b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
        let frame = parser.parse_frame().unwrap().unwrap();
        assert_eq!(
            frame,
            RespValue::Array(vec![
                RespValue::BulkString(b"foo".to_vec()),
                RespValue::BulkString(b"bar".to_vec()),
            ])
        );
    }

    #[test]
    fn parse_nested_array() {
        let mut parser = RespParser::new();
        parser.feed(b"*2\r\n*1\r\n$3\r\nfoo\r\n:1\r\n");
        let frame = parser.parse_frame().unwrap().unwrap();
        assert_eq!(
            frame,
            RespValue::Array(vec![
                RespValue::Array(vec![RespValue::BulkString(b"foo".to_vec())]),
                RespValue::Integer(1),
            ])
        );
    }

    #[test]
    fn parse_null_array() {
        let mut parser = RespParser::new();
        parser.feed(b"*-1\r\n");
        let frame = parser.parse_frame().unwrap().unwrap();
        assert_eq!(frame, RespValue::NullArray);
    }

    #[test]
    fn parse_empty_array() {
        let mut parser = RespParser::new();
        parser.feed(b"*0\r\n");
        let frame = parser.parse_frame().unwrap().unwrap();
        assert_eq!(frame, RespValue::Array(vec![]));
    }

    #[test]
    fn partial_frame_returns_none() {
        let mut parser = RespParser::new();
        parser.feed(b"+OK");
        assert_eq!(parser.parse_frame().unwrap(), None);
    }

    #[test]
    fn multiple_frames() {
        let mut parser = RespParser::new();
        parser.feed(b"+OK\r\n:42\r\n");
        let f1 = parser.parse_frame().unwrap().unwrap();
        assert_eq!(f1, RespValue::SimpleString("OK".to_string()));
        let f2 = parser.parse_frame().unwrap().unwrap();
        assert_eq!(f2, RespValue::Integer(42));
    }

    #[test]
    fn parse_command_array() {
        let mut parser = RespParser::new();
        parser.feed(b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
        let frame = parser.parse_frame().unwrap().unwrap();
        match frame {
            RespValue::Array(items) => {
                assert_eq!(items.len(), 3);
                assert_eq!(items[0], RespValue::BulkString(b"SET".to_vec()));
                assert_eq!(items[1], RespValue::BulkString(b"foo".to_vec()));
                assert_eq!(items[2], RespValue::BulkString(b"bar".to_vec()));
            }
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn default_creates_parser() {
        let parser = RespParser::default();
        assert!(parser.buffer.is_empty());
    }
}
