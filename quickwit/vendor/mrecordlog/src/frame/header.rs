pub const HEADER_LEN: usize = 4 + 2 + 1;

/// High bit of the `frame_type` byte. When set, the frame payload is a chunk of a
/// zstd-compressed record; the remaining bits hold the [`FrameType`]. Records written
/// before this flag existed have the bit clear and decode unchanged.
const COMPRESSED_FLAG: u8 = 0b1000_0000;

fn crc32(data: &[u8], frame_type_byte: u8) -> u32 {
    let mut hash = crc32fast::Hasher::default();
    hash.update(&[frame_type_byte]);
    hash.update(data);
    hash.finalize()
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct Header {
    checksum: u32,
    len: u16,
    frame_type: FrameType,
    compressed: bool,
}

impl Header {
    pub fn for_payload(frame_type: FrameType, compressed: bool, payload: &[u8]) -> Header {
        assert!(payload.len() < crate::BLOCK_NUM_BYTES);
        Header {
            checksum: crc32(payload, Self::type_byte(frame_type, compressed)),
            len: payload.len() as u16,
            frame_type,
            compressed,
        }
    }

    /// The single header byte that carries both the frame type and the compressed flag,
    /// and that is covered by the CRC.
    fn type_byte(frame_type: FrameType, compressed: bool) -> u8 {
        frame_type.to_u8() | if compressed { COMPRESSED_FLAG } else { 0 }
    }

    pub fn len(&self) -> usize {
        self.len as usize
    }

    pub fn frame_type(&self) -> FrameType {
        self.frame_type
    }

    pub fn is_compressed(&self) -> bool {
        self.compressed
    }

    pub fn check(&self, payload: &[u8]) -> bool {
        crc32(payload, Self::type_byte(self.frame_type, self.compressed)) == self.checksum
    }

    /// Serialize the header
    ///
    /// # Panics
    /// panic if `dest` isn't exactly `HEADER_LEN` bytes long
    pub fn serialize(&self, dest: &mut [u8]) {
        assert_eq!(dest.len(), HEADER_LEN);
        dest[..4].copy_from_slice(&self.checksum.to_le_bytes()[..]);
        dest[4..6].copy_from_slice(&self.len.to_le_bytes()[..]);
        dest[6] = Self::type_byte(self.frame_type, self.compressed);
    }

    /// Deserialize a header
    ///
    /// # Panics
    /// panic if `data` isn't exactly `HEADER_LEN` bytes long
    pub fn deserialize(data: &[u8]) -> Option<Header> {
        assert_eq!(data.len(), HEADER_LEN);
        let checksum = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
        let len = u16::from_le_bytes([data[4], data[5]]);
        let compressed = data[6] & COMPRESSED_FLAG != 0;
        let frame_type = FrameType::from_u8(data[6] & !COMPRESSED_FLAG)?;
        Some(Header {
            checksum,
            len,
            frame_type,
            compressed,
        })
    }
}

#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum FrameType {
    Full = 1u8,
    First = 2u8,
    Middle = 3u8,
    Last = 4u8,
}

impl FrameType {
    fn from_u8(b: u8) -> Option<FrameType> {
        match b {
            1u8 => Some(FrameType::Full),
            2u8 => Some(FrameType::First),
            3u8 => Some(FrameType::Middle),
            4u8 => Some(FrameType::Last),
            _ => None,
        }
    }

    fn to_u8(self) -> u8 {
        self as u8
    }

    pub fn is_first_frame_of_record(&self) -> bool {
        match self {
            FrameType::Full | FrameType::First => true,
            FrameType::Last | FrameType::Middle => false,
        }
    }

    pub fn is_last_frame_of_record(&self) -> bool {
        match self {
            FrameType::Full | FrameType::Last => true,
            FrameType::First | FrameType::Middle => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::frame::header::{Header, HEADER_LEN};
    use crate::frame::FrameType;

    #[test]
    fn test_frame_type_serialize_deserialize() {
        const ALL_FRAME_TYPES: [FrameType; 4] = [
            FrameType::Full,
            FrameType::First,
            FrameType::Middle,
            FrameType::Last,
        ];
        for frame_type in ALL_FRAME_TYPES {
            assert_eq!(FrameType::from_u8(frame_type.to_u8()), Some(frame_type));
        }
    }

    #[test]
    fn test_frame_deserialize_invalid() {
        assert_eq!(FrameType::from_u8(14u8), None);
    }

    #[test]
    fn test_header_serialize_deserialize() {
        let header = Header {
            checksum: 17u32,
            len: 42,
            frame_type: FrameType::Full,
            compressed: false,
        };
        let mut buffer = [0u8; HEADER_LEN];
        header.serialize(&mut buffer);
        let serdeser_header = Header::deserialize(&buffer).unwrap();
        assert_eq!(header, serdeser_header);
    }

    #[test]
    fn test_header_serialize_deserialize_compressed() {
        let header = Header {
            checksum: 17u32,
            len: 42,
            frame_type: FrameType::Middle,
            compressed: true,
        };
        let mut buffer = [0u8; HEADER_LEN];
        header.serialize(&mut buffer);
        let serdeser_header = Header::deserialize(&buffer).unwrap();
        assert_eq!(header, serdeser_header);
        assert!(serdeser_header.is_compressed());
    }

    #[test]
    fn test_header_deserialize_invalid() {
        let invalid_header_buffer = [14u8; HEADER_LEN];
        assert_eq!(Header::deserialize(&invalid_header_buffer), None);
    }
}
