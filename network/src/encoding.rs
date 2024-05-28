use std::io::{Cursor, Read};
use std::string::ToString;
use std::u32;
use bytes::{BufMut, Bytes, BytesMut};
use byteorder::{BigEndian, ReadBytesExt};

#[derive(Clone, Debug)]
pub enum DataEncodingType {
    BINARY,
    JSON,
}

impl DataEncodingType {
    pub fn parse(data_type: u8) -> Option<Self> {
        match data_type {
            0u8 => { Some(Self::BINARY) }
            1u8 => { Some(Self::JSON) },
            _ => { None }
        }
    }

    pub fn u8_value(&self) -> u8 {
        match self {
            DataEncodingType::BINARY => {0}
            DataEncodingType::JSON => {1}
        }
    }
}

pub const ENCODING_DELIMITER: &[u8; 4] = b"!CZ~";

#[derive(Clone, Debug, PartialEq, thiserror::Error, strum::Display)]
pub enum EncodingError {
    UnableToDecodeStream(String),
    CorruptedDataStream,
    InvalidEncoding
}

pub type Result<T> = std::result::Result<T, EncodingError>;


#[derive(Default, Debug, Clone)]
pub struct Encoding {
    version: Option<u8>,
    data_length: Option<u32>,
    data: BytesMut,
    encoding_type: Option<DataEncodingType>,
    is_stream: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum EncodingState {
    Complete,
    Incomplete,
}

impl Encoding {

    pub fn is_complete(&self) -> bool {
        self.data_length
            .map(|l| l == self.data.len() as u32)
            .unwrap_or(false)
    }

    pub fn get_bytes(&self) -> &[u8] {
        self.data.as_ref()
    }

    pub fn read_from_stream<'a>(&mut self, buf: &'a mut [u8]) -> Result<(EncodingState, Option<&'a mut [u8]>)> {
        let mut cursor_start = 0;
        let mut cursor_end = ENCODING_DELIMITER.len();

        let mut delimiter = buf.get(cursor_start..cursor_end);
        let is_first_part_encoding_delimiter = delimiter
            .map(|d| d == ENCODING_DELIMITER)
            .unwrap_or(false);

        // If a stream is being read & it's not complete and a new stream arrives
        // it's a corrupted stream
        let is_data_being_read = self.data_length
            .map(|l| l != self.data.len() as u32)
            .unwrap_or(false);
        if is_data_being_read && is_first_part_encoding_delimiter {
            return Err(EncodingError::CorruptedDataStream);
        }

        // If a stream is being read & it's not complete
        // it adds the buffer to it's stream and
        // mutates the buffer to the start of a continuous stream
        if is_data_being_read && !is_first_part_encoding_delimiter {
            let remaining_bytes = (self.data_length.unwrap() as i32) - (self.data.len() as i32);
            if (buf.len() as i32) < remaining_bytes {
                self.data.put_slice(buf);
                return Ok((EncodingState::Incomplete, None));
            }

            let (complete_part,  another_stream_part) = buf.split_at_mut(remaining_bytes as usize);
            self.data.put_slice(complete_part);
            let buf = if another_stream_part.is_empty() {
                None
            } else {
                Some(buf)
            };
            return Ok((EncodingState::Complete, buf));
        }


        // If data stream is not being read and first part is not a delimiter
        // it's an invalid stream
        if !is_data_being_read && !is_first_part_encoding_delimiter {
            return Err(EncodingError::CorruptedDataStream);
        }

        if buf.len() < 7 {
            return Ok((EncodingState::Incomplete, Some(buf)));
        }

        cursor_start = cursor_end;
        cursor_end = cursor_end + 1;

        let version = buf.get(cursor_start..cursor_end)
            .map(|v| u8::from_be_bytes(v.try_into().unwrap()));
        if version.is_none() {
            return Err(EncodingError::UnableToDecodeStream("Unable to extract network version".to_string()))
        }
        self.version = version;

        cursor_start = cursor_end;
        cursor_end = cursor_end + 1;

        let encoding_type = buf.get(cursor_start..cursor_end)
            .map(|v| {
                Cursor::new(v.to_vec()).read_u8().ok()
                    .map(DataEncodingType::parse)
                    .flatten()
            }).flatten();
        if encoding_type.is_none() {
            return Err(EncodingError::UnableToDecodeStream("Unable to extract data type".to_string()))
        }
        self.encoding_type = encoding_type;

        cursor_start = cursor_end;
        cursor_end = cursor_end + 1;

        let is_stream = buf.get(cursor_start..cursor_end)
            .map(|v| {
                Cursor::new(v.to_vec()).read_u8().ok()
                    .map(|v| v == 1u8)
            }).flatten();
        if is_stream.is_none() {
            return Err(EncodingError::UnableToDecodeStream("Unable to detect if stream is continuous".to_string()))
        }
        self.is_stream = is_stream.unwrap();

        cursor_start = cursor_end;
        cursor_end = cursor_end + 4;

        let data_length = buf.get(cursor_start..cursor_end)
            .map(|v| {
                Cursor::new(v.to_vec()).read_u32::<BigEndian>().ok()
            }).flatten();
        if data_length.is_none() {
            return Err(EncodingError::UnableToDecodeStream("Unable to extract length of data stream".to_string()))
        }
        self.data_length = data_length;

        let (_, mut data_part) = buf.split_at_mut(cursor_end);
        return self.read_from_stream(data_part);
    }
}

pub struct ObaEncoder;

impl ObaEncoder {


}

/**
  V1 Encoding layout
 -----------------------------------------------------------------------------------------------------------------------|
  delimiter(32bytes)|version(1byte)|data_type(1byte)|is_stream(1byte)|data_length(32byte)|data)(data_length bytes)
 -----------------------------------------------------------------------------------------------------------------------|
**/

const  ENCODING_VERSION_1: u8 = 1u8;

pub fn create_v1_encoding(encoding_type: DataEncodingType, raw_byte: &[u8]) -> BytesMut {
    return create_v1_encoding_inner(encoding_type, raw_byte, false);
}
pub fn create_v1_stream_encoding(encoding_type: DataEncodingType, raw_byte: &[u8]) -> BytesMut {
    return create_v1_encoding_inner(encoding_type, raw_byte, true);
}

fn create_v1_encoding_inner(encoding_type: DataEncodingType, raw_byte: &[u8], is_stream: bool) -> BytesMut {

    let data_length = raw_byte.len() as u32;
    let mut buf = BytesMut::with_capacity(raw_byte.len());
    let data_type = encoding_type.u8_value();
    let is_stream = if is_stream { 1u8  }else { 0u8 };

    // buf.put(&ENCODING_VERSION_1.to_be_bytes()[..]);
    buf.put(&ENCODING_DELIMITER[..]);
    buf.put_u8(ENCODING_VERSION_1);
    buf.put_u8(data_type);
    buf.put_u8(is_stream);
    buf.put_u32(data_length);
    buf.put(&raw_byte[..]);

    buf
}



#[cfg(test)]
mod tests {
    use std::io::Read;

    use bytes::{BufMut, BytesMut};
    use super::{create_v1_encoding, create_v1_stream_encoding, DataEncodingType, Encoding, ENCODING_DELIMITER, EncodingError, EncodingState};

    #[test]
    fn it_works() {

        // delimiter(32bytes)|version(1byte)|data_type(1byte)|is_stream(1byte)|data_length(32byte)|data)(data_length bytes)

        let mut delimiter = b"!CZ~";
        let mut delimiter_length = delimiter.len();
        let mut buf = BytesMut::with_capacity(1024);
        let is_stream = 0u8;
        let is_binary_data = 0u8;

        let raw_instruction = "Avenuer";
        let instruction = raw_instruction.as_bytes();

        let version = 255u8;
        let data_length = instruction.len() as u32;
        println!("raw_instruction: {:?}  | instruction: {:?}  | length={}", &raw_instruction, instruction, data_length);


        println!("delimiter: {:?}  | length={}", &delimiter, delimiter_length);

        let binding = version.to_be_bytes();
        let version_slice = binding.as_slice();
        println!("version: {:?}", version_slice);

        let binding = data_length.to_be_bytes();
        let data_length_slice = binding.as_slice();
        println!("data_length: {:?}", data_length_slice);

        buf.put(&delimiter[..]);
        buf.put_u8(version);
        buf.put_u8(is_binary_data);
        buf.put_u8(is_stream);
        buf.put_u32(data_length);
        buf.put(&instruction[..]);

        println!("buf: {:?}", buf);
        println!("buf_slice_operation: {:?}", &buf[..]);
        println!("buf_to_vec_to_slice: {:?}", buf.to_vec().as_slice());

        let mut cursor_start = 0;
        let mut cursor_end = delimiter_length;

        let mut delimiter = buf.get(cursor_start..cursor_end).unwrap();
        let mut version_str = "".to_string();
        delimiter.read_to_string(&mut version_str);
        // println!("delimiter: {:?}", delimiter.unwrap());
        println!("delimiter: {:?}", version_str);

        cursor_start = cursor_end;
        cursor_end = cursor_end + 1;

        let version = buf.get(cursor_start..cursor_end);
        println!("version: {:?}", version.unwrap());

        cursor_start = cursor_end;
        cursor_end = cursor_end + 1;

        let is_binary_data = buf.get(cursor_start..cursor_end);
        println!("is_binary_data: {:?}", is_binary_data.unwrap());

        cursor_start = cursor_end;
        cursor_end = cursor_end + 1;

        let is_stream = buf.get(cursor_start..cursor_end);
        println!("is_stream: {:?}", is_stream.unwrap());

        cursor_start = cursor_end;
        cursor_end = cursor_end + 4;

        let data_length = buf.get(cursor_start..cursor_end).unwrap();
        println!("data_length: {:?}", data_length);


        let data_length = u32::from_be_bytes(data_length.clone().try_into().unwrap());
        println!("parsed data_length= {}", data_length);

        cursor_start = cursor_end;
        cursor_end = cursor_end + (data_length as usize);

        let instruction = buf.get(cursor_start..cursor_end).unwrap();
        println!("instruction: {:?}", instruction);
        println!("instruction: {}", String::from_utf8_lossy(instruction));
    }

    #[test]
    fn it_should_return_corrupted_data_stream() {
        let mut raw_byte = ENCODING_DELIMITER.clone().to_vec();
        raw_byte.append(&mut vec![0u8, 0, 0, 7]);

        let mut encoding = Encoding {
            version: Some(1),
            data_length: Some(20),
            encoding_type: Some(DataEncodingType::BINARY),
            ..Default::default()
        };

        let mut binding = raw_byte.as_mut_slice();
        let result = encoding.read_from_stream(&mut binding);
        assert!(result.unwrap_err() == EncodingError::CorruptedDataStream);
    }

    #[test]
    fn it_should_return_data_stream_incomplete() {
        let mut raw_byte = vec![0u8, 0, 0, 7];
        let mut buf = raw_byte.as_mut_slice();

        let mut encoding = Encoding {
            version: Some(1),
            data_length: Some(200),
            encoding_type: Some(DataEncodingType::BINARY),
            ..Default::default()
        };

        let result = encoding.read_from_stream(buf);
        assert!(result.unwrap().0 == EncodingState::Incomplete);
    }

    #[test]
    fn it_should_return_data_stream_complete() {
        let mut raw_byte = vec![0u8, 0, 0, 7];
        let mut buf = raw_byte.as_mut_slice();

        let mut encoding = Encoding {
            version: Some(1),
            data_length: Some(4),
            encoding_type: Some(DataEncodingType::BINARY),
            ..Default::default()
        };

        let result = encoding.read_from_stream(buf);
        assert!(result.unwrap().0 == EncodingState::Complete);
    }

    #[test]
    fn it_should_return_corrupt_stream_for_stream_not_being_read_and_has_no_delimiter() {
        let mut raw_byte = vec![0u8, 0, 0, 7];
        let mut buf = raw_byte.as_mut_slice();

        let mut encoding = Encoding::default();

        let result = encoding.read_from_stream(buf);
        assert!(result.unwrap_err() == EncodingError::CorruptedDataStream);
    }

    #[test]
    fn it_should_read_complete_stream() {
        let mut byte_stream = vec![33, 67, 90, 126, 1, 0, 0, 0, 0, 0, 7, 65, 118, 101, 110, 117, 101, 114];
        let mut buf = &mut byte_stream;

        let mut encoding = Encoding::default();
        let result = encoding.read_from_stream(buf);

        dbg!(&encoding);
        assert!(result.unwrap().0 == EncodingState::Complete);
        assert!(encoding.is_complete());
    }

    #[test]
    fn it_should_read_continuous_stream() {
        let mut byte_stream = vec![33, 67, 90, 126, 1, 0, 0, 0, 0, 0, 7, 65, 118, 101, 110, 117, 101, 114];
        // The second stream is incomplete at this point, so it's supposed to have a partial read
        byte_stream.append(&mut vec![33u8, 67, 90, 126, 1, 0, 0, 0, 0, 0, 7, 65, 118, 101, 110 ]);

        let mut byte_stream = byte_stream.as_mut_slice();

        dbg!(&byte_stream.len());
        let mut first_encoding_read = Encoding::default();
        let result = first_encoding_read.read_from_stream(byte_stream);

        let result = result.unwrap();
        assert!(result.0 == EncodingState::Complete);
        assert!(first_encoding_read.is_complete());

        // decoding the second continuous stream
        let byte_stream = result.1.unwrap();
        // the second stream is 3 bytes short
        let mut second_encoding_read = Encoding::default();
        let result = second_encoding_read.read_from_stream(byte_stream)
            .unwrap();

        assert!(result.0 == EncodingState::Incomplete);
        assert!(result.1.is_none());
        assert!(!second_encoding_read.is_complete());

        // The stream should be completed now after next 3 bytes
        let mut byte_stream = [117u8, 101, 114];
        let result = second_encoding_read.read_from_stream(&mut byte_stream);

        let result = result.unwrap();
        assert!(result.0 == EncodingState::Complete);
        assert!(second_encoding_read.is_complete());
    }

    #[test]
    fn it_should_create_v1_encoding() {
        use std::convert::AsMut;

        let byte_stream = "Avenuer".as_bytes();
        let mut encoding = Encoding::default();
        let mut encoded_bytes = create_v1_encoding(DataEncodingType::BINARY, byte_stream);

        dbg!(encoded_bytes.as_ref());
        let byte_stream = encoded_bytes.as_mut();
        let result = encoding.read_from_stream(byte_stream);

        assert!(!encoding.is_stream);
    }

    #[test]
    fn it_should_create_v1_stream_encoding() {
        use std::convert::AsMut;

        let byte_stream = "Avenuer".as_bytes();
        let mut encoding = Encoding::default();
        let mut encoded_bytes = create_v1_stream_encoding(DataEncodingType::BINARY, byte_stream);

        let byte_stream = encoded_bytes.as_mut();
        let result = encoding.read_from_stream(byte_stream);

        dbg!(&encoding);
        assert!(encoding.is_stream);
    }

    #[test]
    fn parse_byte_slice_to_number() {
        let value: [u8; 4] = [0, 0, 0, 7];
        let x = &value;

        let result = u32::from_be_bytes(x.clone().try_into().unwrap());
        println!("{}", result);
    }
}
