use crate::attribute::*;
use crate::bitmap::*;
use crate::page::*;
use crate::paged_file::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::prelude::*;
use std::io::Cursor;
use std::io::Error;
use std::io::ErrorKind;
use std::io::Result;
use std::io::SeekFrom;
use std::path::Path;

// TODO - We should support configurable page size.
// For now, it's easiest to keep it const.
const PAGE_SIZE: usize = 8 * 1024;
type Pf = PagedFile<PAGE_SIZE>;
type P = Page<PAGE_SIZE>;

// The following are the minimum sizes for storing the header
const HDR_SIZE: usize = 12;
const RECORD_ENTRY_SIZE: usize = 8;

pub struct RecordBasedFileMgr {
    paged_file: PagedFile<PAGE_SIZE>,
    attributes: Vec<Attribute>,
}

#[derive(PartialEq, Debug)]
pub struct RecordId {
    pub page_num: u32,
    pub slot_num: u32,
}

#[derive(Serialize, Deserialize)]
struct SlotDirectoryRecordEntry {
    // Total length of the record
    length: u32,
    // Offset into the page of the start of the record
    offset: i32,
}

enum SlotStatus {
    Valid,
    Dead,
    Moved(RecordId),
}

impl SlotDirectoryRecordEntry {
    fn status(&self) -> SlotStatus {
        if self.length == 0 && self.offset == 0 {
            SlotStatus::Dead
        } else if self.offset < 0 {
            SlotStatus::Moved(RecordId {
                page_num: self.length,
                slot_num: -self.offset as u32,
            })
        } else {
            SlotStatus::Valid
        }
    }
}

// Header will begin at byte 0 of the page and grow forward
// Records will begin at the end of the page and grow backward
#[derive(Serialize, Deserialize)]
struct SlotDirectoryHeader {
    // Points to the first used byte
    data_start_offset: u32,
    // Vec stored as a size + the entries in a compact form
    // Using serdes just as space efficient, and easier to use than
    // custom serialize would be.
    slots_vec: Vec<SlotDirectoryRecordEntry>,
}

impl RecordBasedFileMgr {
    /// Create a new Record Based file at the given path.
    pub fn create(path: &Path, attributes: Vec<Attribute>) -> Result<Self> {
        let mut paged_file = Pf::create(path)?;
        let mut page = P::new();
        Self::init_rb_page(&mut page);
        paged_file.append_page(&page)?;
        Ok(Self {
            paged_file,
            attributes,
        })
    }

    // Open a Record Based file at the given path.
    pub fn open(path: &Path, attributes: Vec<Attribute>) -> Result<Self> {
        let paged_file = Pf::open(path)?;
        Ok(Self {
            paged_file,
            attributes,
        })
    }

    /// Insert a new record to store the values from insert_vals
    /// Returns the RecordId of the newly inserted record
    pub fn insert(&mut self, insert_vals: &HashMap<String, AttributeValue>) -> Result<RecordId> {
        let required_space = self.required_space(insert_vals);
        let num_pages = self.paged_file.num_pages()?;

        let mut page = P::new();
        let mut page_found = false;
        let mut page_num = 0;
        for i in 0..num_pages {
            self.paged_file.read_page(i, &mut page)?;
            let hdr = Self::get_slot_directory_hdr(&page);
            if Self::free_space(&hdr) < required_space {
                continue;
            }
            page_found = true;
            page_num = i;
        }

        if !page_found {
            page_num = num_pages;
            Self::init_rb_page(&mut page);
        }

        let mut slot_dir_hdr = Self::get_slot_directory_hdr(&page);
        let rid = RecordId {
            page_num: page_num as u32,
            slot_num: slot_dir_hdr.slots_vec.len() as u32,
        };

        let starting_offset = slot_dir_hdr.data_start_offset - required_space as u32;
        slot_dir_hdr.data_start_offset = starting_offset;
        slot_dir_hdr.slots_vec.push(SlotDirectoryRecordEntry {
            length: required_space as u32,
            offset: starting_offset as i32,
        });

        Self::write_slot_directory_hdr(&mut page, &slot_dir_hdr);

        // Struct fields are written to disk and should be fixed sizes
        // But it's easier to work with usizes for slicing
        let starting_offset = starting_offset as usize;

        let bytes_written = self
            .write_record_into_buf(
                &mut page.as_mut_buf()[starting_offset..starting_offset + required_space],
                insert_vals,
            )
            .unwrap();

        debug_assert_eq!(bytes_written, self.record_size(insert_vals).unwrap());

        if page_found {
            self.paged_file.write_page(page_num, &page)?;
        } else {
            self.paged_file.append_page(&page)?;
        }

        Ok(rid)
    }

    /// Reads the record with RecordId rid and returns a HashMap mapping
    /// attribute name to value.
    pub fn read(&mut self, rid: &RecordId) -> Result<HashMap<String, AttributeValue>> {
        let mut page = P::new();
        self.paged_file.read_page(rid.page_num as u64, &mut page)?;

        let hdr = Self::get_slot_directory_hdr(&page);
        if hdr.slots_vec.len() <= rid.slot_num as usize {
            return Err(Error::new(ErrorKind::InvalidInput, "Slot does not exist"));
        }

        let slot = hdr.slots_vec.get(rid.slot_num as usize).unwrap();
        return match slot.status() {
            SlotStatus::Dead => Err(Error::new(ErrorKind::InvalidData, "Record deleted")),
            SlotStatus::Moved(rid) => self.read(&rid),
            SlotStatus::Valid => self.read_record_from_buf(
                &page.as_buf()[slot.offset as usize..slot.offset as usize + slot.length as usize],
            ),
        };
    }

    /// Initialize a new Page for use by RBFM
    fn init_rb_page(page: &mut P) {
        // First we 0 out the buffer
        page.as_mut_buf().iter_mut().for_each(|i| *i = 0);
        // Then we write the header
        let hdr = SlotDirectoryHeader {
            data_start_offset: PAGE_SIZE as u32,
            slots_vec: vec![],
        };
        bincode::serialize_into(&mut page.as_mut_buf()[0..HDR_SIZE], &hdr).unwrap();
    }

    fn get_slot_directory_hdr(page: &P) -> SlotDirectoryHeader {
        bincode::deserialize(page.as_buf()).unwrap()
    }

    fn write_slot_directory_hdr(page: &mut P, hdr: &SlotDirectoryHeader) {
        bincode::serialize_into(&mut page.as_mut_buf()[..], &hdr).unwrap();
    }

    fn free_space(hdr: &SlotDirectoryHeader) -> usize {
        let hdr_size = bincode::serialized_size(hdr).unwrap() as usize;
        hdr.data_start_offset as usize - hdr_size
    }

    fn required_space(&self, insert_vals: &HashMap<String, AttributeValue>) -> usize {
        // Overhead:
        // SlotDirectoryRecordEntry: 8 bytes
        // Total
        // Overhead + Record size
        RECORD_ENTRY_SIZE + self.record_size(insert_vals).unwrap()
    }

    /// Calculate the length of the null bitmap in bytes
    fn null_bitmap_len(attrs_len: usize) -> usize {
        Bitmap::bmp_size_in_bytes(attrs_len as usize)
    }

    fn record_size(&self, insert_vals: &HashMap<String, AttributeValue>) -> Result<usize> {
        // Record Format:
        // Num_Attributes: 2 byte unsigned int
        // Null_Bitmap: Variable length bitmap, byte length is ceil(num_attributes / 8)
        // Offset_Headers: 2 bytes for each non-null attribute
        // Data: 4 bytes for each int, 8 bytes for each real, variable length varchar
        let bmp_len = Self::null_bitmap_len(self.attributes.len());

        let num_attributes_len: usize = 2;

        let mut offset_headers_len: usize = 0;
        let mut data_len: usize = 0;

        for attr in self.attributes.iter() {
            // No change in size for null values
            if !insert_vals.contains_key(&attr.name) {
                continue;
            }
            // All non-null fields have a 2 byte offset header
            offset_headers_len += 2;

            let attr_val = insert_vals.get(&attr.name).unwrap();
            if !Self::attribute_type_matches_value(&attr.attribute_type, attr_val) {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "Attribute type mismatch",
                ));
            }
            match attr_val {
                // All ints are 4 bytes
                AttributeValue::Int(_) => {
                    data_len += 4;
                }
                // All reals are 8 bytes
                AttributeValue::Real(_) => {
                    data_len += 8;
                }
                // Get the length of the actual string value
                AttributeValue::Varchar(val) => {
                    data_len += val.as_bytes().len();
                }
            }
        }

        Ok(bmp_len + num_attributes_len + offset_headers_len + data_len)
    }

    /// True if AttributeValue matches the expected type based on AttributeType
    fn attribute_type_matches_value(attr_type: &AttributeType, attr_val: &AttributeValue) -> bool {
        match attr_type {
            AttributeType::Int => {
                matches!(attr_val, AttributeValue::Int(_))
            }
            AttributeType::Real => {
                matches!(attr_val, AttributeValue::Real(_))
            }
            AttributeType::Varchar { len: max } => match attr_val {
                AttributeValue::Varchar(s) => s.len() <= *max as usize,
                _ => false,
            },
        }
    }

    fn write_record_into_buf(
        &self,
        buf: &mut [u8],
        insert_vals: &HashMap<String, AttributeValue>,
    ) -> Result<usize> {
        // Record Format:
        // Num_Attributes: 2 byte unsigned int
        // Null_Bitmap: Variable length bitmap, byte length is ceil(num_attributes / 8)
        // Offset_Headers: 2 bytes for each non-null attribute
        // Data: 4 bytes for each int, 8 bytes for each real, variable length varchar
        let num_attributes = self.attributes.len() as u16;
        let bmp_len = Self::null_bitmap_len(self.attributes.len());
        let mut bmp = Bitmap::new(num_attributes as usize);
        let mut valid_cnt: usize = 0;

        let mut cursor = Cursor::new(buf);
        let mut bytes_written = 0;

        // First iteration, determine what is null/valid
        for (i, attr) in self.attributes.iter().enumerate() {
            if !insert_vals.contains_key(&attr.name) {
                continue;
            }
            bmp.set(i);
            valid_cnt += 1;
        }
        // At this point, our null bmp is ready
        // and we know the number of offset headers
        let mut offset_hdrs = vec![0_u16; valid_cnt as usize];

        // num_attributes = 2 bytes
        // + bmp_len in bytes
        // + 2 bytes for each valid entry
        let mut data_offset = 2 + bmp_len + valid_cnt * 2;
        cursor.seek(SeekFrom::Start(data_offset as u64))?;

        // idx is the non-null index
        let mut idx = 0;
        // i includes nulls
        for (i, attr) in self.attributes.iter().enumerate() {
            // Skip nulls
            if !bmp.get(i) {
                continue;
            }
            // Guaranteed to be valid because field is non-null
            let attr_val = insert_vals.get(&attr.name).unwrap();
            if !Self::attribute_type_matches_value(&attr.attribute_type, attr_val) {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "Attribute type mismatch",
                ));
            }
            // Convert the attribute value to bytes,
            // then write to the buffer
            let bytes = match attr_val {
                AttributeValue::Int(val) => val.to_le_bytes().to_vec(),
                AttributeValue::Real(val) => val.to_le_bytes().to_vec(),
                // Inverse is from_utf8 for reading
                AttributeValue::Varchar(val) => val.as_bytes().to_vec(),
            };
            cursor.write_all(&bytes[..]).unwrap();
            // Offset headers point to END of value
            // so we update the offset first, then the header
            data_offset += bytes.len();
            bytes_written += bytes.len();
            offset_hdrs[idx] = data_offset as u16;
            idx += 1;
        }

        // Write headers
        cursor.seek(SeekFrom::Start(0))?;

        let bytes = num_attributes.to_le_bytes();
        cursor.write_all(&bytes)?;
        bytes_written += bytes.len();

        let bytes = bmp.into_bytes();
        cursor.write_all(&bytes)?;
        bytes_written += bytes.len();

        for offset_hdr in offset_hdrs.iter() {
            let bytes = offset_hdr.to_le_bytes();
            cursor.write_all(&bytes)?;
            bytes_written += bytes.len();
        }

        Ok(bytes_written)
    }

    fn read_record_from_buf(&self, buf: &[u8]) -> Result<HashMap<String, AttributeValue>> {
        // Record Format:
        // Num_Attributes: 2 byte unsigned int
        // Null_Bitmap: Variable length bitmap, byte length is ceil(num_attributes / 8)
        // Offset_Headers: 2 bytes for each non-null attribute
        // Data: 4 bytes for each int, 8 bytes for each real, variable length varchar
        let mut cursor = Cursor::new(buf);
        let mut num_attributes_bytes = [0; 2];
        cursor.read_exact(&mut num_attributes_bytes)?;
        let num_attributes = u16::from_le_bytes(num_attributes_bytes);

        let bmp_len = Bitmap::bmp_size_in_bytes(num_attributes as usize);
        let mut bmp_vec = vec![0; bmp_len];
        cursor.read_exact(&mut bmp_vec)?;
        let bmp = Bitmap::new_with_vec(num_attributes as usize, bmp_vec);

        let mut offset_hdrs = Vec::<u16>::new();

        for i in 0..num_attributes {
            if bmp.get(i as usize) {
                let mut hdr_bytes = [0; 2];
                cursor.read_exact(&mut hdr_bytes)?;
                offset_hdrs.push(u16::from_le_bytes(hdr_bytes));
            }
        }

        let mut results = HashMap::new();

        // offset_idx skips nulls
        let mut offset_idx = 0;
        for (i, attr) in self.attributes.iter().enumerate() {
            // Skip nulls
            if !bmp.get(i) {
                continue;
            }
            let attr_val = match attr.attribute_type {
                AttributeType::Int => {
                    let mut int_bytes = [0; 4];
                    cursor.read_exact(&mut int_bytes)?;
                    AttributeValue::Int(i32::from_le_bytes(int_bytes))
                }
                AttributeType::Real => {
                    let mut real_bytes = [0; 8];
                    cursor.read_exact(&mut real_bytes)?;
                    AttributeValue::Real(f64::from_le_bytes(real_bytes))
                }
                AttributeType::Varchar { len: max } => {
                    let curr = cursor.position();
                    let end = *offset_hdrs.get(offset_idx).unwrap() as u64;
                    let str_len = (end - curr) as usize;
                    if str_len > max as usize {
                        return Err(Error::new(
                            ErrorKind::InvalidData,
                            "Varchar larger than max len",
                        ));
                    }
                    let mut str_bytes = vec![0; str_len];
                    cursor.read_exact(&mut str_bytes)?;
                    AttributeValue::Varchar(String::from_utf8(str_bytes).unwrap())
                }
            };
            results.insert(attr.name.clone(), attr_val);
            offset_idx += 1;
        }
        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn rbfm_create_test() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("testfile");
        // File should be created with no errors
        RecordBasedFileMgr::create(&file_path, vec![]).unwrap();

        // Opening the newly created file should work
        RecordBasedFileMgr::open(&file_path, vec![]).unwrap();

        // Creating the same file twice should cause an error
        assert!(RecordBasedFileMgr::create(&file_path, vec!()).is_err());
    }

    #[test]
    fn rbfm_serialize_test() {
        // We check that the serialized header matches the minimum size we'd expect
        let mut hdr = SlotDirectoryHeader {
            data_start_offset: PAGE_SIZE as u32,
            slots_vec: vec![],
        };
        let hdr_size = bincode::serialized_size(&hdr).unwrap() as usize;
        assert_eq!(hdr_size, HDR_SIZE);

        hdr.slots_vec.push(SlotDirectoryRecordEntry {
            length: 0,
            offset: 0,
        });
        let hdr_size = bincode::serialized_size(&hdr).unwrap() as usize;
        assert_eq!(hdr_size, HDR_SIZE + RECORD_ENTRY_SIZE);

        hdr.slots_vec.push(SlotDirectoryRecordEntry {
            length: 0,
            offset: 0,
        });
        let hdr_size = bincode::serialized_size(&hdr).unwrap() as usize;
        assert_eq!(hdr_size, HDR_SIZE + RECORD_ENTRY_SIZE * 2);
    }

    #[test]
    fn rbfm_insert_test() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("testfile");
        let attrs = vec![
            Attribute {
                name: "FirstName".to_string(),
                attribute_type: AttributeType::Varchar { len: 20 },
            },
            Attribute {
                name: "Age".to_string(),
                attribute_type: AttributeType::Int,
            },
            Attribute {
                name: "PowerLevel".to_string(),
                attribute_type: AttributeType::Real,
            },
        ];
        let mut file = RecordBasedFileMgr::create(&file_path, attrs).unwrap();

        let mut attr_vals = HashMap::new();
        attr_vals.insert(
            "FirstName".to_string(),
            AttributeValue::Varchar("Cow".to_string()),
        );
        attr_vals.insert("Age".to_string(), AttributeValue::Int(28));
        attr_vals.insert("PowerLevel".to_string(), AttributeValue::Real(8999.999));
        let rid = file.insert(&attr_vals).unwrap();
        assert_eq!(
            rid,
            RecordId {
                page_num: 0,
                slot_num: 0,
            }
        );

        let read_result = file.read(&rid).unwrap();
        assert_eq!(read_result, attr_vals);

        // Try with one null
        attr_vals.remove("Age");
        let rid = file.insert(&attr_vals).unwrap();
        assert_eq!(
            rid,
            RecordId {
                page_num: 0,
                slot_num: 1,
            }
        );

        let read_result = file.read(&rid).unwrap();
        assert_eq!(read_result, attr_vals);

        // Try with all null values
        let null_attr_vals: HashMap<String, AttributeValue> = HashMap::new();
        let rid = file.insert(&null_attr_vals).unwrap();
        assert_eq!(
            rid,
            RecordId {
                page_num: 0,
                slot_num: 2,
            }
        );

        let read_result = file.read(&rid).unwrap();
        assert_eq!(read_result, null_attr_vals);
        assert_ne!(read_result, attr_vals);
    }
}
