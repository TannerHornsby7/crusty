use common::ids::{PageId, SlotId};
use common::PAGE_SIZE;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Write;
use std::fs::File;
use std::hash::Hash;
use std::io::prelude::*;
use std::io::Write as IoWrite;
use std::io::{stdout, BufWriter};
use std::{fmt, option};

// Type to hold any value smaller than the size of a page.
// We choose u16 because it is sufficient to represent any slot that fits in a 4096-byte-sized page.
// Note that you will need to cast Offset to usize if you want to use it to index an array.
pub type Offset = u16;
// For debug
const BYTES_PER_LINE: usize = 40;

/// Page struct. This must occupy not more than PAGE_SIZE when serialized.
/// In the header, you are allowed to allocate 8 bytes for general page metadata and
/// 6 bytes per value/entry/slot stored. For example a page that has stored 3 values, can use
/// up to 8+3*6=26 bytes, leaving the rest (PAGE_SIZE-26 for data) when serialized.
/// If you delete a value, you do not need reclaim header space the way you must reclaim page
/// body space. E.g., if you insert 3 values then delete 2 of them, your header can remain 26
/// bytes & subsequent inserts can simply add 6 more bytes to the header as normal.
/// The rest must filled as much as possible to hold values.
///
/// I built own struct, header, to hold information about the page
///
pub struct Header {
    p_id: PageId,                                // 2 bytes
    open_slot: Option<SlotId>, // None if no open slots, if open_slot not in hash_map, its length and index is given by remaining space.
    slot_map: HashMap<SlotId, (Offset, Offset)>, // slot id maps to its index and its size (6 bytes per entry)
    s_space: Offset, // allocated space for slots ** May have to get rid of this since we need bitmap for deletes**
                     // or just don't write this var when we serialize but derive it from the hashmap
}
pub(crate) struct Page {
    // the metadata for a given page
    header: Header,
    // the records for a given page
    data: [u8; PAGE_SIZE],
}

/// The functions required for page
impl Page {
    /*
    HELPERS
    */

    /*
    HELPER: Find Next Slot
    DESCRIPTION: This function finds the next available slot id for a given page.
                It returns the slot id if there is an available slot id.
                It returns None if there is no available slot id.
    NOTES:      - This function is called by append and delete to assign the new open
                - slot after operation is performed.
    */
    #[allow(dead_code)]
    pub fn find_next_slot(&self) -> Option<SlotId> {
        let slot_map = &self.header.slot_map;
        // find the minimum next open slot id and if none are open, then
        // use the max slot id + 1 if there is space otherwise return None
        // a slot id is open if its correlated tuple has a length value of 0
        let mut min = SlotId::max_value();
        let mut max = SlotId::min_value();
        let mut deleted = false;
        // iterate through the hashmap and find the min deleted slot id and max slot id
        for (slot_id, (_idx, len)) in slot_map.iter() {
            if *len == 0 {
                if *slot_id < min {
                    min = *slot_id;
                }
                deleted = true;
            }
            if *slot_id > max {
                max = *slot_id;
            }
        }
        // if there is a deleted slot, return the min deleted slot id
        if deleted {
            return Some(min);
        }
        // if there is no deleted slot, return the max slot id + 1 if there is space
        // otherwise return None
        if max + 1 < SlotId::max_value() {
            return Some(max + 1);
        }
        None
    }

    /*

    HELPER: Append Slot
    DESCRIPTION: This function appends a slot to the end page.
                It returns the slot id of the value if the insertion was successful.
                It returns None if the insertion was not successful.
    */
    #[allow(dead_code)]
    fn append_slot(&mut self, slot_id: SlotId, bytes: &[u8]) -> Option<SlotId> {
        // get the end bound of the value as usize for array slice
        let j = PAGE_SIZE - self.header.s_space as usize;

        // get the end index of the value for tuple
        let e_idx = PAGE_SIZE as Offset - self.header.s_space - 1;
        // get the length of the value as offset for tuple
        let len = bytes.len() as Offset;

        // get the start index of the value using j and len as usize
        let i = j - len as usize;

        // if the value doesn't fit, return None, as no insertion can occur
        // no need to check upperbound since s_space is unsigned int

        // also need to check if there is enough space to add a slot id
        // if slot_id isn't in the hashmap already
        // that is what the - 6 is for
        if i - 6 < self.get_header_size() {
            return None;
        }

        // insert the value into the page
        self.data[i..j].clone_from_slice(bytes);

        // make sure you reuse old slot id's by using a for loop to
        // iterate through the hashmap and finding keys where the associated
        // size is 0

        // insert the slot id with tuple into the hashmap
        self.header.slot_map.insert(slot_id, (e_idx, len));

        // set the next slot based on the current slot_map
        self.header.open_slot = self.find_next_slot();

        // update the s_space length to include the added slot length
        self.header.s_space += len;

        // print the page
        // println!("Page after append: {:?}", self);

        // return the slot id
        Some(slot_id)
    }

    /*
        HELPER: FIRST_SPACE
        DESCRIPTION: this function finds the first open space in that data byte array and
                    returns it's index
        NOTES:
    */
    #[allow(dead_code)]
    pub fn helper_first_space(&self) -> usize {
        (PAGE_SIZE - 1) - self.header.s_space as usize
    }

    /*
    END OF HELPERS
    */
    /// Create a new page
    #[allow(dead_code)]
    pub fn new(page_id: PageId) -> Self {
        let header = Header {
            p_id: page_id,
            open_slot: Some(0),       // since 0 is the first id the tests expect
            slot_map: HashMap::new(), // empty bitmap takes up no space
            s_space: 0,
        };

        Page {
            // header will be placed into data when serialized
            header,
            // initialize page to all zeros
            data: [0; PAGE_SIZE],
        }
    }

    /// Return the page id for a page
    #[allow(dead_code)]
    pub fn get_page_id(&self) -> PageId {
        self.header.p_id
    }

    /// Attempts to add a new value to this page if there is space available.
    /// Returns Some(SlotId) if it was inserted or None if there was not enough space.
    /// Note that where the bytes are stored in the page does not matter (heap), but it
    /// should not change the slotId for any existing value. This means that
    /// bytes in the page may not follow the slot order.
    /// If a slot is deleted you should reuse the slotId in the future.
    /// The page should always assign the lowest available slot_id to an insertion.
    ///
    /// HINT: You can copy/clone bytes into a slice using the following function.
    /// They must have the same size.
    /// self.data[X..y].clone_from_slice(&bytes);
    ///

    #[allow(dead_code)]
    pub fn add_value(&mut self, bytes: &[u8]) -> Option<SlotId> {
        //header.slot_map.insert(0, (SIZE_OFFSET - 1, 0)); // can't do this
        if bytes.is_empty() || self.get_free_space() < bytes.len() {
            // works since we compact after each deletion
            return None;
        }

        // if the open_slot is None, page is full
        let open_slot = self.header.open_slot;
        if open_slot.is_none() {
            println!("Page Full!");
            return None;
        }

        // if the open_slot is not in the hashmap, then it should be appended
        self.append_slot(open_slot.unwrap(), bytes)
    }

    /// Return the bytes for the slotId. If the slotId is not valid then return None
    pub fn get_value(&self, slot_id: SlotId) -> Option<Vec<u8>> {
        // if there are no insertions, return none
        if self.header.slot_map.is_empty() {
            return None;
        }
        // get the Optional tuple from the given slotid
        let tuple = self.header.slot_map.get(&slot_id);
        if tuple.is_some() {
            // if there is some tuple, then spit out value
            let (idx, len) = *self.header.slot_map.get(&slot_id).unwrap();
            if len == 0 {
                return None;
            }
            let j = idx as usize;
            let i: usize = j - len as usize + 1;
            //second index of slice is non-inclusive
            Some(self.data[i..j + 1].to_vec())
        } else {
            None
        }
    }

    /// Delete the bytes/slot for the slotId. If the slotId is not valid then return None
    /// The slotId for a deleted slot should be assigned to the next added value
    /// The space for the value should be free to use for a later added value.
    /// HINT: Return Some(()) for a valid delete
    #[allow(dead_code)]
    pub fn delete_value(&mut self, slot_id: SlotId) -> Option<()> {
        // request the tuple from the slotmap
        let tuple = self.header.slot_map.get(&slot_id);
        // if its non-existent, then no delete can occur
        tuple?;
        // otherwise we can delete by moving the rest of the array down
        // by length of the slot
        let data_start = self.get_header_size();
        let data_end = (tuple?.0 - tuple?.1) as usize + 1;

        let len = tuple?.1 as usize;
        // copy slice of data[start to end] to data[start + len to end + len]
        let moved_data = &self.data[data_start..data_end];
        let copy = moved_data.to_vec();

        self.data[(data_start + len)..(data_end + len)].clone_from_slice(&copy);
        // iterate through length of usize setting data[start + i] = 0
        for i in 0..len {
            self.data[data_start + i] = 0;
        }

        // update hashmap indices accordingly
        for tuple in self.header.slot_map.values_mut() {
            if tuple.0 < data_end as Offset {
                tuple.0 += len as Offset; // Update the value using a mutable reference
            }
        }

        // set the length of the deleted id to zero in the hm
        self.header.slot_map.insert(slot_id, (0, 0));

        // check if theres enough space, if so, assign openslot to deleted slot
        // otherwise, set open_slot to none
        self.header.open_slot = self.find_next_slot();

        // update the s_size by removing the previous length
        self.header.s_space -= len as Offset;

        // print the page
        // println!("Page after delete: {:?}", self);
        Some(())
    }

    /// Deserialize bytes into Page
    ///
    /// HINT to create a primitive data type from a slice you can use the following
    /// (the example is for a u16 type and the data store in little endian)
    /// u16::from_le_bytes(data[X..Y].try_into().unwrap());
    #[allow(dead_code)]
    pub fn from_bytes(data: &[u8]) -> Self {
        //first 8 bytes are fixed elements of the header
        // - data[0..2] = p_id
        // - data[2..5] = option open_slot
        // - data[5..7] = num_slots
        // - data[7..(7 + 6*num_slots)] = hashmap (each 6 bytes is a new entry)
        // DATA
        // to get the data from the byte array, we simply copy the byte array
        // into the struct.data
        // - data[6 + num_slots .. PAGE_SIZE-1] = values
        //

        // pull in basic info from data to local variables following
        // schema
        let p_id = u16::from_le_bytes(data[0..2].try_into().unwrap());
        // option data
        let none = data[2];
        let open_slot = u16::from_le_bytes(data[3..5].try_into().unwrap());
        // this value is stored but not represented in our page struct
        let num_slots = u16::from_le_bytes(data[5..7].try_into().unwrap());
        let mut s_space = 0;
        let mut slot_map = HashMap::new();
        // set page's open slot
        let mut option_open_slot = None;
        if none == 1 {
            // 1 means something
            option_open_slot = Some(open_slot);
        }

        // iterate through bytes using num_slots inserting vals into slot_map
        for i in 0..num_slots {
            let idx = 7 + 6 * i as usize;
            let key = u16::from_le_bytes(data[idx..(idx + 2)].try_into().unwrap());
            let eidx = u16::from_le_bytes(data[(idx + 2)..(idx + 4)].try_into().unwrap());
            let len = u16::from_le_bytes(data[(idx + 4)..(idx + 6)].try_into().unwrap());
            slot_map.insert(key, (eidx, len));
        }

        for (_key, tuple) in slot_map.clone() {
            s_space += tuple.1;
        }

        // construct page
        let header = Header {
            p_id,
            open_slot: option_open_slot, // since 0 is the first id the tests expect
            slot_map,                    // empty bitmap takes up no space
            s_space,
        };
        let mut data_trait: [u8; PAGE_SIZE] = [0; PAGE_SIZE];
        let len = data.len();
        data_trait[0..len].clone_from_slice(data);

        Page {
            // header will be placed into data when serialized
            header,
            // initialize page to all zeros
            data: data_trait,
        }
    }

    /// Serialize page into a byte array. This must be same size as PAGE_SIZE.
    /// We use a Vec<u8> for simplicity here.
    ///
    /// HINT: To convert a vec of bytes using little endian, use
    /// to_le_bytes().to_vec()
    /// HINT: Do not use the self debug ({:?}) in this function, which calls this function.
    pub fn to_bytes(&self) -> Vec<u8> {
        // pack header into data
        // determine number of slots and write to data
        // turn data into vector byte vector and return
        let mut res_arr = [0; PAGE_SIZE];
        res_arr.clone_from_slice(&self.data);

        res_arr[0..2].clone_from_slice(&(self.header.p_id.to_le_bytes()));
        res_arr[2] = 1; // 1 means Some
        if self.header.open_slot.is_none() {
            res_arr[2] = 0; // 0 means None
        }

        res_arr[3..5].clone_from_slice(&(self.header.open_slot.unwrap().to_le_bytes()));

        res_arr[5..7].clone_from_slice(&((self.header.slot_map.len() as Offset).to_le_bytes()));

        // order the hashmap by key values so that it is deterministic in its
        // serialization
        let map = &self.header.slot_map;
        let mut keys: Vec<u16> = self.header.slot_map.keys().cloned().collect();
        keys.sort();

        //place the hashmap
        let mut idx = 7;

        for key in keys {
            res_arr[idx..(idx + 2)].clone_from_slice(&key.to_le_bytes());
            res_arr[(idx + 2)..(idx + 4)].clone_from_slice(&map[&key].0.to_le_bytes());
            res_arr[(idx + 4)..(idx + 6)].clone_from_slice(&map[&key].1.to_le_bytes());

            /*

            let key = u16::from_le_bytes(data[idx..(idx+2)].try_into().unwrap());
            let eidx = u16::from_le_bytes(data[(idx + 2)..(idx + 4)].try_into().unwrap());
            let len = u16::from_le_bytes(data[(idx + 4)..(idx + 6)].try_into().unwrap());

            */

            idx += 6
        }

        res_arr.to_vec()
    }

    /// A utility function to determine the size of the header in the page
    /// when serialized/to_bytes.
    /// Will be used by tests. Optional for you to use in your code
    #[allow(dead_code)]
    pub(crate) fn get_header_size(&self) -> usize {
        /*
        since each element in the vector is 2 bytes, the total space taken by the
        header is 2 * size of vector.
         */
        6 * self.header.slot_map.len()
            + self.header.p_id.to_le_bytes().len()
            + serde_cbor::to_vec(&self.header.open_slot).unwrap().len()
            + self.header.s_space.to_le_bytes().len()
    }

    /// A utility function to determine the total current free space in the page.
    /// This should account for the header space used and space that could be reclaimed if needed.
    /// Will be used by tests. Optional for you to use in your code, but strongly suggested
    #[allow(dead_code)]
    pub(crate) fn get_free_space(&self) -> usize {
        PAGE_SIZE - self.get_header_size() - self.header.s_space as usize
    }

    /// Utility function for comparing the bytes of another page.
    /// Returns a vec  of Offset and byte diff
    #[allow(dead_code)]
    pub fn compare_page(&self, other_page: Vec<u8>) -> Vec<(Offset, Vec<u8>)> {
        let mut res = Vec::new();
        let bytes = self.to_bytes();
        assert_eq!(bytes.len(), other_page.len());
        let mut in_diff = false;
        let mut diff_start = 0;
        let mut diff_vec: Vec<u8> = Vec::new();
        for (i, (b1, b2)) in bytes.iter().zip(&other_page).enumerate() {
            if b1 != b2 {
                if !in_diff {
                    diff_start = i;
                    in_diff = true;
                }
                diff_vec.push(*b1);
            } else if in_diff {
                //end the diff
                res.push((diff_start as Offset, diff_vec.clone()));
                diff_vec.clear();
                in_diff = false;
            }
        }
        res
    }
}

/// The (consuming) iterator struct for a page.
/// This should iterate through all valid values of the page.
pub struct PageIntoIter {
    page: Page,
    next_slot: SlotId,
    max_slot: SlotId,
}

/// The implementation of the (consuming) page iterator.
/// This should return the values in slotId order (ascending)
impl Iterator for PageIntoIter {
    // Each item returned by the iterator is the bytes for the value and the slot id.
    type Item = (Vec<u8>, SlotId);

    fn next(&mut self) -> Option<Self::Item> {
        // if next_slot is greater than max_slot, return None
        if self.next_slot > self.max_slot {
            return None;
        }
        // otherwise, get the tuple from the slot_map if the second value is not 0
        // if it is 0, move to next slot and get that tuple unless we exceed max slot
        let slot_id = self.next_slot;
        let wrapped_tuple = self.page.header.slot_map.get(&slot_id);
        // if key is not in slot_map, then we want to skip this slot
        if wrapped_tuple.is_none() {
            self.next_slot += 1;
            return self.next();
        }
        // otherwise, if it is in the slotmap, but its deleted then we also want
        // to skip it
        let tuple = wrapped_tuple.unwrap();
        if tuple.1 == 0 {
            // we want to skip this slot
            self.next_slot += 1;
            return self.next();
        }
        // if its non-zero, then we have a valid slot and want to return the
        // byte array for it
        let val = self.page.get_value(slot_id).unwrap();

        // get next slot id by checkinig the slot map and the prev_slots
        self.next_slot += 1;
        Some((val, slot_id))
    }
}

/// The implementation of IntoIterator which allows an iterator to be created
/// for a page. This should create the PageIter struct with the appropriate state/metadata
/// on initialization.
impl IntoIterator for Page {
    type Item = (Vec<u8>, SlotId);
    type IntoIter = PageIntoIter;

    fn into_iter(self) -> Self::IntoIter {
        PageIntoIter {
            max_slot: self.header.slot_map.len() as SlotId,
            page: self,
            next_slot: 0,
        }
    }
}

impl fmt::Debug for Page {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        //let bytes: &[u8] = unsafe { any_as_u8_slice(&self) };
        let p = self.to_bytes();
        let mut buffer = String::new();
        let len_bytes = p.len();

        // If you want to add a special header debugger to appear before the bytes add it here
        buffer += "Header: \n";

        let mut pos = 0;
        let mut remaining;
        let mut empty_lines_count = 0;
        let comp = [0; BYTES_PER_LINE];
        //hide the empty lines
        while pos < len_bytes {
            remaining = len_bytes - pos;
            if remaining > BYTES_PER_LINE {
                let pv = &(p)[pos..pos + BYTES_PER_LINE];
                if pv.eq(&comp) {
                    empty_lines_count += 1;
                    pos += BYTES_PER_LINE;
                    continue;
                }
                if empty_lines_count != 0 {
                    write!(&mut buffer, "{} ", empty_lines_count).unwrap();
                    buffer += "empty lines were hidden\n";
                    empty_lines_count = 0;
                }
                // for hex offset
                write!(&mut buffer, "[{:4}] ", pos).unwrap();
                #[allow(clippy::needless_range_loop)]
                for i in 0..BYTES_PER_LINE {
                    match pv[i] {
                        0x00 => buffer += ".  ",
                        0xff => buffer += "## ",
                        _ => write!(&mut buffer, "{:02x} ", pv[i]).unwrap(),
                    };
                }
            } else {
                let pv = &(p.clone())[pos..pos + remaining];
                if pv.eq(&comp) {
                    empty_lines_count += 1;
                    pos += BYTES_PER_LINE;
                    continue;
                }
                if empty_lines_count != 0 {
                    write!(&mut buffer, "{} ", empty_lines_count).unwrap();
                    buffer += "empty lines were hidden\n";
                    empty_lines_count = 0;
                }
                // for hex offset
                //buffer += &format!("[0x{:08x}] ", pos);
                write!(&mut buffer, "[{:4}] ", pos).unwrap();
                #[allow(clippy::needless_range_loop)]
                for i in 0..remaining {
                    match pv[i] {
                        0x00 => buffer += ".  ",
                        0xff => buffer += "## ",
                        _ => write!(&mut buffer, "{:02x} ", pv[i]).unwrap(),
                    };
                }
            }
            buffer += "\n";
            pos += BYTES_PER_LINE;
        }
        if empty_lines_count != 0 {
            write!(&mut buffer, "{} ", empty_lines_count).unwrap();
            buffer += "empty lines were hidden\n";
        }
        write!(f, "{}", buffer)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::fmt::write;
    use std::fmt::Debug;

    use super::*;
    use common::testutil::init;
    use common::testutil::*;
    use common::Tuple;
    use rand::Rng;

    /// Limits how on how many bytes we can use for page metadata / header
    pub const FIXED_HEADER_SIZE: usize = 8;
    pub const HEADER_PER_VAL_SIZE: usize = 6;

    #[test]
    fn hs_page_create() {
        init();
        let p = Page::new(0);
        assert_eq!(0, p.get_page_id());
        assert_eq!(PAGE_SIZE - p.get_header_size(), p.get_free_space());
    }

    #[test]
    fn debug_page_insert() {
        init();
        let mut p = Page::new(0);
        let n = 20;
        let size = 20;
        let vals = get_ascending_vec_of_byte_vec_02x(n, size, size);
        for x in &vals {
            p.add_value(x);
        }
        assert_eq!(
            p.get_free_space(),
            PAGE_SIZE - p.get_header_size() - n * size
        );
    }

    #[test]
    fn hs_page_simple_insert() {
        init();
        let mut p = Page::new(0);
        let tuple = int_vec_to_tuple(vec![0, 1, 2]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        let byte_len = tuple_bytes.len();
        assert_eq!(Some(0), p.add_value(&tuple_bytes));
        assert_eq!(
            PAGE_SIZE - byte_len - p.get_header_size(),
            p.get_free_space()
        );
        let tuple_bytes2 = serde_cbor::to_vec(&tuple).unwrap();
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - byte_len - byte_len,
            p.get_free_space()
        );
    }

    #[test]
    fn hs_page_space() {
        init();
        let mut p = Page::new(0);
        let size = 10;
        let bytes = get_random_byte_vec(size);
        assert_eq!(10, bytes.len());
        assert_eq!(Some(0), p.add_value(&bytes));
        assert_eq!(PAGE_SIZE - p.get_header_size() - size, p.get_free_space());
        assert_eq!(Some(1), p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 2,
            p.get_free_space()
        );
        assert_eq!(Some(2), p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 3,
            p.get_free_space()
        );
    }

    #[test]
    fn hs_page_get_value() {
        init();
        let mut p = Page::new(0);
        let tuple = int_vec_to_tuple(vec![0, 1, 2]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        assert_eq!(Some(0), p.add_value(&tuple_bytes));
        let check_bytes = p.get_value(0).unwrap();
        let check_tuple: Tuple = serde_cbor::from_slice(&check_bytes).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        assert_eq!(tuple, check_tuple);

        let tuple2 = int_vec_to_tuple(vec![3, 3, 3]);
        let tuple_bytes2 = serde_cbor::to_vec(&tuple2).unwrap();
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));
        let check_bytes2 = p.get_value(1).unwrap();
        let check_tuple2: Tuple = serde_cbor::from_slice(&check_bytes2).unwrap();
        assert_eq!(tuple_bytes2, check_bytes2);
        assert_eq!(tuple2, check_tuple2);

        //Recheck
        let check_bytes2 = p.get_value(1).unwrap();
        let check_tuple2: Tuple = serde_cbor::from_slice(&check_bytes2).unwrap();
        assert_eq!(tuple_bytes2, check_bytes2);
        assert_eq!(tuple2, check_tuple2);
        let check_bytes = p.get_value(0).unwrap();
        let check_tuple: Tuple = serde_cbor::from_slice(&check_bytes).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        assert_eq!(tuple, check_tuple);

        //Check that invalid slot gets None
        assert_eq!(None, p.get_value(2));
    }

    #[test]
    fn hs_page_header_size_small() {
        init();
        // Testing that the header is no more than 8 bytes for the header, and 6 bytes per value inserted
        let mut p = Page::new(0);
        println!("{:?}", p.get_header_size());
        assert!(p.get_header_size() <= FIXED_HEADER_SIZE);
        let bytes = get_random_byte_vec(10);
        assert_eq!(Some(0), p.add_value(&bytes));
        assert!(p.get_header_size() <= FIXED_HEADER_SIZE + HEADER_PER_VAL_SIZE);
        assert_eq!(Some(1), p.add_value(&bytes));
        assert_eq!(Some(2), p.add_value(&bytes));
        assert_eq!(Some(3), p.add_value(&bytes));
        assert!(p.get_header_size() <= FIXED_HEADER_SIZE + HEADER_PER_VAL_SIZE * 4);
    }

    #[test]
    fn hs_page_header_size_full() {
        init();
        // Testing that the header is no more than 8 bytes for the header, and 6 bytes per value inserted
        let mut p = Page::new(0);
        assert!(p.get_header_size() <= FIXED_HEADER_SIZE);
        let byte_size = 10;
        let bytes = get_random_byte_vec(byte_size);
        // how many vals can we hold with 8 bytes
        let num_vals: usize = (((PAGE_SIZE - FIXED_HEADER_SIZE) as f64
            / (byte_size + HEADER_PER_VAL_SIZE) as f64)
            .floor()) as usize;
        if PAGE_SIZE == 4096 && FIXED_HEADER_SIZE == 8 && HEADER_PER_VAL_SIZE == 6 {
            assert_eq!(255, num_vals);
        }
        for _ in 0..num_vals {
            p.add_value(&bytes);
        }
        assert!(p.get_header_size() <= FIXED_HEADER_SIZE + (num_vals * HEADER_PER_VAL_SIZE));
        assert!(
            p.get_free_space()
                >= PAGE_SIZE
                    - (byte_size * num_vals)
                    - FIXED_HEADER_SIZE
                    - (num_vals * HEADER_PER_VAL_SIZE)
        );
    }

    #[test]
    fn hs_page_no_space() {
        init();
        let mut p = Page::new(0);
        let size = PAGE_SIZE / 4;
        let bytes = get_random_byte_vec(size);
        assert_eq!(Some(0), p.add_value(&bytes));
        assert_eq!(PAGE_SIZE - p.get_header_size() - size, p.get_free_space());
        assert_eq!(Some(1), p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 2,
            p.get_free_space()
        );
        assert_eq!(Some(2), p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 3,
            p.get_free_space()
        );
        //Should reject here
        assert_eq!(None, p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 3,
            p.get_free_space()
        );
        // Take small amount of data
        let small_bytes = get_random_byte_vec(size / 4);
        assert_eq!(Some(3), p.add_value(&small_bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 3 - small_bytes.len(),
            p.get_free_space()
        );
    }

    #[test]
    pub fn hs_page_get_first_free_space() {
        init();
        println!("Testing that the first free space is correct\n");
        let mut p = Page::new(0);
        let tuple = int_vec_to_tuple(vec![0, 1, 2]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        assert_eq!(Some(0), p.add_value(&tuple_bytes));
        let check_bytes = p.get_value(0).unwrap();
        let check_tuple: Tuple = serde_cbor::from_slice(&check_bytes).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        assert_eq!(tuple, check_tuple);
        // delete tuple
        p.delete_value(0);

        // check that after an arbitrary number of adds and deletes, the index
        // of free space matches our prediction
        assert_eq!(4095, p.helper_first_space());
        assert_eq!(Some(0), p.add_value(&[1, 1, 1]));
        assert_eq!(Some(1), p.add_value(&[1, 1]));
        assert_eq!(Some(2), p.add_value(&[1, 1, 1]));
        p.delete_value(1);
        assert_eq!(Some(1), p.add_value(&[1, 1, 1]));

        assert_eq!(4086, p.helper_first_space());
    }

    #[test]
    fn hs_page_simple_delete() {
        init();
        let mut p = Page::new(0);
        let tuple = int_vec_to_tuple(vec![0, 1, 2]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        assert_eq!(Some(0), p.add_value(&tuple_bytes));
        let check_bytes = p.get_value(0).unwrap();
        let check_tuple: Tuple = serde_cbor::from_slice(&check_bytes).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        assert_eq!(tuple, check_tuple);

        let tuple2 = int_vec_to_tuple(vec![3, 3, 3]);
        let tuple_bytes2 = serde_cbor::to_vec(&tuple2).unwrap();
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));
        let check_bytes2 = p.get_value(1).unwrap();
        let check_tuple2: Tuple = serde_cbor::from_slice(&check_bytes2).unwrap();
        assert_eq!(tuple_bytes2, check_bytes2);
        assert_eq!(tuple2, check_tuple2);

        //Delete slot 0
        assert_eq!(Some(()), p.delete_value(0));

        //Recheck slot 1
        let check_bytes2 = p.get_value(1).unwrap();
        let check_tuple2: Tuple = serde_cbor::from_slice(&check_bytes2).unwrap();
        assert_eq!(tuple_bytes2, check_bytes2);
        assert_eq!(tuple2, check_tuple2);

        //Verify slot 0 is gone
        assert_eq!(None, p.get_value(0));

        //Check that invalid slot gets None
        assert_eq!(None, p.get_value(2));

        //Delete slot 1
        assert_eq!(Some(()), p.delete_value(1));

        //Verify slot 0 is gone
        assert_eq!(None, p.get_value(1));
    }

    #[test]
    fn hs_page_delete_insert() {
        init();
        let mut p = Page::new(0);
        let tuple_bytes = get_random_byte_vec(20);
        let tuple_bytes2 = get_random_byte_vec(20);
        let tuple_bytes3 = get_random_byte_vec(20);
        let tuple_bytes4 = get_random_byte_vec(20);
        let tuple_bytes_big = get_random_byte_vec(40);
        let tuple_bytes_small1 = get_random_byte_vec(5);
        let tuple_bytes_small2 = get_random_byte_vec(5);

        //Add 3 values
        assert_eq!(Some(0), p.add_value(&tuple_bytes));
        let check_bytes = p.get_value(0).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));

        let check_bytes = p.get_value(1).unwrap();
        assert_eq!(tuple_bytes2, check_bytes);
        assert_eq!(Some(2), p.add_value(&tuple_bytes3));

        let check_bytes = p.get_value(2).unwrap();
        assert_eq!(tuple_bytes3, check_bytes);

        //Delete slot 1
        assert_eq!(Some(()), p.delete_value(1));
        //Verify slot 1 is gone
        assert_eq!(None, p.get_value(1));

        let check_bytes = p.get_value(0).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        let check_bytes = p.get_value(2).unwrap();
        assert_eq!(tuple_bytes3, check_bytes);

        //Insert same bytes, should go to slot 1
        assert_eq!(Some(1), p.add_value(&tuple_bytes4));

        let check_bytes = p.get_value(1).unwrap();
        assert_eq!(tuple_bytes4, check_bytes);

        //Delete 0
        assert_eq!(Some(()), p.delete_value(0));

        //Insert big, should go to slot 0 with space later in free block
        assert_eq!(Some(0), p.add_value(&tuple_bytes_big));

        //Insert small, should go to 3
        assert_eq!(Some(3), p.add_value(&tuple_bytes_small1));

        //Insert small, should go to new
        assert_eq!(Some(4), p.add_value(&tuple_bytes_small2));
    }

    #[test]
    fn hs_page_size() {
        init();
        let mut p = Page::new(2);
        let tuple = int_vec_to_tuple(vec![0, 1, 2]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        assert_eq!(Some(0), p.add_value(&tuple_bytes));

        let page_bytes = p.to_bytes();
        assert_eq!(PAGE_SIZE, page_bytes.len());
    }

    #[test]
    fn hs_page_simple_byte_serialize() {
        init();
        let mut p = Page::new(0);
        let tuple = int_vec_to_tuple(vec![0, 1, 2]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        assert_eq!(Some(0), p.add_value(&tuple_bytes));
        let tuple2 = int_vec_to_tuple(vec![3, 3, 3]);
        let tuple_bytes2 = serde_cbor::to_vec(&tuple2).unwrap();
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));

        //Get bytes and create from bytes
        let bytes = p.to_bytes();
        println!("{:?}", p);
        let mut p2 = Page::from_bytes(&bytes);
        assert_eq!(0, p2.get_page_id());

        //Check reads
        let check_bytes2 = p2.get_value(1).unwrap();
        println!("{:?}", check_bytes2);
        let check_tuple2: Tuple = serde_cbor::from_slice(&check_bytes2).unwrap();
        assert_eq!(tuple_bytes2, check_bytes2);
        assert_eq!(tuple2, check_tuple2);
        let check_bytes = p2.get_value(0).unwrap();
        let check_tuple: Tuple = serde_cbor::from_slice(&check_bytes).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        assert_eq!(tuple, check_tuple);

        //Add a new tuple to the new page
        let tuple3 = int_vec_to_tuple(vec![4, 3, 2]);
        let tuple_bytes3 = tuple3.to_bytes();
        assert_eq!(Some(2), p2.add_value(&tuple_bytes3));
        assert_eq!(tuple_bytes3, p2.get_value(2).unwrap());
        assert_eq!(tuple_bytes2, p2.get_value(1).unwrap());
        assert_eq!(tuple_bytes, p2.get_value(0).unwrap());
    }

    #[test]
    fn hs_page_iter() {
        init();
        let mut p = Page::new(0);
        let tuple = int_vec_to_tuple(vec![0, 0, 1]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        assert_eq!(Some(0), p.add_value(&tuple_bytes));

        let tuple2 = int_vec_to_tuple(vec![0, 0, 2]);
        let tuple_bytes2 = serde_cbor::to_vec(&tuple2).unwrap();
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));

        let tuple3 = int_vec_to_tuple(vec![0, 0, 3]);
        let tuple_bytes3 = serde_cbor::to_vec(&tuple3).unwrap();
        assert_eq!(Some(2), p.add_value(&tuple_bytes3));

        let tuple4 = int_vec_to_tuple(vec![0, 0, 4]);
        let tuple_bytes4 = serde_cbor::to_vec(&tuple4).unwrap();
        assert_eq!(Some(3), p.add_value(&tuple_bytes4));

        let tup_vec = vec![
            tuple_bytes.clone(),
            tuple_bytes2.clone(),
            tuple_bytes3.clone(),
            tuple_bytes4.clone(),
        ];
        let page_bytes = p.to_bytes();

        // Test iteration 1
        let mut iter = p.into_iter();
        assert_eq!(Some((tuple_bytes.clone(), 0)), iter.next());
        assert_eq!(Some((tuple_bytes2.clone(), 1)), iter.next());
        assert_eq!(Some((tuple_bytes3.clone(), 2)), iter.next());
        assert_eq!(Some((tuple_bytes4.clone(), 3)), iter.next());
        assert_eq!(None, iter.next());

        //Check another way
        let p = Page::from_bytes(&page_bytes);
        assert_eq!(Some(tuple_bytes.clone()), p.get_value(0));

        for (i, x) in p.into_iter().enumerate() {
            assert_eq!(tup_vec[i], x.0);
        }

        let p = Page::from_bytes(&page_bytes);
        let mut count = 0;
        for _ in p {
            count += 1;
        }
        assert_eq!(count, 4);

        //Add a value and check
        let mut p = Page::from_bytes(&page_bytes);
        assert_eq!(Some(4), p.add_value(&tuple_bytes));
        //get the updated bytes
        let page_bytes = p.to_bytes();
        count = 0;
        for _ in p {
            count += 1;
        }
        assert_eq!(count, 5);

        //Delete
        let mut p = Page::from_bytes(&page_bytes);
        p.delete_value(2);
        let mut iter = p.into_iter();
        assert_eq!(Some((tuple_bytes.clone(), 0)), iter.next());
        assert_eq!(Some((tuple_bytes2.clone(), 1)), iter.next());
        assert_eq!(Some((tuple_bytes4.clone(), 3)), iter.next());
        assert_eq!(Some((tuple_bytes.clone(), 4)), iter.next());
        assert_eq!(None, iter.next());
    }

    #[test]
    pub fn hs_page_test_delete_reclaim_same_size() {
        init();
        let size = 800;
        let values = get_ascending_vec_of_byte_vec_02x(6, size, size);
        let mut p = Page::new(0);
        assert_eq!(Some(0), p.add_value(&values[0]));
        assert_eq!(Some(1), p.add_value(&values[1]));
        assert_eq!(Some(2), p.add_value(&values[2]));
        assert_eq!(Some(3), p.add_value(&values[3]));
        assert_eq!(Some(4), p.add_value(&values[4]));
        assert_eq!(values[0], p.get_value(0).unwrap());
        assert_eq!(None, p.add_value(&values[0]));
        assert_eq!(Some(()), p.delete_value(1));
        assert_eq!(None, p.get_value(1));
        assert_eq!(Some(1), p.add_value(&values[5]));
        assert_eq!(values[5], p.get_value(1).unwrap());
    }

    #[test]
    pub fn hs_page_test_delete_reclaim_larger_size() {
        init();
        let size = 500;
        let values = get_ascending_vec_of_byte_vec_02x(8, size, size);
        let larger_val = get_random_byte_vec(size * 2 - 20);
        let mut p = Page::new(0);
        assert_eq!(Some(0), p.add_value(&values[0]));
        assert_eq!(Some(1), p.add_value(&values[1]));
        assert_eq!(Some(2), p.add_value(&values[2]));
        assert_eq!(Some(3), p.add_value(&values[3]));
        assert_eq!(Some(4), p.add_value(&values[4]));
        assert_eq!(Some(5), p.add_value(&values[5]));
        assert_eq!(Some(6), p.add_value(&values[6]));
        assert_eq!(Some(7), p.add_value(&values[7]));
        assert_eq!(values[5], p.get_value(5).unwrap());
        assert_eq!(None, p.add_value(&values[0]));
        assert_eq!(Some(()), p.delete_value(1));
        assert_eq!(None, p.get_value(1));
        assert_eq!(Some(()), p.delete_value(6));
        assert_eq!(None, p.get_value(6));
        assert_eq!(Some(1), p.add_value(&larger_val));
        assert_eq!(larger_val, p.get_value(1).unwrap());
    }

    #[test]
    pub fn hs_page_test_delete_reclaim_smaller_size() {
        init();
        let size = 800;
        let values = vec![
            get_random_byte_vec(size),
            get_random_byte_vec(size),
            get_random_byte_vec(size),
            get_random_byte_vec(size),
            get_random_byte_vec(size),
            get_random_byte_vec(size / 4),
        ];
        let mut p = Page::new(0);
        assert_eq!(Some(0), p.add_value(&values[0]));
        assert_eq!(Some(1), p.add_value(&values[1]));
        assert_eq!(Some(2), p.add_value(&values[2]));
        assert_eq!(Some(3), p.add_value(&values[3]));
        assert_eq!(Some(4), p.add_value(&values[4]));
        assert_eq!(values[0], p.get_value(0).unwrap());
        assert_eq!(None, p.add_value(&values[0]));
        assert_eq!(Some(()), p.delete_value(1));
        assert_eq!(None, p.get_value(1));
        assert_eq!(Some(1), p.add_value(&values[5]));
        assert_eq!(values[5], p.get_value(1).unwrap());
    }

    #[test]
    pub fn hs_page_test_multi_ser() {
        init();
        let size = 500;
        let values = vec![
            get_random_byte_vec(size),
            get_random_byte_vec(size),
            get_random_byte_vec(size),
            get_random_byte_vec(size),
            get_random_byte_vec(size),
            get_random_byte_vec(size),
            get_random_byte_vec(size),
            get_random_byte_vec(size),
        ];
        let mut p = Page::new(0);
        assert_eq!(Some(0), p.add_value(&values[0]));
        assert_eq!(Some(1), p.add_value(&values[1]));
        assert_eq!(Some(2), p.add_value(&values[2]));
        let bytes = p.to_bytes();
        let mut p2 = Page::from_bytes(&bytes);
        assert_eq!(values[0], p2.get_value(0).unwrap());
        assert_eq!(values[1], p2.get_value(1).unwrap());
        assert_eq!(values[2], p2.get_value(2).unwrap());
        assert_eq!(Some(3), p2.add_value(&values[3]));
        assert_eq!(Some(4), p2.add_value(&values[4]));

        let bytes2 = p2.to_bytes();
        let mut p3 = Page::from_bytes(&bytes2);
        assert_eq!(values[0], p3.get_value(0).unwrap());
        assert_eq!(values[1], p3.get_value(1).unwrap());
        assert_eq!(values[2], p3.get_value(2).unwrap());
        assert_eq!(values[3], p3.get_value(3).unwrap());
        assert_eq!(values[4], p3.get_value(4).unwrap());
        assert_eq!(Some(5), p3.add_value(&values[5]));
        assert_eq!(Some(6), p3.add_value(&values[6]));
        assert_eq!(Some(7), p3.add_value(&values[7]));
        assert_eq!(None, p3.add_value(&values[0]));

        let bytes3 = p3.to_bytes();
        let p4 = Page::from_bytes(&bytes3);
        assert_eq!(values[0], p4.get_value(0).unwrap());
        assert_eq!(values[1], p4.get_value(1).unwrap());
        assert_eq!(values[2], p4.get_value(2).unwrap());
        assert_eq!(values[7], p4.get_value(7).unwrap());
    }

    #[test]
    pub fn hs_page_stress_test() {
        init();
        let mut p = Page::new(23);
        let mut original_vals: VecDeque<Vec<u8>> =
            VecDeque::from_iter(get_ascending_vec_of_byte_vec_02x(300, 20, 100));
        let mut stored_vals: Vec<Vec<u8>> = Vec::new();
        let mut stored_slots: Vec<SlotId> = Vec::new();
        let mut has_space = true;
        let mut rng = rand::thread_rng();

        // Load up page until full
        while has_space {
            let bytes = original_vals
                .pop_front()
                .expect("ran out of data -- shouldn't happen");
            let slot = p.add_value(&bytes);
            match slot {
                Some(slot_id) => {
                    stored_vals.push(bytes);
                    stored_slots.push(slot_id);
                }
                None => {
                    // No space for this record, we are done. go ahead and stop. add back value
                    original_vals.push_front(bytes);
                    has_space = false;
                }
            };
        }
        // let (check_vals, check_slots): (Vec<Vec<u8>>, Vec<SlotId>) = p.into_iter().map(|(a, b)| (a, b)).unzip();
        let bytes = p.to_bytes();
        let p_clone = Page::from_bytes(&bytes);
        let mut check_vals: Vec<Vec<u8>> = p_clone.into_iter().map(|(a, _)| a).collect();
        assert!(compare_unordered_byte_vecs(&stored_vals, check_vals));
        trace!("\n==================\n PAGE LOADED - now going to delete to make room as needed \n =======================");
        // Delete and add remaining values until goes through all. Should result in a lot of random deletes and adds.
        while !original_vals.is_empty() {
            let bytes = original_vals.pop_front().unwrap();
            trace!("Adding new value (left:{}). Need to make space for new record (len:{}).\n - Stored_slots {:?}", original_vals.len(), &bytes.len(), stored_slots);
            let mut added = false;
            // debugging index
            while !added {
                let try_slot = p.add_value(&bytes);
                match try_slot {
                    Some(new_slot) => {
                        stored_slots.push(new_slot);
                        stored_vals.push(bytes.clone());
                        let bytes = p.to_bytes();
                        let p_clone = Page::from_bytes(&bytes);
                        check_vals = p_clone.into_iter().map(|(a, _)| a).collect();
                        assert!(compare_unordered_byte_vecs(&stored_vals, check_vals));
                        trace!("Added new value ({}) {:?}", new_slot, stored_slots);
                        added = true;
                    }
                    None => {
                        //Delete a random value and try again
                        let random_idx = rng.gen_range(0..stored_slots.len());
                        trace!(
                            "Deleting a random val to see if that makes enough space {}",
                            stored_slots[random_idx]
                        );
                        let value_id_to_del = stored_slots.remove(random_idx);
                        stored_vals.remove(random_idx);
                        p.delete_value(value_id_to_del)
                            .expect("Error deleting slot_id");
                        trace!("Stored vals left {}", stored_slots.len());
                    }
                }
            }
        }
    }
}
