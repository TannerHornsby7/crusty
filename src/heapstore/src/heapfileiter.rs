use crate::heapfile::HeapFile;
use crate::page::{PageIntoIter, self};
use common::prelude::*;
use std::sync::Arc;
use crate::page::Page;

#[allow(dead_code)]
/// The struct for a HeapFileIterator.
/// We use a slightly different approach for HeapFileIterator than
/// standard way of Rust's IntoIter for simplicity (avoiding lifetime issues).
/// This should store the state/metadata required to iterate through the file.
///
/// HINT: This will need an Arc<HeapFile>
pub struct HeapFileIterator {
    tid: TransactionId,
    hf: Arc<HeapFile>,     
    curr_pid: u16,
    curr_record_idx: u16,
}

/// Required HeapFileIterator functions
impl HeapFileIterator {
    /// Create a new HeapFileIterator that stores the tid, and heapFile pointer.
    /// This should initialize the state required to iterate through the heap file.
    pub(crate) fn new(tid: TransactionId, hf: Arc<HeapFile>) -> Self {
        HeapFileIterator {tid,
        hf,
        curr_pid: 0,
        curr_record_idx: 0,
        }
    }
}

/// Trait implementation for heap file iterator.
/// Note this will need to iterate through the pages and their respective iterators.
impl Iterator for HeapFileIterator {
    type Item = (Vec<u8>, ValueId);
    fn next(&mut self) -> Option<Self::Item> {
        if self.curr_pid < self.hf.num_pages() {
            // create page iterator local variable based on current page
            // we will use this to iterate through all values in the page
            let mut page_iterator = self.hf.read_page_from_file(self.curr_pid).unwrap().into_iter();

            // move to current record index
            for _ in 0..self.curr_record_idx {
                page_iterator.next();
            }
            
            if let Some((value, value_id)) = page_iterator.next() {
                let id = ValueId {
                    container_id: self.hf.container_id, 
                    segment_id: None, 
                    page_id: Some(self.curr_pid), 
                    slot_id: value_id.into()
                };
                // increment record index
                self.curr_record_idx += 1;
                return Some((value, id));
            } else {
                // reset record index and increment page id
                self.curr_record_idx = 0;
                self.curr_pid += 1;
                return self.next();
            }
        }
        None
    }
}

#[cfg(test)]
#[allow(unused_must_use)]
mod test {
    use super::*;
    use common::testutil::*;
    use temp_testdir::TempDir;

    #[test]
    fn hs_hf_iter() {
        init();

        //Create a temp file
        let f = gen_random_test_sm_dir();
        let tdir = TempDir::new(f, true);
        let mut f = tdir.to_path_buf();
        f.push(gen_rand_string(4));
        f.set_extension("hf");

        let mut hf = HeapFile::new(f.to_path_buf(), 0).expect("Unable to create HF for test");

        // Make a page and write
        let mut p0 = Page::new(0);
        let bytes1 = get_random_byte_vec(100);
        p0.add_value(&bytes1);
        let bytes2 = get_random_byte_vec(100);
        p0.add_value(&bytes2);
        let bytes3 = get_random_byte_vec(100);
        p0.add_value(&bytes3);
        let p0_bytes = p0.to_bytes();

        hf.write_page_to_file(p0);
        //check the page
        assert_eq!(1, hf.num_pages());
        let checkp0 = hf.read_page_from_file(0).unwrap();
        assert_eq!(p0_bytes, checkp0.to_bytes());

        //Add another page
        let mut p1 = Page::new(1);
        let bytes11 = get_random_byte_vec(100);
        p1.add_value(&bytes11);
        let bytes12 = get_random_byte_vec(100);
        p1.add_value(&bytes12);
        let bytes = get_random_byte_vec(100);
        p1.add_value(&bytes);
        let p1_bytes = p1.to_bytes();

        hf.write_page_to_file(p1);

        // create iterator
        let mut iter = HeapFileIterator::new(TransactionId::new(), Arc::new(hf));

        // check that each value matches the corresponding value in the page
        assert_eq!(iter.next().unwrap().0, bytes1);
        assert_eq!(iter.next().unwrap().0, bytes2);
        assert_eq!(iter.next().unwrap().0, bytes3);
        assert_eq!(iter.next().unwrap().0, bytes11);
        assert_eq!(iter.next().unwrap().0, bytes12);

    }
}
