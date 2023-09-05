use crate::page::Page;
use common::prelude::*;
use common::PAGE_SIZE;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::{Arc, RwLock};

use std::io::BufWriter;
use std::io::{Seek, SeekFrom};

/// The struct for a heap file.  
///
/// HINT: You likely will want to design for interior mutability for concurrent accesses.
/// eg Arc<RwLock<>> on some internal members
///
/// HINT: You will probably not be able to serialize HeapFile, as it needs to maintain a link to a
/// File object, which cannot be serialized/deserialized/skipped by serde. You don't need to worry
/// about persisting read_count/write_count during serialization.
///
/// Your code should persist what information is needed to recreate the heapfile.
///
///

/*
FIXES

1) page slot map isn't stored in a specific order, so we can't just read the file and get the page slot map
    fix by ordering increasing. This is an update to our page serialization.
    - This fixed it!!!

2) may need to get rid of pg_cnt since it is not stored in the file
    - This fixed it!!!
*/
pub(crate) struct HeapFile {
    // implement locking
    lock: Arc<RwLock<File>>,
    // Track this HeapFile's container Id
    pub container_id: ContainerId,
    // The following are for profiling/ correctness checks
    pub read_count: AtomicU16,
    pub write_count: AtomicU16,
    // holds the pg_cnt
    pub pg_cnt: Arc<RwLock<u16>>,
}

/// HeapFile required functions
impl HeapFile {
    /// Create a new heapfile for the given path. Return Result<Self> if able to create.
    /// Errors could arise from permissions, space, etc when trying to create the file used by HeapFile.
    pub(crate) fn new(file_path: PathBuf, container_id: ContainerId) -> Result<Self, CrustyError> {
        fs::create_dir_all(file_path.parent().unwrap())?;
        let file = match OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&file_path)
        {
            Ok(f) => f,
            Err(error) => {
                return Err(CrustyError::CrustyError(format!(
                    "Cannot open or create heap file: {} {:?}",
                    file_path.to_string_lossy(),
                    error
                )))
            }
        };
        // get the initial page count from the file by using the fixed pg size
        // and the file size
        let pg_cnt = (file.metadata().unwrap().len() / PAGE_SIZE as u64) as u16;

        // read it from disk to finish storage
        // fix insert to finish project

        Ok(HeapFile {
            lock: Arc::new(RwLock::new(file)),
            container_id,
            read_count: AtomicU16::new(0),
            write_count: AtomicU16::new(0),
            pg_cnt: Arc::new(RwLock::new(pg_cnt)), // get rid of this to fix shutdown
        })
    }

    /// Return the number of pages for this HeapFile.
    /// Return type is PageId (alias for another type) as we cannot have more
    /// pages than PageId can hold.
    pub fn num_pages(&self) -> PageId {
        // return the number of pages in the file
        self.pg_cnt.read().unwrap().clone()
    }

    /// Read the page from the file.
    /// Errors could arise from the filesystem or invalid pageId
    /// Note: that std::io::{Seek, SeekFrom} require Write locks on the underlying std::fs::File
    pub(crate) fn read_page_from_file(&self, pid: PageId) -> Result<Page, CrustyError> {
        //If profiling count reads
        #[cfg(feature = "profile")]
        {
            self.read_count.fetch_add(1, Ordering::Relaxed);
        }
        // create write lock
        let mut f = self.lock.write().unwrap();
        f.seek(SeekFrom::Start(0))?; // seek to start of file

        // find the page in the file
        for i in 0..self.pg_cnt.read().unwrap().clone() {
            // seek to next page
            f.seek(SeekFrom::Start(i as u64 * PAGE_SIZE as u64))?;
            // create temp buffer to hold page data
            let mut buf = [0; PAGE_SIZE];
            // read page into buffer
            f.read_exact(&mut buf)?;
            // create page from buffer
            let page = Page::from_bytes(&buf);
            // check if page is the one we want
            if page.get_page_id() == pid {
                return Ok(page);
            }
        }

        // drop write lock
        drop(f);

        // return error if page not found
        Err(CrustyError::CrustyError(format!(
            "Cannot read page {} from file {}",
            pid, self.container_id
        )))
    }

    /// Take a page and write it to the underlying file.
    /// This could be an existing page or a new page
    pub(crate) fn write_page_to_file(&self, page: Page) -> Result<(), CrustyError> {
        trace!(
            "Writing page {} to file {}",
            page.get_page_id(),
            self.container_id
        );
        //If profiling count writes
        #[cfg(feature = "profile")]
        {
            self.write_count.fetch_add(1, Ordering::Relaxed);
        }
        // create write lock
        let mut f = self.lock.write().unwrap();
        f.seek(SeekFrom::Start(0))?; // seek to start of file

        // seek to page
        for i in 0..self.pg_cnt.read().unwrap().clone() {
            // seek to next page
            f.seek(SeekFrom::Start((i as u64) * (PAGE_SIZE as u64)))?;
            // create temp buffer to hold page data
            let mut buf = [0; PAGE_SIZE];

            // read page into buffer
            f.read_exact(&mut buf)?;

            // create page from buffer
            let mut p = Page::from_bytes(&buf);

            // check if page has matching id to the one we have
            if p.get_page_id() == page.get_page_id() {
                // if it does, write our page to this location in the file
                // and return
                // move back to correc position and write
                f.seek(SeekFrom::Start((i as u64) * (PAGE_SIZE as u64)))?;
                f.write_all(&page.to_bytes())?;

                // print that you wrote to the specified file in the filepath
                return Ok(());
            }
        }
        // if the page isn't already in the file, we insert it at the end
        f.seek(SeekFrom::End(0))?;

        // we have already seeked to end of file,
        let write = f.write_all(&page.to_bytes());

        // so we just write the page to the end of the file
        if write.is_ok() {
            // increment page count
            *self.pg_cnt.write().unwrap() += 1;
            return Ok(());
        } else {
            // write out the error in console
            println!("Error writing page to file: {:?}", write);
        }

        // return error if page couldn't be written
        Err(CrustyError::CrustyError(format!(
            "Cannot write page {} to file {}",
            page.get_page_id(),
            self.container_id
        )))?
    }
}

#[cfg(test)]
#[allow(unused_must_use)]
mod test {
    use super::*;
    use common::testutil::*;
    use temp_testdir::TempDir;

    #[test]
    fn hs_hf_insert() {
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
        let bytes = get_random_byte_vec(100);
        p0.add_value(&bytes);
        let bytes = get_random_byte_vec(100);
        p0.add_value(&bytes);
        let bytes = get_random_byte_vec(100);
        p0.add_value(&bytes);
        let p0_bytes = p0.to_bytes();

        hf.write_page_to_file(p0);
        //check the page
        assert_eq!(1, hf.num_pages());
        let checkp0 = hf.read_page_from_file(0).unwrap();
        assert_eq!(p0_bytes, checkp0.to_bytes());

        //Add another page
        let mut p1 = Page::new(1);
        let bytes = get_random_byte_vec(100);
        p1.add_value(&bytes);
        let bytes = get_random_byte_vec(100);
        p1.add_value(&bytes);
        let bytes = get_random_byte_vec(100);
        p1.add_value(&bytes);
        let p1_bytes = p1.to_bytes();

        hf.write_page_to_file(p1);

        assert_eq!(2, hf.num_pages());
        //Recheck page0
        let checkp0 = hf.read_page_from_file(0).unwrap();
        assert_eq!(p0_bytes, checkp0.to_bytes());

        //check page 1
        let checkp1 = hf.read_page_from_file(1).unwrap();
        assert_eq!(p1_bytes, checkp1.to_bytes());

        #[cfg(feature = "profile")]
        {
            assert_eq!(*hf.read_count.get_mut(), 3);
            assert_eq!(*hf.write_count.get_mut(), 2);
        }
    }
}
