use crate::heapfile::HeapFile;
use crate::heapfileiter::HeapFileIterator;
use crate::page::Page;
use common::prelude::*;
use common::storage_trait::StorageTrait;
use common::testutil::gen_random_test_sm_dir;
use common::PAGE_SIZE;
use std::borrow::BorrowMut;
use std::collections::HashMap;
use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::{Arc, RwLock};

/*
StorageManager is a hashmap from container ids to heapfile structs
heapfiles should hold file contents in memory
*/

/// The StorageManager struct
// #[derive(Serialize, Deserialize)]
pub struct StorageManager {
    /// Path to database metadata files.
    pub storage_path: PathBuf,
    /// Map from container id to heapfile
    c_map: Arc<RwLock<HashMap<ContainerId, Arc<HeapFile>>>>,
    /// Indicates if this is a temp StorageManager (for testing)
    is_temp: bool,
}

/// The required functions in HeapStore's StorageManager that are specific for HeapFiles
impl StorageManager {
    /// Get a page if exists for a given container.
    pub(crate) fn get_page(
        &self,
        container_id: ContainerId,
        page_id: PageId,
        _tid: TransactionId,
        _perm: Permissions,
        _pin: bool,
    ) -> Option<Page> {
        let c_map = self.c_map.read().unwrap();
        if !(c_map.contains_key(&container_id)) {
            println!("Container ID not found in StorageManager's c_map");
            return None;
        }
        // otherwise we get the specified container and read the page
        let hf = &c_map[&container_id];
        match hf.read_page_from_file(page_id) {
            Ok(page) => Some(page),
            Err(_) => None,
        }
    }

    /// Write a page
    pub(crate) fn write_page(
        &self,
        container_id: ContainerId,
        page: Page,
        _tid: TransactionId,
    ) -> Result<(), CrustyError> {
        let c_map = self.c_map.write().unwrap();
        if !(c_map.contains_key(&container_id)) {
            return Err(CrustyError::CrustyError(String::from("Container ID not found in StorageManager's c_map")));
        }
        // otherwise we get the specified container and write the page
        let hf = &c_map[&container_id];
        hf.write_page_to_file(page)
    }

    /// Get the number of pages for a container
    fn get_num_pages(&self, container_id: ContainerId) -> PageId {
        self.c_map.read().unwrap()[&container_id].num_pages()
    }


    /// Test utility function for counting reads and writes served by the heap file.
    /// Can return 0,0 for invalid container_ids
    #[allow(dead_code)]
    pub(crate) fn get_hf_read_write_count(&self, container_id: ContainerId) -> (u16, u16) {
        let c_map = self.c_map.read().unwrap();
        if !(c_map.contains_key(&container_id)) {
            return (0, 0);
        }
        let hf = &c_map[&container_id];
        let read_count = hf.read_count.load(Ordering::Relaxed);
        let write_count = hf.write_count.load(Ordering::Relaxed);
        ( read_count, write_count)
    }

    /// For testing
    pub fn get_page_debug(&self, container_id: ContainerId, page_id: PageId) -> String {
        match self.get_page(
            container_id,
            page_id,
            TransactionId::new(),
            Permissions::ReadOnly,
            false,
        ) {
            Some(p) => {
                format!("{:?}", p)
            }
            None => String::new(),
        }
    }

    /// For testing
    pub fn get_page_bytes(&self, container_id: ContainerId, page_id: PageId) -> Vec<u8> {
        match self.get_page(
            container_id,
            page_id,
            TransactionId::new(),
            Permissions::ReadOnly,
            false,
        ) {
            Some(p) => p.to_bytes(),
            None => Vec::new(),
        }
    }
}

/// Implementation of storage trait
impl StorageTrait for StorageManager {
    type ValIterator = HeapFileIterator;

    /// Create a new storage manager that will use storage_path as the location to persist data
    /// (if the storage manager persists records on disk; not the case for memstore)
    /// For startup/shutdown: check the storage_path for data persisted in shutdown() that you can
    /// use to populate this instance of the SM. Otherwise create a new one.
    fn new(storage_path: PathBuf) -> Self {
        // check the c_map file for data persisted in shutdown()
        let mut path = PathBuf::from(storage_path.clone());
        path = path.join(String::from("c_map"));
        let mut f = fs::File::open(path);
        // if the file doesn't exist, return a new storage manager
        if f.is_err() {
            println!("File not found");
            return StorageManager { storage_path, c_map: Arc::new(RwLock::new(HashMap::new())), is_temp: false}
        }
        let f = f.unwrap();
        // read the file into a byte buffer
        let mut reader = BufReader::new(f);

        // deserialize the reader from serde_json
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer).unwrap();
        let buffer: Vec<u16> = serde_json::from_slice(&buffer).unwrap();

        // get the length of the c_map
        let cnt = buffer[0];

        // if there are no containers, return a new storage manager
        if cnt == 0 {
            return StorageManager { storage_path, c_map: Arc::new(RwLock::new(HashMap::new())), is_temp: false}
        }
        // otherwise, create a new hashmap to hold the container id and heapfile pairs
        let mut c_map = HashMap::new();
        for idx in 1..cnt + 1 {
            
            // convert the bytes to a container id
            let container_id = buffer[idx as usize];
            // create a path for the heapfile based on the c_id
            let mut file_path = storage_path.clone();
            // use push to add the c_id to the path
            file_path.push(String::from("c") + &container_id.to_string());
            // create a new heapfile with the path specified
            let hf = HeapFile::new(file_path.clone(), container_id).unwrap();

            // add the heapfile to the c_map
            c_map.insert(container_id, Arc::new(hf));
        }
        StorageManager { storage_path, c_map: Arc::new(RwLock::new(c_map)), is_temp: false }
        // move through the buff reading every 2 bytes into a container_id. The first
        // two bytes are the length, and the filepath for a given container is given
        // by joining the storage path with 'c' + container_id
    }

    /// Create a new storage manager for testing. There is no startup/shutdown logic here: it
    /// should simply create a fresh SM and set is_temp to true
    fn new_test_sm() -> Self {
        let storage_path = gen_random_test_sm_dir();
        StorageManager { storage_path, c_map: Arc::new(RwLock::new(HashMap::new())), is_temp: true }
    }

    /// Insert some bytes into a container for a particular value (e.g. record).
    /// Any validation will be assumed to happen before.
    /// Returns the value id associated with the stored value.
    /// Function will need to find the first page that can hold the value.
    /// A new page may need to be created if no space on existing pages can be found.
    fn insert_value(
        &self,
        container_id: ContainerId,
        value: Vec<u8>,
        tid: TransactionId,
    ) -> ValueId {
        if value.len() > PAGE_SIZE {
            panic!("Cannot handle inserting a value larger than the page size");
        }
        // if the container has no pages, make one and insert the value
        if self.get_num_pages(container_id) == 0 {
            let mut new_page = Page::new(0);
            new_page.add_value(&value);
            self.write_page(container_id, new_page, tid).unwrap();
            return ValueId {
                container_id,
                segment_id: None,
                page_id: Some(0),
                slot_id: Some(0),
            }
        }

        // starting with the smallest p_id, iterate through all pages until you
        // find a page that can hold the value
        // if no page can hold the value, create a new page and insert the value

        let mut p_id = 0;
        loop {
            let mut pg = self.get_page(container_id, p_id, tid, Permissions::ReadWrite, false).unwrap();
            match pg.add_value(&value) {
                Some(slot_id) => {
                    // if the addition is successful, write the page to the hf
                    // and return the ValueID
                    self.write_page(container_id, pg, tid).unwrap();
                    return ValueId {
                        container_id,
                        segment_id: None,
                        slot_id: Some(slot_id),
                        page_id: Some(p_id),
                    }
                }
                None => {
                    // increment p_id to try next page
                    p_id += 1;
                    // if we are at the end of the file, append and return v_id
                    if p_id >= self.c_map.read().unwrap()[&container_id].num_pages() {
                        // create a new page with the page_id and append it to the file
                        let mut new_page = Page::new(p_id);
                        let slot_id = new_page.add_value(&value).unwrap();
                        self.write_page(container_id, new_page, tid).unwrap();
                        return ValueId {
                            container_id,
                            segment_id: None,
                            page_id: Some(p_id),
                            slot_id: Some(slot_id),
                        }
                    }

                }
            }
        }
    }

    /// Insert some bytes into a container for vector of values (e.g. record).
    /// Any validation will be assumed to happen before.
    /// Returns a vector of value ids associated with the stored values.
    fn insert_values(
        &self,
        container_id: ContainerId,
        values: Vec<Vec<u8>>,
        tid: TransactionId,
    ) -> Vec<ValueId> {
        let mut ret = Vec::new();
        for v in values {
            ret.push(self.insert_value(container_id, v, tid));
        }
        ret
    }

    /// Delete the data for a value. If the valueID is not found it returns Ok() still.
    fn delete_value(&self, id: ValueId, tid: TransactionId) -> Result<(), CrustyError> {
        // get the page from the value id
        let mut page = self.get_page(id.container_id, id.page_id.unwrap(), tid, Permissions::ReadWrite, false).unwrap();
        // delete the value from the page
        page.delete_value(id.slot_id.unwrap());
        // write the page back to the heapfile
        self.write_page(id.container_id, page, tid).unwrap();
        Ok(())
    }

    /// Updates a value. Returns valueID on update (which may have changed). Error on failure
    /// Any process that needs to determine if a value changed will need to compare the return valueId against
    /// the sent value.
    fn update_value(
        &self,
        value: Vec<u8>,
        id: ValueId,
        _tid: TransactionId,
    ) -> Result<ValueId, CrustyError> {
        // delete the old value
        match self.delete_value(id, _tid) {
            Ok(_) => (),
            Err(e) => return Err(e),
        } 
        // add the new value
        Ok(self.insert_value(id.container_id, value, _tid))
    }

    /// Create a new container to be stored.
    /// fn create_container(&self, name: String) -> ContainerId;
    /// Creates a new container object.
    /// For this milestone you will not need to utilize
    /// the container_config, name, container_type, or dependencies
    ///
    ///
    /// # Arguments
    ///
    /// * `container_id` - Id of container to add delta to.
    fn create_container(
        &self,
        container_id: ContainerId,
        _name: Option<String>,
        _container_type: common::ids::StateType,
        _dependencies: Option<Vec<ContainerId>>,
    ) -> Result<(), CrustyError> {
        // create a new path for the heapfile based on the storage path using
        // Path::new and .join()
        let mut path = PathBuf::from(self.storage_path.clone());
        // creating a new path for the container (heapfile)
        path = path.join(String::from("c") + &container_id.to_string());
        // create a new heapfile with the path specified
        let hf = HeapFile::new(path, container_id).unwrap();

        self.c_map.write().unwrap().insert(container_id, Arc::new(hf));
        Ok(())
    }

    /// A wrapper function to call create container
    fn create_table(&self, container_id: ContainerId) -> Result<(), CrustyError> {
        self.create_container(container_id, None, common::ids::StateType::BaseTable, None)
    }

    /// Remove the container and all stored values in the container.
    /// If the container is persisted remove the underlying files
    fn remove_container(&self, container_id: ContainerId) -> Result<(), CrustyError> {
        // get the path to the container
        let mut path = PathBuf::from(self.storage_path.clone());
        path = path.join(String::from("c") + &container_id.to_string());
        // delete the file
        fs::remove_file(path)?;
        // update the c_map
        self.c_map.write().unwrap().remove(&container_id);
        Ok(())
    }

    /// Get an iterator that returns all valid records
    fn get_iterator(
        &self,
        container_id: ContainerId,
        tid: TransactionId,
        _perm: Permissions,
    ) -> Self::ValIterator {
        //create an iterator for the specified container
        let hf = self.c_map.write().unwrap()[&container_id].clone();
        HeapFileIterator::new(tid, hf)
    }

    /// Get the data for a particular ValueId. Error if does not exists
    fn get_value(
        &self,
        id: ValueId,
        tid: TransactionId,
        perm: Permissions,
    ) -> Result<Vec<u8>, CrustyError> {
        // use the value id to get the right container, page, and slot and return
        // either the matching data or an error if the data can't be found
        let page = self.get_page(id.container_id, id.page_id.unwrap(), tid, perm, false).unwrap();
        match page.get_value(id.slot_id.unwrap()) {
            Some(val) => Ok(val),
            None => Err(CrustyError::CrustyError(String::from("Unable to get value"))),
        }
    }

    /// Notify the storage manager that the transaction is finished so that any held resources can be released.
    fn transaction_finished(&self, tid: TransactionId) {
        panic!("TODO milestone tm");
    }

    /// Testing utility to reset all state associated the storage manager. Deletes all data in
    /// storage path (keeping storage path as a directory). Doesn't need to serialize any data to
    /// disk as its just meant to clear state.
    ///
    /// Clear any data structures in the SM you add
    fn reset(&self) -> Result<(), CrustyError> {
        fs::remove_dir_all(self.storage_path.clone())?;
        fs::create_dir_all(self.storage_path.clone()).unwrap();
        // delete cmap
        self.c_map.write().unwrap().clear();
        Ok(())
    }

    /// If there is a buffer pool or cache it should be cleared/reset.
    /// Otherwise do nothing.
    fn clear_cache(&self) {
    }

    /// Shutdown the storage manager. Should be safe to call multiple times. You can assume this
    /// function will never be called on a temp SM.
    /// This should serialize the mapping between containerID and Heapfile to disk in a way that
    /// can be read by StorageManager::new.
    /// HINT: Heapfile won't be serializable/deserializable. You'll want to serialize information
    /// that can be used to create a HeapFile object pointing to the same data. You don't need to
    /// worry about recreating read_count or write_count.
    fn shutdown(&self) {
        // serialize c_map to disk
        let mut path = PathBuf::from(self.storage_path.clone());
        path = path.join(String::from("c_map"));
        let mut f = fs::File::create(path).unwrap();
        let c_map = self.c_map.read().unwrap();
        let len: u16 = c_map.len() as u16;

        // create a vector to hold the length of the c_map and all c_id's
        let mut buffer = Vec::new();
        // push the length of the c_map to the buffer
        buffer.push(len);
        // iterate through the c_map and push each c_id to the buffer
        for (c_id, _) in c_map.iter() {
            buffer.push(*c_id);
        }
        // use serde to serialize the buffer to json
        let serialized = serde_json::to_string(&buffer).unwrap();
        println!("serialized = {}", serialized);
        // write this to the specified file
        f.write_all(serialized.as_bytes()).unwrap();
    }

    fn import_csv(
        &self,
        table: &Table,
        path: String,
        _tid: TransactionId,
        container_id: ContainerId,
    ) -> Result<(), CrustyError> {
        // Err(CrustyError::CrustyError(String::from("TODO")))
        // Convert path into an absolute path.
        let path = fs::canonicalize(path)?;
        debug!("server::csv_utils trying to open file, path: {:?}", path);
        let file = fs::File::open(path)?;
        // Create csv reader.
        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(file);

        // Iterate through csv records.
        let mut inserted_records = 0;
        for result in rdr.records() {
            #[allow(clippy::single_match)]
            match result {
                Ok(rec) => {
                    // Build tuple and infer types from schema.
                    let mut tuple = Tuple::new(Vec::new());
                    for (field, attr) in rec.iter().zip(table.schema.attributes()) {
                        // TODO: Type mismatch between attributes and record data>
                        match &attr.dtype() {
                            DataType::Int => {
                                let value: i32 = field.parse::<i32>().unwrap();
                                tuple.field_vals.push(Field::IntField(value));
                            }
                            DataType::String => {
                                let value: String = field.to_string().clone();
                                tuple.field_vals.push(Field::StringField(value));
                            }
                        }
                    }
                    //TODO: How should individual row insertion errors be handled?
                    debug!(
                        "server::csv_utils about to insert tuple into container_id: {:?}",
                        &container_id
                    );
                    self.insert_value(container_id, tuple.to_bytes(), _tid);
                    inserted_records += 1;
                }
                _ => {
                    // FIXME: get error from csv reader
                    error!("Could not read row from CSV");
                    return Err(CrustyError::IOError(
                        "Could not read row from CSV".to_string(),
                    ));
                }
            }
        }
        info!("Num records imported: {:?}", inserted_records);
        Ok(())
    }
}

/// Trait Impl for Drop
impl Drop for StorageManager {
    // if temp SM this clears the storage path entirely when it leaves scope; used for testing
    fn drop(&mut self) {
        if self.is_temp {
            debug!("Removing storage path on drop {:?}", self.storage_path);
            let remove_all = fs::remove_dir_all(self.storage_path.clone());
            if let Err(e) = remove_all {
                println!("Error on removing temp dir {}", e);
            }
        }
    }
}

#[cfg(test)]
#[allow(unused_must_use)]
mod test {
    use super::*;
    use crate::storage_manager::StorageManager;
    use common::storage_trait::StorageTrait;
    use common::testutil::*;
    #[test]
    fn hs_sm_basic_read_write(){
        init();
        let sm = StorageManager::new_test_sm();
        let cid = 1;
        sm.create_table(cid);
        let tid = TransactionId::new();
        let page_id = 0;

        let bytes = get_random_byte_vec(40);

        let mut page = Page::new(page_id);
        page.add_value(&bytes);
        
        // write a page with the storage manager into the only container
        sm.write_page(cid, page, tid);
        
        // check that the page we get from the heap file matches the original page
        let page2 = sm.get_page(cid, 0, tid, Permissions::ReadOnly, false)
            .expect("Unable to get page from heapfile");
        assert_eq!(bytes, page2.get_value(0).unwrap());
    }
    #[test]
    fn hs_sm_a_insert() { // currently overwriting page data instead of adding to it
        init();
        let sm = StorageManager::new_test_sm();
        let cid = 1;
        sm.create_table(cid);

        let bytes = get_random_byte_vec(40);
        let tid = TransactionId::new();

        let val1 = sm.insert_value(cid, bytes.clone(), tid);
        assert_eq!(1, sm.get_num_pages(cid));
        assert_eq!(0, val1.page_id.unwrap());
        assert_eq!(0, val1.slot_id.unwrap());

        let p1 = sm
            .get_page(cid, 0, tid, Permissions::ReadOnly, false)
            .unwrap();

        let val2 = sm.insert_value(cid, [1].to_vec(), tid);
        assert_eq!(1, sm.get_num_pages(cid));
        assert_eq!(0, val2.page_id.unwrap());
        assert_eq!(1, val2.slot_id.unwrap());

        let p2 = sm
            .get_page(cid, 0, tid, Permissions::ReadOnly, false)
            .unwrap();

        //print the valueid's to see if they are different
        assert_ne!(p1.to_bytes()[..], p2.to_bytes()[..]);
    }

    // #[test]
    // fn hs_sm_insertalization() { // currently overwriting page data instead of adding to it
    //     init();
    //     let sm = StorageManager::new_test_sm();
    //     let cid = 1;
    //     sm.create_table(cid);

    //     let bytes = get_random_byte_vec(40);
    //     let tid = TransactionId::new();

    //     let val1 = sm.insert_value(cid, bytes.clone(), tid);
    //     assert_eq!(1, sm.get_num_pages(cid));
    //     assert_eq!(0, val1.page_id.unwrap());
    //     assert_eq!(0, val1.slot_id.unwrap());

    //     let p1 = sm
    //         .get_page(cid, 0, tid, Permissions::ReadOnly, false)
    //         .unwrap();

    //     let val2 = sm.insert_value(cid, [1].to_vec(), tid);
    //     assert_eq!(1, sm.get_num_pages(cid));
    //     assert_eq!(0, val2.page_id.unwrap());
    //     assert_eq!(1, val2.slot_id.unwrap());

    //     // insert 25 more values into page2
    //     for _ in 0..1000 {
    //         sm.insert_value(cid, [1].to_vec(), tid);
    //     }

    //     // this should cause a third page to be created, check that it exists
    //     let p3 = sm
    //         .get_page(cid, 2, tid, Permissions::ReadOnly, false)
    //         .unwrap();

    //     let p2 = sm
    //         .get_page(cid, 0, tid, Permissions::ReadOnly, false)
    //         .unwrap();

    //     //print the valueid's to see if they are different
    //     assert_eq!(p1.to_bytes()[..], p2.to_bytes()[..]);


    // }

    #[test]
    fn hs_sm_b_iter_small() {
        init();
        let sm = StorageManager::new_test_sm();
        let cid = 1;
        sm.create_table(cid);
        let tid = TransactionId::new();

        //Test one page
        let mut byte_vec: Vec<Vec<u8>> = vec![
            get_random_byte_vec(400),
            get_random_byte_vec(400),
            get_random_byte_vec(400),
        ];
        for val in &byte_vec {
            sm.insert_value(cid, val.clone(), tid);
        }
        let iter = sm.get_iterator(cid, tid, Permissions::ReadOnly);
        for (i, x) in iter.enumerate() {
            assert_eq!(byte_vec[i], x.0);
        }

        // Should be on two pages
        let mut byte_vec2: Vec<Vec<u8>> = vec![
            get_random_byte_vec(400),
            get_random_byte_vec(400),
            get_random_byte_vec(400),
            get_random_byte_vec(400),
        ];

        for val in &byte_vec2 {
            sm.insert_value(cid, val.clone(), tid);
        }
        byte_vec.append(&mut byte_vec2);

        let iter = sm.get_iterator(cid, tid, Permissions::ReadOnly);
        for (i, x) in iter.enumerate() {
            assert_eq!(byte_vec[i], x.0);
        }

        // Should be on 3 pages
        let mut byte_vec2: Vec<Vec<u8>> = vec![
            get_random_byte_vec(300),
            get_random_byte_vec(500),
            get_random_byte_vec(400),
        ];

        for val in &byte_vec2 {
            sm.insert_value(cid, val.clone(), tid);
        }
        byte_vec.append(&mut byte_vec2);

        let iter = sm.get_iterator(cid, tid, Permissions::ReadOnly);
        for (i, x) in iter.enumerate() {
            assert_eq!(byte_vec[i], x.0);
        }
    }

    #[test]
    #[ignore]
    fn hs_sm_b_iter_large() {
        init();
        let sm = StorageManager::new_test_sm();
        let cid = 1;

        sm.create_table(cid).unwrap();
        let tid = TransactionId::new();

        let vals = get_random_vec_of_byte_vec(1000, 40, 400);
        sm.insert_values(cid, vals, tid);
        let mut count = 0;
        for _ in sm.get_iterator(cid, tid, Permissions::ReadOnly) {
            count += 1;
        }
        assert_eq!(1000, count);
    }
}
