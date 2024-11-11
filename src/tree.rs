// keylime: lightweight distributed ACID transaction binary tree for AWS EFS (Elastic File System).
// It provides a persistent binary tree data structure that can be concurrently accessed and modified by 
// multiple processes or machines (single-writer multiple-reader), while maintaining transactional consistency.
//
// Designed specifically for AWS EFS "close-to-open consistency" and concorrent access from AWS Lambda.
//
// Key features:
// - Uses an append-only file format for durability and consistency
// - Supports serialization and deserialization of tree nodes using pluggable storage formats (e.g. text, binary)
// - Utilizes file locking and mutex synchronization to coordinate concurrent writes (reads are lock-free)
// - Handles edge cases like incomplete writes and inconsistencies between header and file length
// - Provides methods for inserting nodes, retrieving nodes by key, and scanning key ranges
// - Includes a test suite to verify correct behavior
//
// The main components are:
// - `BinaryTree`: The core data structure representing the binary tree
// - `Node`: Represents a single node in the tree, with a key, value, and left/right child pointers 
// - `FileStorage`: Handles file I/O and locking
// - `StorageFormat` trait: Defines methods for serializing and deserializing tree nodes
// - `TextFormat` and `BinaryFormat` structs: Concrete implementations of `StorageFormat`
//

use base64::Engine;
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::thread::sleep;

// some tuning knobs
const SMALL_BUFFER_SIZE: usize = 256;
const LARGE_BUFFER_SIZE: usize = 2048;
const WRITE_LOCK_RETRY_COUNT: usize = 10;
const WRITE_LOCK_RETRY_DELAY_MS: u64 = 500;

pub struct BinaryTree {
    storage: FileStorage,
    root_offset: usize,
    storage_format: Box<dyn StorageFormat>,
    mutex: tokio::sync::Mutex<()>,
}

#[derive(Debug, Clone)]
pub struct Node {
    pub key: String,
    pub value: Vec<u8>,
    left: Option<usize>,
    right: Option<usize>,
}
impl Node {
    pub fn new(id: String, value: Vec<u8>) -> Self {
        Self {
            key: id,
            value: value,
            left: None,
            right: None,
        }
    }
}

pub trait StorageFormat {
    fn serialize(&self, node: &Node) -> std::io::Result<Vec<u8>>;

    fn deserialize_node(&self, file: &mut BufReader<&File>) -> std::io::Result<Node>;

    fn deserialize_header(
        &self,
        reader: &mut BufReader<&File>,
    ) -> std::io::Result<(String, Option<usize>, Option<usize>)>;

    fn update_left(&self, value: usize) -> (usize, Vec<u8>);

    fn update_right(&self, value: usize) -> (usize, Vec<u8>);
}

pub struct TextFormat;

impl StorageFormat for TextFormat {
    fn serialize(&self, node: &Node) -> std::io::Result<Vec<u8>> {
        let line = format!(
            "{:0>10}|{:0>10}|{}|{:?}\n",
            node.left.unwrap_or(0),
            node.right.unwrap_or(0),
            node.key,
            base64::engine::general_purpose::STANDARD
                .encode(&node.value)
                .as_str()
        );
        Ok(line.as_bytes().to_vec())
    }

    fn deserialize_node(&self, reader: &mut BufReader<&File>) -> std::io::Result<Node> {
        let (key, left, right) = self.deserialize_header(reader).unwrap();

        let mut value = Vec::new();
        reader.read_until(b'\n', &mut value).unwrap();
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(&value[1..value.len() - 2])
            .unwrap();

        Ok(Node {
            key,
            value: decoded.to_vec(),
            left,
            right,
        })
    }
    fn update_left(&self, offset: usize) -> (usize, Vec<u8>) {
        return (0, format!("{:0>10}", offset).as_bytes().to_vec());
    }
    fn update_right(&self, offset: usize) -> (usize, Vec<u8>) {
        return (10 + 1, format!("{:0>10}", offset).as_bytes().to_vec());
    }

    fn deserialize_header(
        &self,
        reader: &mut BufReader<&File>,
    ) -> std::io::Result<(String, Option<usize>, Option<usize>)> {
        let mut left = Vec::new();
        reader.read_until(b'|', &mut left).unwrap();
        left.pop(); // remove the trailing '|'
        let left = String::from_utf8(left).unwrap();

        let mut right = Vec::new();
        reader.read_until(b'|', &mut right).unwrap();
        right.pop();
        let right = String::from_utf8(right).unwrap();

        let mut key = Vec::new();
        reader.read_until(b'|', &mut key).unwrap();
        key.pop();
        let key = String::from_utf8(key).unwrap();

        Ok((
            key,
            match left.parse::<usize>() {
                Ok(0) => None,
                Ok(val) => Some(val),
                Err(e) => panic!("Unexpected Error: {:?} on [{}]", e, left),
            },
            match right.parse::<usize>() {
                Ok(0) => None,
                Ok(val) => Some(val),
                Err(e) => panic!("Unexpected Error: {:?} on [{}]", e, right),
            },
        ))
    }
}

pub struct BinaryFormat; // placeholder, not implemented yet

// impl StorageFormat for BinaryFormat {
//     fn serialize(&self, _node: &Node) -> std::io::Result<Vec<u8>> {
//         //Ok(bincode::serialize(node).unwrap())
//         todo!();
//     }

//     fn deserialize(&self, _file: &mut File) -> std::io::Result<Node> {
//         todo!();
//         // let mut data = Vec::new();
//         // Ok(bincode::deserialize(data).unwrap())
//     }
//     fn update_left(&self, _value: usize) -> (usize, Vec<u8>) {
//         todo!()
//         //return (0, bincode::serialize(&value).unwrap());
//     }
//     fn update_right(&self, _value: usize) -> (usize, Vec<u8>) {
//         todo!()
//         //return (8, bincode::serialize(&value).unwrap());
//     }
//     fn extract_header(&self, _file: &mut File) -> std::io::Result<(String,Option<usize>,Option<usize>)> {
//         todo!();
//     }
// }

impl BinaryTree {
    pub fn new(storage: FileStorage, serializer: Box<dyn StorageFormat>) -> Self {
        BinaryTree {
            storage,
            root_offset: Lock::format_header(0).len(),
            storage_format: serializer,
            mutex: tokio::sync::Mutex::new(()),
        }
    }

    /**
     * Locking strategy:
     *
     * file-locking does not prevent concurrent access from tasks in the same process,
     * so we need to use a mutex lock too. This also sidesteps the well-known NFS bug
     * where multiple file locks held by the same process are all released at once.
     *
     * There is a race condition between opening of the file and acquiring the lock,
     * during which a different process may have acquired a lock and appended to the
     * file. AWS EFS guarantees "close-to-open" consistency for "appending
     * workloads" and stronger "sync-to-read" consistency only for "non-appending".
     * So we use a non-appending workload (data at offset zero) to track the
     * current append position of the file and close/reopen if needed.
     */

    pub async fn put(&mut self, new_nodes: &[Node]) -> Vec<Option<usize>> {
        let started = std::time::Instant::now();
        let _mutex = self.mutex.lock().await;
        let mut lock = self.storage.write_lock();
        let mut file_size = lock.barrier;
        let header_cache = &mut std::collections::HashMap::new();

        let written: Vec<Option<usize>> = new_nodes
            .iter()
            .map(|new_node| {
                if file_size == self.root_offset {
                    // The tree is empty, write the new node as the root
                    let (new_node_offset, len) =
                        lock.write_node(new_node, &*self.storage_format).unwrap();
                    file_size = new_node_offset + len;
                    return Some(new_node_offset);
                }

                let mut cursor = self.root_offset;
                loop {
                    let (key, left, right) = header_cache.entry(cursor).or_insert_with(|| {
                        lock.read_header(cursor, &*self.storage_format).unwrap()
                    });

                    if new_node.key == *key {
                        // Duplicate key found, return early without writing the node
                        return None;
                    }
                    if new_node.key < *key {
                        if left.is_none() {
                            let (new_node_offset, len) =
                                lock.write_node(new_node, &*self.storage_format).unwrap();
                            file_size = new_node_offset + len;
                            let (pos, serialized_left) =
                                self.storage_format.update_left(new_node_offset);
                            lock.write_offset_pointer(cursor + pos, serialized_left)
                                .unwrap();
                            *left = Some(new_node_offset);
                            return Some(new_node_offset);
                        } else {
                            cursor = left.unwrap();
                        }
                    } else {
                        if right.is_none() {
                            let (new_node_offset, len) =
                                lock.write_node(new_node, &*self.storage_format).unwrap();
                            file_size = new_node_offset + len;
                            let (pos, serialized_right) =
                                self.storage_format.update_right(new_node_offset);
                            lock.write_offset_pointer(cursor + pos, serialized_right)
                                .unwrap();
                            *right = Some(new_node_offset);
                            return Some(new_node_offset);
                        } else {
                            cursor = right.unwrap();
                        }
                    }
                }
            })
            .collect();

        lock.sync_length_header(file_size).unwrap();
        println!(
            "{} total, {} skipped, size: +{} = {}, elapsed: {:.1?}",
            written.len(),
            written.iter().filter(|x| x.is_none()).count(),
            file_size - lock.barrier,
            file_size,
            started.elapsed()
        );

        return written;
    }

    pub fn get(&mut self, key: &str) -> Option<Node> {
        let mut current_node_offset = Some(self.root_offset);
        let mut lock = self.storage.read_lock();
        let key = key.to_string();
        loop {
            let current_node = lock
                .read_node(current_node_offset.unwrap(), &*self.storage_format)
                .unwrap();
            if current_node.key == key {
                return Some(current_node);
            } else if key < current_node.key {
                current_node_offset = current_node.left;
            } else {
                current_node_offset = current_node.right;
            }
            if current_node_offset.is_none() {
                return None;
            }
        }
    }

    pub fn scan(&mut self, start_key: &str, end_key: &str) -> Vec<Node> {
        let mut nodes = Vec::new();
        let start_key = start_key.to_string();
        let end_key = end_key.to_string();
        let mut lock = self.storage.read_lock();

        self.scan_recursive(
            Some(self.root_offset),
            &start_key,
            &end_key,
            &mut nodes,
            &mut lock,
        );

        nodes
    }

    fn scan_recursive(
        &self,
        node_offset: Option<usize>,
        start_key: &String,
        end_key: &String,
        nodes: &mut Vec<Node>,
        lock: &mut Lock,
    ) {
        if let Some(offset) = node_offset {
            let current_node = lock.read_node(offset, &*self.storage_format).unwrap();

            if current_node.key >= *start_key {
                self.scan_recursive(current_node.left, start_key, end_key, nodes, lock);
            }

            if current_node.key >= *start_key && current_node.key < *end_key {
                nodes.push(current_node.clone());
            }

            if current_node.key < *end_key {
                self.scan_recursive(current_node.right, start_key, end_key, nodes, lock);
            }
        }
    }
}

pub struct FileStorage {
    pub path: String,
}

pub struct Lock {
    file: File,
    #[allow(dead_code)]
    path: String,
    barrier: usize,
    locked: bool,
}

impl Drop for Lock {
    fn drop(&mut self) {
        // XXX: is there a race condition between unlocking and closing?
        if self.locked {
            fs2::FileExt::unlock(&self.file).unwrap();
        }
    }
}

impl Lock {
    pub fn write_node(
        &mut self,
        node: &Node,
        format: &dyn StorageFormat,
    ) -> std::io::Result<(usize, usize)> {
        let offset = self.file.seek(SeekFrom::End(0))?;
        let serialized_node = format.serialize(node)?;
        self.file.write_all(&serialized_node)?;
        Ok((offset as usize, serialized_node.len()))
    }

    pub fn read_node(
        &mut self,
        offset: usize,
        format: &dyn StorageFormat,
    ) -> std::io::Result<Node> {
        self.file.seek(SeekFrom::Start(offset as u64))?;
        let mut reader = BufReader::with_capacity(LARGE_BUFFER_SIZE, &self.file);
        format.deserialize_node(&mut reader)
    }

    pub fn read_header(
        &mut self,
        offset: usize,
        format: &dyn StorageFormat,
    ) -> std::io::Result<(String, Option<usize>, Option<usize>)> {
        self.file.seek(SeekFrom::Start(offset as u64))?;
        let mut reader = BufReader::with_capacity(SMALL_BUFFER_SIZE, &self.file);
        format.deserialize_header(&mut reader)
    }

    /** Read a node, with transaction isolation */
    pub fn read_isolated(
        &mut self,
        offset: usize,
        format: &dyn StorageFormat,
    ) -> Option<std::io::Result<Node>> {
        if offset >= self.barrier {
            return None;
        } else {
            return Some(self.read_node(offset, format));
        }
    }

    fn write_offset_pointer(&mut self, offset: usize, bytes: Vec<u8>) -> std::io::Result<()> {
        self.file.seek(SeekFrom::Start(offset as u64))?;
        self.file.write_all(&bytes)
    }

    fn exclusive(storage: &FileStorage) -> Lock {
        for _ in 0..WRITE_LOCK_RETRY_COUNT {
            let mut file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&storage.path)
                .unwrap();
            fs2::FileExt::lock_exclusive(&file).unwrap();

            // Handle the race condition between the file being opened and being locked.
            // This matters in AWS EFS environment because consistent file metadata is only
            // guaranteed at open, not at lock acquisition.
            // Lock acquisition can take a long time waiting for another client to finish
            // writing, so this is not a rare condition.

            let meta_len = file.metadata().unwrap().len();
            let len = Lock::read_len_header(&mut file);

            if len > meta_len {
                // AWS EFS documentation implies that this could happen anytime, as "appending"
                // workloads are not guaranteed to be consistent. So data updates to the header
                // by another client could theoretically become visible before the file growth
                // is visible to this client, until the file is closed and re-opened here.
                println!(
                    "Header length [{}] greater than file length [{}]",
                    len, meta_len
                );
                fs2::FileExt::unlock(&mut file).unwrap();
                sleep(std::time::Duration::from_millis(WRITE_LOCK_RETRY_DELAY_MS));
                continue; // re-enter the open-and-lock race
            }

            let barrier = if len < meta_len {
                // This condition covers the rare but expected scenario where another writer
                // crashed and released its exclusive lock before updating the length header.
                // XXX: The right thing to do here would be to rollback the other client's
                // updates, and truncate the file to the header length. For now we just ignore
                // the failure and keep going. This shouldn't happen often.
                println!(
                    "Incomplete write detected: header len={}, file len={}",
                    len, meta_len
                );
                meta_len as usize
            } else if len == 0 {
                let buf = Lock::format_header(len as usize);
                file.write_all(&buf.as_bytes()).unwrap();
                buf.len()
            } else {
                len as usize
            };

            return Lock {
                file,
                path: storage.path.clone(),
                barrier,
                locked: true,
            };
        }
        panic!("Too many retries"); // FIXME: better error handling
    }

    fn shared(storage: &FileStorage) -> Lock {
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(false)
            .open(&storage.path)
            .map_err(|e| {
                println!("Error opening file {}: {:?}", &storage.path, e);
                e
            })
            .unwrap();

        // locking is actually not needed, because offsets monotonically
        // increase and pointers only ever transition from zero to non-zero,
        // so offsets larger than the barrier can be safely ignored.
        // fs2::FileExt::lock_shared(&file).unwrap();

        let barrier = Self::read_len_header(&mut file) as usize;
        Lock {
            file: file,
            path: storage.path.clone(),
            barrier,
            locked: false,
        }
    }

    pub fn format_header(len: usize) -> String {
        // file header is in printable ASCII format for ease of inspection/debugging.
        format!("\"{:016x}\"\n", len)
    }

    fn sync_length_header(&mut self, len: usize) -> Result<(), Box<dyn std::error::Error>> {
        let buf = Lock::format_header(len);
        self.file.seek(std::io::SeekFrom::Start(0))?;
        self.file.write_all(&buf.as_bytes())?;
        self.file.flush()?;
        self.file.sync_all()?;
        Ok(())
    }

    fn read_len_header(file: &mut File) -> u64 {
        let header_bytes = Lock::format_header(0).len();
        let mut buf: Vec<u8> = vec![0; header_bytes];
        return match file.read_exact(buf.as_mut_slice()) {
            Ok(_) => {
                let slice = std::str::from_utf8(&buf[1..header_bytes - 2]).unwrap();
                u64::from_str_radix(slice, 16).unwrap()
            }
            Err(e) => {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    0
                } else {
                    panic!("Error: {:?}", e);
                }
            }
        };
    }
}

impl FileStorage {
    pub fn new(path: String) -> Self {
        FileStorage { path }
    }

    fn write_lock(&mut self) -> Lock {
        Lock::exclusive(self)
    }

    fn read_lock(&mut self) -> Lock {
        Lock::shared(self)
    }
}

#[cfg(test)]
mod tests {
    use std::fs::remove_file;

    use super::*;

    #[tokio::test]
    async fn test_binary_tree() {
        let path = "testdata/test-tree.txt";
        let _ = remove_file(path);
        let _ = std::fs::create_dir_all("testdata");

        let storage = FileStorage::new(path.to_string());
        let format = Box::new(TextFormat);
        let mut tree = BinaryTree::new(storage, format);

        let node1 = Node {
            key: "key1".to_string(),
            value: vec![1, 2, 3],
            left: None,
            right: None,
        };
        let node2 = Node {
            key: "key3".to_string(),
            value: vec![4, 5, 6],
            left: None,
            right: None,
        };
        let node3 = Node {
            key: "key0".to_string(),
            value: vec![7, 8, 9],
            left: None,
            right: None,
        };
        let node4 = Node {
            key: "key2".to_string(),
            value: vec![10, 11, 12],
            left: None,
            right: None,
        };

        let initial_offset = 19;

        let offset1 = tree.put(&[node1.clone()]).await[0];
        assert!(offset1.unwrap() == initial_offset + 0);

        let offset2 = tree.put(&[node2]).await[0];
        assert!(offset2.unwrap() == initial_offset + 34);

        let offset3 = tree.put(&[node3]).await[0];
        assert!(offset3.unwrap() == initial_offset + 68);

        let offset4 = tree.put(&[node4]).await[0];
        assert!(offset4.unwrap() == initial_offset + 102);

        let retrieved_node1 = tree.get("key1").unwrap();
        assert_eq!(retrieved_node1.key, "key1");
        assert_eq!(retrieved_node1.value, vec![1, 2, 3]);

        let retrieved_node2 = tree.get("key3").unwrap();
        assert_eq!(retrieved_node2.key, "key3");
        assert_eq!(retrieved_node2.value, vec![4, 5, 6]);

        let retrieved_node3 = tree.get("key0").unwrap();
        assert_eq!(retrieved_node3.key, "key0");
        assert_eq!(retrieved_node3.value, vec![7, 8, 9]);

        let retrieved_node4 = tree.get("key2").unwrap();
        assert_eq!(retrieved_node4.key, "key2");
        assert_eq!(retrieved_node4.value, vec![10, 11, 12]);

        // test duplicate insert
        let offset5 = tree.put(&[node1]).await[0];
        assert!(offset5.is_none());

        let scan = tree.scan("key0", "key3");
        assert_eq!(scan.len(), 3);
        assert_eq!(scan[0].key, "key0");
        assert_eq!(scan[1].key, "key1");
        assert_eq!(scan[2].key, "key2");

        let scan = tree.scan("key1", "key2");
        assert_eq!(scan.len(), 1);
        assert_eq!(scan[0].key, "key1");

        let scan = tree.scan("key", "key1");
        assert_eq!(scan.len(), 1);
        assert_eq!(scan[0].key, "key0");
    }
}
