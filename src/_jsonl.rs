/**
 * Initial JSONL proof of concept implementation. For documentation/reference only.
 */

use std::{
    collections::HashMap,
    error::Error,
    io::{Read, Seek, Write},
};

const N_PARTITIONS: usize = 32;
const HASH_MIXER: usize = 31;

pub fn format_header(len: u64) -> String {
    // file header is in printable ASCII format for ease of inspection/debugging.
    format!("\"{:016x}\"\n", len)
}

#[derive(Eq, PartialEq, Hash, Clone)]
pub struct Key {
    partition: usize,
    identifier: String,
}

impl Key {
    pub fn from_id(id: &String) -> Self {
        Key {
            partition: id
                .chars()
                .fold(0, |acc, c| (acc * HASH_MIXER + c as usize) % N_PARTITIONS),
            identifier: id.clone(),
        }
    }
}

pub struct JsonStore {
    partitions: Vec<Partition>,
}

impl JsonStore {
    pub fn new(data_dir: String) -> Self {
        let partitions = (0..N_PARTITIONS)
            .map(|partition| Partition::new(partition, &data_dir))
            .collect();
        JsonStore { partitions }
    }
    pub async fn write_json_lines(&'static self, stream: Vec<(Key, String)>) -> usize {

        let partitioned_buffers = &mut HashMap::<usize, Vec<(Key,Vec<u8>)>>::new();

        for (key, value) in stream.into_iter() {
            let buffer = partitioned_buffers
                .entry(key.partition)
                .or_insert_with(Vec::new);
            buffer.push((key, format!("{}\n", value).as_bytes().to_vec()));
        }

        let futures = partitioned_buffers.keys().map(|partition| {
            let partition = partition.clone();
            let buffer = partitioned_buffers.get(&partition).unwrap().clone();
            tokio::task::spawn_blocking(move || {
                tokio::runtime::Handle::current()
                    .block_on(async {
                        let buffer = buffer.iter().fold(Vec::new(), |mut acc, (_, value)| {
                            acc.extend_from_slice(value);
                            acc
                        });
                        self.partitions[partition].atomic_append(buffer).await.unwrap()
                })
            })
        });

        let results = futures::future::join_all(futures).await;

        let ok_count = results
            .iter()
            .filter_map(|result| {
                if let Err(e) = result {
                    panic!("Error: {:?}", e);
                } else {
                    Some(())
                }
            })
            .count();

        ok_count
    }
}

pub struct Partition {
    partition: usize,
    data_file_path: String,
    mutex: tokio::sync::Mutex<()>
}

impl Partition {
    pub fn new(partition: usize, data_dir: &str) -> Self {
        let data_file_path = format!("{}/{:02x}.jsonl", data_dir, partition);
        let mutex = tokio::sync::Mutex::new(());
        Partition {
            partition,
            data_file_path,
            mutex,
        }
    }

    pub async fn atomic_append(
        &self,
        data: Vec<u8>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let started = std::time::Instant::now();

        // file-locking does not prevent concurrent access from tasks in the same process,
        // so we need to use a mutex lock too. This also sidesteps the well-known NFS bug
        // where multiple file locks held by the same process are all released at once.
        let _mutex = self.mutex.lock().await;
        let (mutex_elapsed, started) = (started.elapsed(), std::time::Instant::now());

        let data_file = &mut std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&self.data_file_path)
            .unwrap();

        let (opened_elapsed, started) = (started.elapsed(), std::time::Instant::now());

        fs2::FileExt::lock_exclusive(data_file).unwrap();
        let (locked_elapsed, started) = (started.elapsed(), std::time::Instant::now());

        let old_len =
            read_or_init_len_header(data_file).expect(&format!("File={}", &self.data_file_path));
        let (old_len_elapsed, started) = (started.elapsed(), std::time::Instant::now());

        let new_added_len = data.len() as u64;

        data_file.seek(std::io::SeekFrom::End(0))?;
        data_file.write_all(&data)?;

        let (write_elapsed, started) = (started.elapsed(), std::time::Instant::now());

        write_len_header(data_file, old_len + new_added_len)?;
        let (write_header_elapsed, started) = (started.elapsed(), std::time::Instant::now());

        fs2::FileExt::unlock(data_file).unwrap();
        let (unlock_elapsed, _started) = (started.elapsed(), std::time::Instant::now());

        println!("partition[{}] len=({}+{}={}), elapsed times: mutex={}, open={}, lock={}, read={}, write={}, write_header={} unlock={}",
                 self.partition, old_len, new_added_len, old_len + new_added_len,
                 mutex_elapsed.as_millis(), opened_elapsed.as_millis(),
                 locked_elapsed.as_millis(), old_len_elapsed.as_millis(),
                 write_elapsed.as_millis(), write_header_elapsed.as_millis(),
                 unlock_elapsed.as_millis());
        Ok(())
    }

}

fn write_len_header(data_file: &mut std::fs::File, len: u64) -> Result<(), Box<dyn Error>> {
    let buf = format_header(len);
    data_file.seek(std::io::SeekFrom::Start(0))?;
    data_file.write_all(&buf.as_bytes())?;
    data_file.flush()?;
    data_file.sync_all()?;
    Ok(())
}

fn read_or_init_len_header(data_file: &mut std::fs::File) -> Result<u64, Box<dyn Error>> {
    let meta_len = data_file.metadata().unwrap().len();
    let header_bytes = format_header(0).len();
    let mut buf: Vec<u8> = vec![0; header_bytes];
    let len = match data_file.read_exact(buf.as_mut_slice()) {
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
    if len != meta_len {
        let msg = format!(
            "Initial length mismatch: header={} != meta={}",
            len, meta_len
        );
        return Err(msg.into());
    }
    if len == 0 {
        let buf = format_header(len);
        data_file.write_all(&buf.as_bytes())?;
        Ok(buf.len() as u64)
    } else {
        Ok(len)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[tokio::test]
    async fn test_atomic_append() {
        // Create a temporary directory for testing
        let id1 = "id1".to_string();
        let data_dir = "testdata".to_owned();
        fs::create_dir_all(&data_dir).unwrap();
        let key = Key::from_id(&id1);

        let partition = Partition::new(key.partition, &data_dir.clone());
        let _rm = fs::remove_file(&partition.data_file_path);


        // Test data
        let lines1 = "line1\nline2\n";
        let lines2 = "line3\nline4\n";

        // Perform atomic append
        partition
            .atomic_append(lines1.into())
            .await
            .unwrap();
        partition
            .atomic_append(lines2.into())
            .await
            .unwrap();

        let contents = fs::read_to_string(&partition.data_file_path).unwrap();

        // Verify the contents
        let expected_contents = "\"000000000000002b\"\nline1\nline2\nline3\nline4\n";
        assert_eq!(contents, expected_contents);
    }
}
