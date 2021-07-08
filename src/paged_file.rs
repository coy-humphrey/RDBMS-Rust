use crate::page::*;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::io::Error;
use std::io::ErrorKind;
use std::io::Result;
use std::io::SeekFrom;
use std::path::Path;

const HEADER_LEN: usize = 8;

pub struct PagedFile<const PAGE_SIZE: usize> {
    file: File,
}

impl<const PAGE_SIZE: usize> PagedFile<PAGE_SIZE> {
    /// Create a Paged File Handle for the file at the given path.
    pub fn open(path: &Path) -> Result<Self> {
        let mut file = OpenOptions::new().read(true).write(true).open(path)?;

        // Read the header from the first HEADER_LEN bytes of the file
        let mut hdr = [0; HEADER_LEN];
        file.read_exact(&mut hdr)?;
        // Extract the page_size from the header
        let hdr_page_size: u64 = u64::from_le_bytes(hdr);
        // Confirm the hdr page size matches the expected page size
        if hdr_page_size != PAGE_SIZE as u64 {
            let err_str = format!(
                "Page size mismatch. Header: {}, Expected: {}",
                hdr_page_size, PAGE_SIZE as u64
            );
            return Err(Error::new(ErrorKind::Other, err_str));
        }
        Ok(PagedFile::<PAGE_SIZE> { file })
    }

    /// Create a Paged File and return a handle for the newly created file.
    pub fn create(path: &Path) -> Result<Self> {
        let mut file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(path)?;
        let hdr = (PAGE_SIZE as u64).to_le_bytes();
        file.write_all(&hdr)?;
        Ok(PagedFile::<PAGE_SIZE> { file })
    }

    /// Return the number of pages in the file.
    pub fn num_pages(&self) -> Result<u64> {
        let metadata = self.file.metadata()?;
        Ok((metadata.len() - HEADER_LEN as u64) / PAGE_SIZE as u64)
    }

    fn seek(&mut self, pagenum: u64) -> Result<()> {
        let num_pages = self.num_pages()?;
        if num_pages < pagenum {
            let err_str = format!(
                "Page {} does not exist. Total pages: {}",
                pagenum, num_pages
            );
            Err(Error::new(ErrorKind::NotFound, err_str))
        } else {
            self.file.seek(SeekFrom::Start(
                HEADER_LEN as u64 + pagenum * PAGE_SIZE as u64,
            ))?;
            Ok(())
        }
    }

    /// Read the given page from the file into a new Page buffer.
    pub fn read_page_alloc(&mut self, pagenum: u64) -> Result<Page<PAGE_SIZE>> {
        let mut result = Page::<PAGE_SIZE>::new();
        self.read_page(pagenum, &mut result)?;
        Ok(result)
    }

    /// Read the given page from the file into the given Page buffer.
    pub fn read_page(&mut self, pagenum: u64, page: &mut Page<PAGE_SIZE>) -> Result<()> {
        self.seek(pagenum)?;
        self.file.read_exact(page.as_mut_buf())?;
        Ok(())
    }

    /// Write to the given page in the file.
    pub fn write_page(&mut self, pagenum: u64, page: &Page<PAGE_SIZE>) -> Result<()> {
        self.seek(pagenum)?;
        self.file.write_all(page.as_buf())
    }

    /// Appends a new page to the file.
    pub fn append_page(&mut self, page: &Page<PAGE_SIZE>) -> Result<()> {
        self.file.seek(SeekFrom::End(0))?;
        self.file.write_all(page.as_buf())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    const PAGE_SIZE: usize = 16;
    type Pf = PagedFile<PAGE_SIZE>;
    type P = Page<PAGE_SIZE>;

    #[test]
    fn pf_init_test() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("testfile");
        Pf::create(&file_path).unwrap();

        // Open the empty file
        let mut handle = Pf::open(&file_path.as_path()).unwrap();
        // Ensure no pages exist, and reading/writing non-existent pages fails
        assert_eq!(handle.num_pages().unwrap(), 0);
        assert!(handle.read_page_alloc(0).is_err());
        assert!(handle.write_page(10, &P::new()).is_err());
    }

    #[test]
    fn pf_write_test() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("testfile");
        Pf::create(&file_path).unwrap();

        let mut handle = Pf::open(&file_path.as_path()).unwrap();
        let page = P::new();

        // Create 3 pages
        handle.append_page(&page).unwrap();
        assert_eq!(handle.num_pages().unwrap(), 1);
        handle.append_page(&page).unwrap();
        assert_eq!(handle.num_pages().unwrap(), 2);
        handle.append_page(&page).unwrap();
        assert_eq!(handle.num_pages().unwrap(), 3);

        // Modify pages one by one and verify they are updated correctly
        let page = P::new_from_buf([128; PAGE_SIZE]);

        handle.write_page(0, &page).unwrap();
        assert_eq!(handle.num_pages().unwrap(), 3);
        assert_eq!(
            *handle.read_page_alloc(0).unwrap().as_buf(),
            [128; PAGE_SIZE]
        );
        assert_eq!(*handle.read_page_alloc(1).unwrap().as_buf(), [0; PAGE_SIZE]);
        assert_eq!(*handle.read_page_alloc(2).unwrap().as_buf(), [0; PAGE_SIZE]);

        let page = P::new_from_buf([64; PAGE_SIZE]);

        handle.write_page(1, &page).unwrap();
        assert_eq!(handle.num_pages().unwrap(), 3);
        assert_eq!(
            *handle.read_page_alloc(0).unwrap().as_buf(),
            [128; PAGE_SIZE]
        );
        assert_eq!(
            *handle.read_page_alloc(1).unwrap().as_buf(),
            [64; PAGE_SIZE]
        );
        assert_eq!(*handle.read_page_alloc(2).unwrap().as_buf(), [0; PAGE_SIZE]);

        let page = P::new_from_buf([16; PAGE_SIZE]);

        handle.write_page(2, &page).unwrap();
        assert_eq!(handle.num_pages().unwrap(), 3);
        assert_eq!(
            *handle.read_page_alloc(0).unwrap().as_buf(),
            [128; PAGE_SIZE]
        );
        assert_eq!(
            *handle.read_page_alloc(1).unwrap().as_buf(),
            [64; PAGE_SIZE]
        );
        assert_eq!(
            *handle.read_page_alloc(2).unwrap().as_buf(),
            [16; PAGE_SIZE]
        );

        // Finally append one more page with data
        // Check that a new page is added, and existing pages aren't modified
        let page = P::new_from_buf([1; PAGE_SIZE]);
        handle.append_page(&page).unwrap();
        assert_eq!(handle.num_pages().unwrap(), 4);
        assert_eq!(
            *handle.read_page_alloc(0).unwrap().as_buf(),
            [128; PAGE_SIZE]
        );
        assert_eq!(
            *handle.read_page_alloc(1).unwrap().as_buf(),
            [64; PAGE_SIZE]
        );
        assert_eq!(
            *handle.read_page_alloc(2).unwrap().as_buf(),
            [16; PAGE_SIZE]
        );
        assert_eq!(*handle.read_page_alloc(3).unwrap().as_buf(), [1; PAGE_SIZE]);

        // Close the handle and open the same file again to verify contents were written to disk
        drop(handle);
        let mut handle = Pf::open(&file_path.as_path()).unwrap();
        assert_eq!(handle.num_pages().unwrap(), 4);
        assert_eq!(
            *handle.read_page_alloc(0).unwrap().as_buf(),
            [128; PAGE_SIZE]
        );
        assert_eq!(
            *handle.read_page_alloc(1).unwrap().as_buf(),
            [64; PAGE_SIZE]
        );
        assert_eq!(
            *handle.read_page_alloc(2).unwrap().as_buf(),
            [16; PAGE_SIZE]
        );
        assert_eq!(*handle.read_page_alloc(3).unwrap().as_buf(), [1; PAGE_SIZE]);
    }

    #[test]
    fn pf_hdr_fail_test() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("testfile");
        Pf::create(&file_path).unwrap();

        // Verify we get a page size mismatch error
        let err = PagedFile::<5000>::open(&file_path.as_path());
        assert!(err.is_err());
        match err {
            Ok(_) => {}
            Err(e) => {
                let err_str = format!(
                    "Page size mismatch. Header: {}, Expected: {}",
                    PAGE_SIZE, 5000
                );
                assert_eq!(e.to_string(), err_str);
            }
        }
    }

    #[test]
    fn pf_double_create_test() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("testfile");
        Pf::create(&file_path).unwrap();

        let err = Pf::create(&file_path);
        assert!(err.is_err());
    }
}
