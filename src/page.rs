// Page

/// A buffer with a fixed page size.
pub struct Page<const PAGE_SIZE: usize> {
    boxed_buf: Box<[u8; PAGE_SIZE]>,
}

impl<const PAGE_SIZE: usize> Page<PAGE_SIZE> {
    /// Creates a new empty page.
    pub fn new() -> Self {
        Page {
            boxed_buf: Box::new([0; PAGE_SIZE]),
        }
    }

    /// Creates a new page from an existing buffer.
    pub fn new_from_buf(buf: [u8; PAGE_SIZE]) -> Self {
        Page {
            boxed_buf: Box::new(buf),
        }
    }

    /// Creates a new page from an existing Boxed buffer.
    pub fn new_from_boxed_buf(boxed_buf: Box<[u8; PAGE_SIZE]>) -> Self {
        Page { boxed_buf }
    }

    /// Consumes the page to return the underlying buffer.
    pub fn into_boxed_buf(self) -> Box<[u8; PAGE_SIZE]> {
        self.boxed_buf
    }

    pub fn as_buf(&self) -> &[u8; PAGE_SIZE] {
        &self.boxed_buf
    }

    pub fn as_mut_buf(&mut self) -> &mut [u8; PAGE_SIZE] {
        &mut self.boxed_buf
    }
}

impl<const PAGE_SIZE: usize> Default for Page<PAGE_SIZE> {
    fn default() -> Self {
        Page::<PAGE_SIZE>::new()
    }
}

#[cfg(test)]
mod tests {
    use crate::page::*;
    use std::mem;
    #[test]
    fn create_test() {
        let page = Page::<4096>::new();
        let another_page = Page::<4096>::new_from_buf([1; 4096]);
        assert_eq!(*another_page.as_buf(), [1; 4096]);
        assert_eq!(*page.as_buf(), [0; 4096]);
        assert_eq!(mem::size_of_val(&*page.as_buf()), 4096);
    }
    #[test]
    fn into_test() {
        let page = Page::<4096>::new();
        let mut buf = page.into_boxed_buf();
        for val in buf.iter_mut() {
            *val = 128;
        }
        let page = Page::<4096>::new_from_boxed_buf(buf);
        assert_eq!(*page.into_boxed_buf(), [128; 4096]);
    }

    #[test]
    fn mut_test() {
        let mut page = Page::<4096>::new();
        {
            let mut_buf = page.as_mut_buf();
            for val in mut_buf.iter_mut() {
                *val = 128;
            }
        }
        assert_eq!(*page.as_buf(), [128; 4096]);
    }

    #[test]
    fn default_test() {
        let page: Page<4096> = Default::default();
        assert_eq!(*page.as_buf(), [0; 4096]);
    }
}
