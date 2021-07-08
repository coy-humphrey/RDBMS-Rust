pub struct Bitmap {
    bmp: Vec<u8>,
    size: usize,
}

impl Bitmap {
    pub fn new(size: usize) -> Self {
        let bmp = vec![0; Self::bmp_size_in_bytes(size)];
        Self { bmp, size }
    }

    pub fn new_with_vec(size: usize, bmp: Vec<u8>) -> Self {
        let size_check = Self::bmp_size_in_bytes(size);
        if size_check != bmp.len() {
            panic!()
        }
        Self { bmp, size }
    }

    pub fn bmp_size_in_bytes(size: usize) -> usize {
        let quot = size / 8;
        let rem = size % 8;
        // If there's any remainder, round up
        if rem != 0 {
            quot + 1
        } else {
            quot
        }
    }

    pub fn set(&mut self, idx: usize) {
        if idx >= self.size {
            panic!()
        }
        let byte_idx = idx / 8;
        let bit_idx = idx % 8;
        let mask = 1 << bit_idx;
        self.bmp[byte_idx] |= mask;
    }

    pub fn get(&self, idx: usize) -> bool {
        if idx >= self.size {
            panic!()
        }
        let byte_idx = idx / 8;
        let bit_idx = idx % 8;
        let mask = 1 << bit_idx;
        self.bmp[byte_idx] & mask != 0
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.bmp
    }
}
