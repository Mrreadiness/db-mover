use crate::uri::URI;

use super::row::TestRow;

pub trait TestableDatabase {
    fn get_uri(&self) -> URI;

    fn create_test_table(&mut self, table_name: &str);

    fn fill_test_table(&mut self, table_name: &str, num_rows: usize);

    fn get_all_rows(&mut self, table_name: &str) -> Vec<TestRow>;
}
