use super::testable_database::TestableDatabase;

fn create_test_tables(in_db: &mut impl TestableDatabase, out_db: &mut impl TestableDatabase) {
    in_db.create_test_table("test");
    in_db.create_test_table("test1");
    out_db.create_test_table("test");
    out_db.create_test_table("test1");
}

pub fn empty(mut in_db: impl TestableDatabase, mut out_db: impl TestableDatabase) {
    create_test_tables(&mut in_db, &mut out_db);
    assert_eq!(in_db.get_all_rows("test"), out_db.get_all_rows("test"));
    assert_eq!(in_db.get_all_rows("test1"), out_db.get_all_rows("test1"));

    let args = db_mover::args::Args::new(in_db.get_uri(), out_db.get_uri());
    db_mover::run(args).unwrap();

    assert_eq!(in_db.get_all_rows("test"), out_db.get_all_rows("test"));
    assert_eq!(in_db.get_all_rows("test1"), out_db.get_all_rows("test1"));

    let mut args = db_mover::args::Args::new(in_db.get_uri(), out_db.get_uri());
    args.table.push("test".to_string());
    db_mover::run(args).unwrap();

    assert_eq!(in_db.get_all_rows("test"), out_db.get_all_rows("test"));
    assert_eq!(in_db.get_all_rows("test1"), out_db.get_all_rows("test1"));
}

pub fn one_table(mut in_db: impl TestableDatabase, mut out_db: impl TestableDatabase) {
    create_test_tables(&mut in_db, &mut out_db);
    in_db.fill_test_table("test", 1000);
    assert_ne!(in_db.get_all_rows("test"), out_db.get_all_rows("test"));
    assert_eq!(in_db.get_all_rows("test1"), out_db.get_all_rows("test1"));

    let mut args = db_mover::args::Args::new(in_db.get_uri(), out_db.get_uri());
    args.table.push("test".to_string());
    db_mover::run(args).unwrap();

    assert_eq!(in_db.get_all_rows("test"), out_db.get_all_rows("test"));
    assert_eq!(in_db.get_all_rows("test1"), out_db.get_all_rows("test1"));
}

pub fn multiple_tables(mut in_db: impl TestableDatabase, mut out_db: impl TestableDatabase) {
    create_test_tables(&mut in_db, &mut out_db);
    in_db.fill_test_table("test", 1000);
    in_db.fill_test_table("test1", 100);
    assert_ne!(in_db.get_all_rows("test"), out_db.get_all_rows("test"));
    assert_ne!(in_db.get_all_rows("test1"), out_db.get_all_rows("test1"));

    let mut args = db_mover::args::Args::new(in_db.get_uri(), out_db.get_uri());
    args.table.push("test".to_string());
    args.table.push("test1".to_string());
    db_mover::run(args).unwrap();

    assert_eq!(in_db.get_all_rows("test"), out_db.get_all_rows("test"));
    assert_eq!(in_db.get_all_rows("test1"), out_db.get_all_rows("test1"));
}
