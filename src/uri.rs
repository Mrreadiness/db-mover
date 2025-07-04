use std::str::FromStr;

#[derive(Debug, Clone)]
pub enum URI {
    Sqlite(String),
    Postgres(String),
    Mysql(String),
}

impl FromStr for URI {
    type Err = String;

    fn from_str(s: &str) -> Result<URI, Self::Err> {
        if s.starts_with("sqlite://") {
            return Ok(URI::Sqlite(s.to_owned()));
        }
        if s.starts_with("postgres://") || s.starts_with("postgresql://") {
            return Ok(URI::Postgres(s.to_owned()));
        }
        if s.starts_with("mysql://") {
            return Ok(URI::Mysql(s.to_owned()));
        }
        return Err("Unknown URI format".to_string());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_uri_from_str_sqlite() {
        let uri = URI::from_str("sqlite://test.db");
        assert!(matches!(uri, Ok(URI::Sqlite(_))));
    }

    #[test]
    fn test_uri_from_str_postgres() {
        let uri = URI::from_str("postgres://user:pass@localhost/db");
        assert!(matches!(uri, Ok(URI::Postgres(_))));
    }

    #[test]
    fn test_uri_from_str_mysql() {
        let uri = URI::from_str("mysql://user@localhost:3306");
        assert!(matches!(uri, Ok(URI::Mysql(_))));
    }

    #[test]
    fn test_uri_from_str_invalid() {
        let uri = URI::from_str("invalid://test");
        assert!(uri.is_err());
    }
}
