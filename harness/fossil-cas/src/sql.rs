// Copyright 2026 The Jujutsu Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The portable SQLite surface.
//!
//! This is deliberately the lowest common denominator of rusqlite and a
//! Durable Object's `ctx.storage.sql`:
//!
//! * positional `?` parameters, at most 100 per statement;
//! * no `BEGIN`/`SAVEPOINT` in SQL text. Atomicity is a [`SqlConn::scope`]
//!   callback, which maps onto `transactionSync()` on a Durable Object;
//! * values are exactly SQLite's storage classes;
//! * every integer, in parameters and in results, lies within
//!   ±[`MAX_SAFE_INT`], because a Durable Object passes numbers through JS
//!   doubles. This covers every integer column (row ids, sizes, encodings,
//!   chunk sequence numbers, kinds, flags, timestamps in milliseconds);
//!   hashes and ids are always text or blobs. Connections reject violations
//!   rather than silently losing precision.
//!
//! The executor handed to a scope has no way to open another scope, so nested
//! transactions (and the deadlocks they cause) are impossible by construction.

use std::fmt::Debug;

use thiserror::Error;

/// The largest integer magnitude exactly representable on every connection:
/// 2^53 - 1, the largest safe integer of a JS double.
pub const MAX_SAFE_INT: i64 = (1 << 53) - 1;

/// Rejects integer parameters outside ±[`MAX_SAFE_INT`]. Every [`SqlConn`]
/// implementation calls this before running a statement.
pub fn check_params(params: &[Param<'_>]) -> SqlResult<()> {
    for (i, param) in params.iter().enumerate() {
        if let Param::Int(n) = param
            && n.unsigned_abs() > MAX_SAFE_INT as u64
        {
            return Err(SqlError(format!(
                "parameter {i}: integer {n} is outside the safe range ±2^53"
            )));
        }
    }
    Ok(())
}

/// A borrowed statement parameter.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Param<'a> {
    /// SQL `NULL`.
    Null,
    /// An integer within ±[`MAX_SAFE_INT`].
    Int(i64),
    /// UTF-8 text.
    Text(&'a str),
    /// Raw bytes.
    Blob(&'a [u8]),
}

/// An owned result value.
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    /// SQL `NULL`.
    Null,
    /// A 64-bit integer.
    Int(i64),
    /// A floating point number. Durable Objects report every number this way.
    Real(f64),
    /// UTF-8 text.
    Text(String),
    /// Raw bytes.
    Blob(Vec<u8>),
}

/// One result row, with columns in `SELECT` order.
#[derive(Clone, Debug, PartialEq)]
pub struct Row(pub Vec<Value>);

impl Row {
    fn get(&self, i: usize) -> SqlResult<&Value> {
        self.0
            .get(i)
            .ok_or_else(|| SqlError(format!("column {i} out of range")))
    }

    /// Returns column `i` as an integer. Integral reals are accepted because a
    /// Durable Object returns every number as a JS double.
    pub fn int(&self, i: usize) -> SqlResult<i64> {
        match self.get(i)? {
            Value::Int(n) if n.unsigned_abs() <= MAX_SAFE_INT as u64 => Ok(*n),
            #[expect(clippy::cast_possible_truncation)]
            Value::Real(f) if f.fract() == 0.0 && f.abs() <= MAX_SAFE_INT as f64 => Ok(*f as i64),
            other => Err(SqlError(format!(
                "column {i}: expected integer, got {other:?}"
            ))),
        }
    }

    /// Returns column `i` as bytes. `NULL` is an error.
    pub fn blob(&self, i: usize) -> SqlResult<&[u8]> {
        match self.get(i)? {
            Value::Blob(b) => Ok(b),
            Value::Text(s) => Ok(s.as_bytes()),
            other => Err(SqlError(format!(
                "column {i}: expected blob, got {other:?}"
            ))),
        }
    }

    /// Returns column `i` as text. `NULL` is an error.
    pub fn text(&self, i: usize) -> SqlResult<&str> {
        match self.get(i)? {
            Value::Text(s) => Ok(s),
            other => Err(SqlError(format!(
                "column {i}: expected text, got {other:?}"
            ))),
        }
    }

    /// Whether column `i` is `NULL`.
    pub fn is_null(&self, i: usize) -> SqlResult<bool> {
        Ok(matches!(self.get(i)?, Value::Null))
    }
}

/// An error reported by the SQL layer.
#[derive(Clone, Debug, Error)]
#[error("sqlite: {0}")]
pub struct SqlError(pub String);

/// Result of SQL operations.
pub type SqlResult<T> = Result<T, SqlError>;

/// Whether a scope may write.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Access {
    /// No transaction is opened. Each statement sees committed data.
    Read,
    /// The whole scope is one atomic transaction, rolled back on error.
    Write,
}

/// Executes statements inside a [`SqlConn::scope`].
pub trait SqlExec {
    /// Runs a statement and returns the number of rows changed.
    fn exec(&self, sql: &str, params: &[Param<'_>]) -> SqlResult<u64>;

    /// Runs several `;`-separated statements that take no parameters.
    fn exec_batch(&self, sql: &str) -> SqlResult<()>;

    /// Runs a query and returns all rows.
    fn query(&self, sql: &str, params: &[Param<'_>]) -> SqlResult<Vec<Row>>;

    /// Runs a query and returns the first row, if any.
    fn query_row(&self, sql: &str, params: &[Param<'_>]) -> SqlResult<Option<Row>> {
        Ok(self.query(sql, params)?.into_iter().next())
    }
}

/// A connection to one SQLite database.
pub trait SqlConn: Send + Sync + Debug {
    /// Runs `f` with an executor. With [`Access::Write`], everything `f` does
    /// is one transaction that commits iff `f` returns `Ok`.
    fn scope(
        &self,
        access: Access,
        f: &mut dyn FnMut(&dyn SqlExec) -> SqlResult<()>,
    ) -> SqlResult<()>;

    /// The largest string or blob value this database accepts. A Durable
    /// Object caps values (and rows) at 2 MB; native SQLite at about 1 GB.
    fn max_value_len(&self) -> usize;
}

/// Runs `f` in a scope of `conn` and returns its result.
///
/// Any error type convertible from [`SqlError`] can be returned from `f`; an
/// error aborts (and for [`Access::Write`] rolls back) the scope and is passed
/// through unchanged.
pub fn with<T, E: From<SqlError>>(
    conn: &dyn SqlConn,
    access: Access,
    f: impl FnOnce(&dyn SqlExec) -> Result<T, E>,
) -> Result<T, E> {
    let mut f = Some(f);
    let mut outcome: Option<Result<T, E>> = None;
    let scope_result = conn.scope(access, &mut |x| {
        let f = f.take().expect("scope callback must run at most once");
        match f(x) {
            Ok(value) => {
                outcome = Some(Ok(value));
                Ok(())
            }
            Err(err) => {
                outcome = Some(Err(err));
                Err(SqlError("scope aborted".to_owned()))
            }
        }
    });
    match (outcome, scope_result) {
        // The callback's own error wins over the synthetic abort error.
        (Some(Err(err)), _) => Err(err),
        (_, Err(err)) => Err(err.into()),
        (Some(Ok(value)), Ok(())) => Ok(value),
        (None, Ok(())) => Err(SqlError("scope did not run its callback".to_owned()).into()),
    }
}

#[cfg(feature = "rusqlite")]
pub use self::native::RusqliteConn;

#[cfg(feature = "rusqlite")]
mod native {
    use std::path::Path;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::Weak;

    use rusqlite::Connection;
    use rusqlite::types::ToSqlOutput;
    use rusqlite::types::ValueRef;

    use super::Access;
    use super::Param;
    use super::Row;
    use super::SqlConn;
    use super::SqlError;
    use super::SqlExec;
    use super::SqlResult;
    use super::Value;
    use super::check_params;

    impl From<rusqlite::Error> for SqlError {
        fn from(err: rusqlite::Error) -> Self {
            Self(err.to_string())
        }
    }

    /// A native SQLite connection.
    ///
    /// The connection sits behind a mutex held for the whole of each scope, so
    /// scopes from different threads are serialised, like on a Durable Object.
    #[derive(Debug)]
    pub struct RusqliteConn {
        conn: Mutex<Connection>,
        max_value_len: usize,
    }

    impl RusqliteConn {
        /// Opens (creating if needed) the database at `path`.
        pub fn open(path: &Path) -> SqlResult<Self> {
            let conn = Connection::open(path)?;
            conn.pragma_update(None, "journal_mode", "WAL")?;
            conn.pragma_update(None, "synchronous", "NORMAL")?;
            Self::new(conn)
        }

        /// Returns the process-wide connection for the database at `path`,
        /// opening it on first use. Every store of one repo shares it, so
        /// their scopes are serialised on one connection, as on a Durable
        /// Object.
        pub fn shared(path: &Path) -> SqlResult<Arc<Self>> {
            static OPEN: Mutex<Vec<(PathBuf, Weak<RusqliteConn>)>> = Mutex::new(Vec::new());
            // Canonicalise the directory: the file itself may not exist yet.
            let dir = path.parent().unwrap_or(Path::new("."));
            let key = std::fs::canonicalize(dir)
                .map_err(|err| SqlError(format!("{}: {err}", dir.display())))?
                .join(path.file_name().unwrap_or_default());
            let mut open = OPEN
                .lock()
                .map_err(|_| SqlError("connection registry poisoned".to_owned()))?;
            open.retain(|(_, conn)| conn.strong_count() > 0);
            if let Some(conn) = open
                .iter()
                .find(|(p, _)| *p == key)
                .and_then(|(_, conn)| conn.upgrade())
            {
                return Ok(conn);
            }
            let conn = Arc::new(Self::open(&key)?);
            open.push((key, Arc::downgrade(&conn)));
            Ok(conn)
        }

        /// Opens a private in-memory database.
        pub fn open_in_memory() -> SqlResult<Self> {
            Self::new(Connection::open_in_memory()?)
        }

        fn new(conn: Connection) -> SqlResult<Self> {
            conn.busy_timeout(std::time::Duration::from_secs(10))?;
            Ok(Self {
                conn: Mutex::new(conn),
                // SQLITE_MAX_LENGTH, the default string/blob limit.
                max_value_len: 1_000_000_000,
            })
        }

        /// Pretends values larger than `n` bytes are rejected, the way a
        /// Durable Object rejects values over 2 MB. Used to exercise chunking.
        pub fn with_max_value_len(mut self, n: usize) -> Self {
            self.max_value_len = n;
            self
        }
    }

    impl SqlConn for RusqliteConn {
        fn scope(
            &self,
            access: Access,
            f: &mut dyn FnMut(&dyn SqlExec) -> SqlResult<()>,
        ) -> SqlResult<()> {
            let conn = self
                .conn
                .lock()
                .map_err(|_| SqlError("connection mutex poisoned".to_owned()))?;
            let exec = Exec(&conn);
            match access {
                Access::Read => f(&exec),
                Access::Write => {
                    conn.execute_batch("BEGIN IMMEDIATE")?;
                    match f(&exec) {
                        Ok(()) => {
                            conn.execute_batch("COMMIT")?;
                            Ok(())
                        }
                        Err(err) => {
                            conn.execute_batch("ROLLBACK")?;
                            Err(err)
                        }
                    }
                }
            }
        }

        fn max_value_len(&self) -> usize {
            self.max_value_len
        }
    }

    struct Exec<'a>(&'a Connection);

    fn to_sql<'a>(params: &[Param<'a>]) -> Vec<ToSqlOutput<'a>> {
        params
            .iter()
            .map(|p| {
                ToSqlOutput::Borrowed(match *p {
                    Param::Null => ValueRef::Null,
                    Param::Int(n) => ValueRef::Integer(n),
                    Param::Text(s) => ValueRef::Text(s.as_bytes()),
                    Param::Blob(b) => ValueRef::Blob(b),
                })
            })
            .collect()
    }

    fn from_sql(value: ValueRef<'_>) -> SqlResult<Value> {
        Ok(match value {
            ValueRef::Null => Value::Null,
            ValueRef::Integer(n) => Value::Int(n),
            ValueRef::Real(f) => Value::Real(f),
            ValueRef::Text(t) => Value::Text(
                str::from_utf8(t)
                    .map_err(|err| SqlError(err.to_string()))?
                    .to_owned(),
            ),
            ValueRef::Blob(b) => Value::Blob(b.to_vec()),
        })
    }

    impl SqlExec for Exec<'_> {
        fn exec(&self, sql: &str, params: &[Param<'_>]) -> SqlResult<u64> {
            check_params(params)?;
            let mut stmt = self.0.prepare_cached(sql)?;
            let n = stmt.execute(rusqlite::params_from_iter(to_sql(params)))?;
            Ok(n as u64)
        }

        fn exec_batch(&self, sql: &str) -> SqlResult<()> {
            Ok(self.0.execute_batch(sql)?)
        }

        fn query(&self, sql: &str, params: &[Param<'_>]) -> SqlResult<Vec<Row>> {
            check_params(params)?;
            let mut stmt = self.0.prepare_cached(sql)?;
            let ncols = stmt.column_count();
            let mut rows = stmt.query(rusqlite::params_from_iter(to_sql(params)))?;
            let mut out = Vec::new();
            while let Some(row) = rows.next()? {
                let values = (0..ncols)
                    .map(|i| from_sql(row.get_ref(i)?))
                    .collect::<SqlResult<Vec<_>>>()?;
                out.push(Row(values));
            }
            Ok(out)
        }
    }
}

#[cfg(all(test, feature = "rusqlite"))]
mod tests {
    use super::*;

    fn setup() -> RusqliteConn {
        let conn = RusqliteConn::open_in_memory().unwrap();
        with(&conn, Access::Write, |x| {
            x.exec_batch("CREATE TABLE t(k INTEGER PRIMARY KEY, v BLOB)")
        })
        .unwrap();
        conn
    }

    fn count(conn: &RusqliteConn) -> i64 {
        with(conn, Access::Read, |x| {
            x.query_row("SELECT count(*) FROM t", &[])?.unwrap().int(0)
        })
        .unwrap()
    }

    #[test]
    fn write_scope_commits() {
        let conn = setup();
        with(&conn, Access::Write, |x| {
            x.exec("INSERT INTO t(v) VALUES (?)", &[Param::Blob(b"abc")])
        })
        .unwrap();
        assert_eq!(count(&conn), 1);
    }

    #[test]
    fn write_scope_rolls_back_and_passes_error_through() {
        #[derive(Debug, PartialEq)]
        struct Mine;
        impl From<SqlError> for Mine {
            fn from(_: SqlError) -> Self {
                panic!("callback error must be passed through unchanged")
            }
        }
        let conn = setup();
        let result: Result<(), Mine> = with(&conn, Access::Write, |x| {
            x.exec("INSERT INTO t(v) VALUES (?)", &[Param::Null])
                .unwrap();
            Err(Mine)
        });
        assert_eq!(result, Err(Mine));
        assert_eq!(count(&conn), 0);
    }

    #[test]
    fn values_round_trip() {
        let conn = setup();
        let row = with(&conn, Access::Read, |x| {
            x.query_row(
                "SELECT ?, ?, ?, ?, 1.5",
                &[
                    Param::Int(-7),
                    Param::Text("hé"),
                    Param::Blob(&[0, 1, 2]),
                    Param::Null,
                ],
            )
        })
        .unwrap()
        .unwrap();
        assert_eq!(row.int(0).unwrap(), -7);
        assert_eq!(row.text(1).unwrap(), "hé");
        assert_eq!(row.blob(2).unwrap(), &[0, 1, 2]);
        assert!(row.is_null(3).unwrap());
        assert_eq!(row.0[4], Value::Real(1.5));
        assert!(row.int(4).is_err());
    }

    #[test]
    fn integers_outside_the_safe_range_are_rejected() {
        let conn = setup();
        let too_big = MAX_SAFE_INT + 1;
        let result = with(&conn, Access::Write, |x| {
            x.exec("INSERT INTO t(k) VALUES (?)", &[Param::Int(too_big)])
        });
        assert!(result.is_err());
        let result = with(&conn, Access::Read, |x| {
            x.query_row("SELECT ?", &[Param::Int(-too_big)])
        });
        assert!(result.is_err());
        // Results are checked too.
        let row = with(&conn, Access::Read, |x| {
            x.query_row("SELECT 9007199254740993", &[])
        })
        .unwrap()
        .unwrap();
        assert!(row.int(0).is_err());
        assert!(
            Row(vec![Value::Real(9_007_199_254_740_992.0)])
                .int(0)
                .is_err()
        );
        assert_eq!(
            Row(vec![Value::Int(MAX_SAFE_INT)]).int(0).unwrap(),
            MAX_SAFE_INT
        );
    }

    #[test]
    fn integral_real_reads_as_int() {
        let row = Row(vec![Value::Real(42.0)]);
        assert_eq!(row.int(0).unwrap(), 42);
    }
}
