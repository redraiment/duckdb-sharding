/// 分区仓库。
/// 因为没有专门的『年月』类型，内部使用`NaiveDate`日期作为分区键，其中『日』固定为1号。
use crate::entity::User;
use anyhow::{anyhow, Context, Result};
use async_duckdb::{Client, ClientBuilder};
use duckdb::params;
use glob::{glob, GlobResult};
use std::fs::create_dir_all;
use std::path::{Path, PathBuf};

/// 分区数据库存放目录
const PARTITION_FOLDER: &str = "repositories";
/// 热数据窗口时长
const WINDOW_MONTHS: u32 = 12; // 保留最近12个月的数据

/// 从指定分区目录下加载数据集。
/// 理论上只需要加载热数据即可；但为了方便测试，本DEMO也会加载冷数据，用于插入写数据。
fn load() -> Vec<PathBuf> {
    let folder = Path::new(PARTITION_FOLDER);
    create_dir_all(folder).unwrap(); // 自动创建分区目录
    folder
        .join("[0-9][0-9][0-9][0-9][01][0-9].db") // 匹配年月格式的文件
        .to_str()
        .map(glob)
        .and_then(Result::ok)
        .map(|paths| paths.filter_map(GlobResult::ok))
        .map(|paths| paths.collect())
        .unwrap_or(Vec::new())
}

/// 内存数据仓库：通过内存数据库ATTACH所有分区数据库，将结果在内存中合并成一张大表。
pub struct Repository {
    client: Client, // 内存数据库的客户端。
}

impl Repository {
    /// 新建仓库。
    /// 创建内存中DuckDB客户端。
    /// ATTACH所有分区数据库。
    pub async fn new() -> Result<Repository> {
        let repository = Repository { client: ClientBuilder::new().open().await? };
        for path in load() {
            repository.attach(&path).await?;
        }
        Ok(repository)
    }

    /// 若`date`日期所属的分区尚未ATTACH到当前内存数据库中，则尝试ATTACH并初始化。
    async fn attach(&self, path: &PathBuf) -> Result<()> {
        let stem = path.file_stem().and_then(|stem| stem.to_str()).unwrap().to_owned();
        let name = path.to_str().unwrap().to_owned();
        if !stem.is_empty() && !name.is_empty() {
            self.client.conn(move |connection| {
                connection.query_row(
                    "select database_name from duckdb_databases() where database_name = ?",
                    params![stem],
                    |_| Ok(())
                ).or_else(|_| {
                    connection.execute_batch(&format!(r#"
                        attach if not exists '{path}' as "{stem}";
                        create table if not exists "{stem}".users (
                          id bigint,
                          name text not null,
                          registered_date date not null,
                        );
                    "#, stem = stem, path = name))
                })
            }).await.context(format!("Attach database {} failed!", path.display()))
        } else {
            Err(anyhow!("Empty database path is not allowed!"))
        }
    }

    /// 将用户信息保存到分区数据库中。
    pub async fn create_user(&self, user: User) -> Result<User> {
        let mut path = PathBuf::from(PARTITION_FOLDER);
        path.push(user.registered_date.format("%Y%m.db").to_string());
        self.attach(&path).await?;

        let sql = user
            .registered_date
            .format("insert into \"%Y%m\".users values (?, ?, ?) returning *")
            .to_string();
        self.client.conn(move |connection| {
            connection.query_row(
                sql.as_str(),
                params![&user.id, &user.name, &user.registered_date],
                |row| User::try_from(row),
            )
        })
        .await
        .context("Failed to create user")
    }

    /// 返回热数据集中的用户列表。
    pub async fn list_users(&self) -> Result<Vec<User>> {
        self.client.conn(|connection| {
            connection.query_row(r#"
              select
                string_agg(
                  format('select * from "{}".users', database_name),
                  ' union all '
                  order by database_name
                ) as sql
              from
                duckdb_databases()
              where
                database_name ~ '\d{6}'
                and make_date(
                  year(current_date),
                  month(current_date),
                  1
                ) - interval (? || ' months') <= strptime(database_name, '%Y%m')
            "#, params![WINDOW_MONTHS], |row| row.get(0)).and_then(|sql: String| {
                let mut statement = connection.prepare(sql.as_str())?;
                let mut rows = statement.query([])?;
                let mut users = Vec::new();
                while let Some(row) = rows.next()? {
                    users.push(User::try_from(row)?);
                }
                Ok(users)
            }).or(Ok(Vec::new()))
        }).await.context("List users failed")
    }
}
