/// 分区仓库。
/// 因为没有专门的『年月』类型，内部使用`NaiveDate`日期作为分区键，其中『日』固定为1号。
use crate::entity::User;
use anyhow::{Context, Result};
use async_duckdb::{Client, ClientBuilder};
use chrono::{Datelike, Duration, Local, Months, NaiveDate};
use duckdb::params;
use glob::{glob, GlobResult};
use std::collections::HashSet;
use std::fs::create_dir_all;
use std::path::{Path, PathBuf};
use std::sync::Mutex;


/// 分区数据库存放目录
const PARTITION_FOLDER: &str = "repositories";
/// 热数据窗口时长
const WINDOW_MONTHS: u32 = 12; // 保留最近12个月的数据

/// 不同类型的对象转成分区键。
trait ToPartitionKey {
    fn to_partition_key(&self) -> Option<NaiveDate>;
}

/// 将repositories/*.db格式的文件名转换成分区键。
impl ToPartitionKey for PathBuf {
    fn to_partition_key(&self) -> Option<NaiveDate> {
        let datetime = format!("{}01", self.file_stem()?.to_str()?);
        NaiveDate::parse_from_str(datetime.as_str(), "%Y%m%d").ok()
    }
}

/// 将任意日期转换成分区键。
impl ToPartitionKey for NaiveDate {
    fn to_partition_key(&self) -> Option<NaiveDate> {
        NaiveDate::from_ymd_opt(self.year(), self.month(), 1)
    }
}

/// 热数据日期范围
fn window() -> (NaiveDate, NaiveDate) {
    let today = Local::now().date_naive();
    let first_day_of_month = NaiveDate::from_ymd_opt(today.year(), today.month(), 1).unwrap();
    (
        first_day_of_month - Months::new(WINDOW_MONTHS),
        first_day_of_month + Months::new(1) - Duration::days(1),
    )
}

/// 从指定分区目录下加载数据集。
/// 理论上只需要加载热数据即可；但为了方便测试，本DEMO也会加载冷数据，用于插入写数据。
fn load() -> HashSet<NaiveDate> {
    let folder = Path::new(PARTITION_FOLDER);
    create_dir_all(folder).unwrap(); // 自动创建分区目录
    folder
        .join("[0-9][0-9][0-9][0-9][01][0-9].db") // 匹配年月格式的文件
        .to_str()
        .map(glob)
        .and_then(Result::ok)
        .map(|paths| paths.filter_map(GlobResult::ok))
        .map(|paths| paths.filter_map(|path| path.to_partition_key())) // 将文件名转成分区键
        .map(|paths| paths.collect())
        .unwrap_or(HashSet::new())
}

/// 内存数据仓库：通过内存数据库ATTACH所有分区数据库，将结果在内存中合并成一张大表。
pub struct Repository {
    client: Client,                       // 内存数据库的客户端。
    databases: Mutex<HashSet<NaiveDate>>, // 当前已经Attach分库所属年月。
}

impl Repository {

    /// 新建仓库。
    /// 创建内存中DuckDB客户端。
    /// ATTACH所有分区数据库。
    pub async fn new() -> Result<Repository> {
        let repository = Repository {
            client: ClientBuilder::new().open().await?,
            databases: Mutex::new(HashSet::new()),
        };
        for partition in load() {
            repository.attach(partition).await?;
        }
        Ok(repository)
    }

    /// 若`date`日期所属的分区尚未ATTACH到当前内存数据库中，则尝试ATTACH并初始化。
    async fn attach(&self, date: NaiveDate) -> Result<()> {
        let mut databases = self.databases.lock().unwrap();
        let database = date.to_partition_key().unwrap();
        if !databases.contains(&database) {
            let key = &date.format("%Y%m").to_string();
            let mut filename = Path::new(PARTITION_FOLDER).join(key);
            filename.set_extension("db");
            let sql = format!(r#"
                    attach if not exists '{filename}' as "{key}";
                    create table if not exists "{key}".users (
                      id bigint,
                      name text not null,
                      registered_date date not null,
                    );
                "#,
                filename = filename.to_string_lossy(),
                key = key
            );
            self.client
                .conn(move |connection| connection.execute_batch(sql.as_str()))
                .await?;
            databases.insert(database);
        }
        Ok(())
    }

    /// 将用户信息保存到分区数据库中。
    pub async fn create_user(&self, user: User) -> Result<User> {
        self.attach(user.registered_date).await?;
        let sql = user
            .registered_date
            .format("insert into \"%Y%m\".users values (?, ?, ?) returning *")
            .to_string();
        self.client
            .conn(move |connection| {
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
    /// TODO 优化View创建：当前每次查询都需要重建View，太耗费资源。
    /// TODO 1）在ATTACH时判断是否需要重建View。
    /// TODO 2）在View中有数据冷却（超出时效）时重建View。
    pub async fn list_users(&self) -> Result<Vec<User>> {
        let (first_date, last_date) = window();
        // 筛选出在热数据时间范围内的分区库
        let partitions: Vec<String> = self
            .databases
            .lock()
            .unwrap()
            .iter()
            .filter_map(|date| {
                if first_date <= *date && *date <= last_date {
                    Some(date.format("%Y%m").to_string())
                } else {
                    None
                }
            })
            .map(|key| format!(r#"select * from "{}".users"#, key))
            .collect();
        if partitions.is_empty() {
            Ok(Vec::new())
        } else {
            let mut sql = partitions.join("\nunion all\n");
            sql.insert_str(0, "create or replace view users as ");
            self.client
                .conn(move |connection| {
                    connection.execute_batch(sql.as_str())?;
                    let mut statement = connection.prepare("select * from users")?;
                    let mut rows = statement.query([])?;
                    let mut users = Vec::new();
                    while let Some(row) = rows.next()? {
                        users.push(User::try_from(row)?);
                    }
                    Ok(users)
                })
                .await
                .context("Failed to list users")
        }
    }
}
