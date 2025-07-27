/// 数据实体
use chrono::NaiveDate;
use duckdb::{Result, Row};
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;

/// 用户实体：同时用于接口的输入与输出。
#[derive(Serialize, Deserialize, Debug)]
pub struct User {
    pub id: i64,                    // 编号
    pub name: String,               // 名称
    pub registered_date: NaiveDate, // 注册日期：用于分区
}

/// 将DuckDB的查询结果Row转成User实体。
impl<'stmt> TryFrom<&Row<'stmt>> for User {
    type Error = duckdb::Error;

    fn try_from(value: &Row<'stmt>) -> Result<User> {
        Ok(User {
            id: value.get(0)?,
            name: value.get(1)?,
            registered_date: value.get(2)?,
        })
    }
}
