mod web;
mod repository;
mod entity;

use crate::web::start;
use anyhow::Result;

#[actix_web::main]
async fn main() -> Result<()> {
    start().await // 启动Web服务
}
