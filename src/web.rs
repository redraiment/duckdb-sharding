use crate::entity::User;
use crate::repository::Repository;
use actix_web::web::{get, post, scope, Data, Json};
use actix_web::{App, HttpResponse, HttpServer, Responder};
use anyhow::{Context, Result};

/// 查询用户：返回注册日期在一年内的用户列表。
async fn index(repository: Data<Repository>) -> impl Responder {
    match repository.list_users().await {
        Ok(users) => HttpResponse::Ok().json(users),
        Err(error) => HttpResponse::BadRequest().body(error.to_string()),
    }
}

/// 创建并返回用户。
async fn create(repository: Data<Repository>, user: Json<User>) -> impl Responder {
    match repository.create_user(User {
        id: user.id,
        name: user.name.clone(),
        registered_date: user.registered_date,
    }).await {
        Ok(user) => HttpResponse::Ok().json(user),
        Err(error) => HttpResponse::BadRequest().body(error.to_string()),
    }
}

/// 启动Web服务器：监听8080端口。
pub async fn start() -> Result<()> {
    let data = Data::new(Repository::new().await?);
    HttpServer::new(move || {
        App::new().app_data(data.clone()).service(
            scope("/users")
                .route("", get().to(index))
                .route("", post().to(create)),
        )
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
    .context("Start Web Server Failed")
}
