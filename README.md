Sharding
====

**注意**：本项目是实验性项目：尝试在应用层实现DuckDB分区表。仅作为思路参考。

## 一、实验目标

要求数据用『年月』作为分区键存储在多个数据库中，并提供以下两个RESTful接口：

1. 新增数据接口：根据分区键将数据保存到相应的数据库文件中。
2. 查询数据接口：查询近12个月内的热数据，超出时间范围的不展示。

## 二、设计思路

### 2.1 数据实体

程序将管理用户（User）的基本信息，其中用户表（`users`）包含以下三个字段：

1. `id`【`bigint`】：唯一编码。
2. `name`【`text`】：用户名称。
3. `registered_date`【`date`】：注册日期。

### 2.2 分区逻辑

将用户注册日期的年份与月份作为分区键，每个分库作为独立的DuckDB数据库文件保存在`repositories`目录下。例如：

* `repositories/202506.db`：保存注册日期在$[2025-06-01, 2025-06-30]$区间内的用户信息。
* `repositories/202507.db`：保存注册日期在$[2025-07-01, 2025-07-31]$区间内的用户信息。
* 以此类推。

### 2.3 接口功能

程序还提供以下两个RESTful接口：

1. GET `/users`：返回注册日期在一年内的用户信息，即只返回一年之内的热数据。
2. POST `/users`：新建用户信息，按照用户的注册日期将数据保存到对应的分库中。

## 三、安装使用

### 3.1 构建程序

本程序需要通过编译安装，需要先按照Rust官方的[安装步骤](https://www.rust-lang.org/zh-CN/learn/get-started)搭建Rust开发环境。
然后按照以下命令编译程序：

```shell
git clone https://github.com/redraiment/duckdb-sharding
cd duckdb-sharding
cargo build --release
```

本程序依赖了`actix-web`与`duckdb`，构建时间较长，请耐心等待。

### 3.2 运行程序

构建成功后用以下命令启动程序：

```shell
target/release/sharding
```

如果在当前目录下看到新建了一个`repositories`目录，说明程序已经启动成功。
并且后续的分区数据库都保存在该目录下。

程序如果启动成功，则开始监听本地的8080端口。

## 四、测试程序

本轮测试于2025年07月27日用`curl`命令完成测试，因为程序返回结果与当前日期相关，有可能展示结果与您测试的实际输出不完全一致，请注意日期范围。

### 4.1 初始数据

```shell
curl http://localhost:8080/users
```

期望结果：

```shell
[]
```

即当前`users`表为空。

### 4.2 创建用户

**用户1**：

```shell
curl -X POST http://localhost:8080/users \
     -H 'Content-Type: application/json' \
     -d '{"id":1,"name":"Joe","registered_date":"2025-07-25"}'
```

期望结果：

```shell
{"id":1,"name":"Joe","registered_date":"2025-07-25"}
```

**用户2**：

```shell
curl -X POST http://localhost:8080/users \
     -H 'Content-Type: application/json' \
     -d '{"id":2,"name":"Alice","registered_date":"2025-05-13"}'
```

期望结果：

```shell
{"id":2,"name":"Alice","registered_date":"2025-05-13"}
```

**用户3**：

```shell
curl -X POST http://localhost:8080/users \
     -H 'Content-Type: application/json' \
     -d '{"id":3,"name":"Wiki","registered_date":"2024-01-02"}'
```

期望结果：

```shell
{"id":3,"name":"Wiki","registered_date":"2024-01-02"}
```

同时`repositories`目录下会出现`202401.db`、`202505`、`202507.db`三个数据库文件。

## 4.3 查询数据

```shell
curl http://localhost:8080/users
```

期望结果：

```shell
[
  {"id":2,"name":"Alice","registered_date":"2025-05-13"},
  {"id":1,"name":"Joe","registered_date":"2025-07-25"}
]
```

其中第三个用户（注册日期为2024年01月02日）因为距离测试日期（2025年07月27日）超出了1年时间，被视为『冷数据』，不在结果中展示。

## 五、讨论交流

本项目仅作为Rust学习和思路验证，分区功能更适合做成DuckDB的插件对应用端透明。
如果您相关的想法或建议，欢迎提交Pull Request，或发送邮件到 [redraiment@gmail.com](mailto:redraiment@gmail.com) 交流。
