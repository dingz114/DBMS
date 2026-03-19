# MiniDB: 简易内存数据库管理系统

MiniDB 是一个基于哈希表的轻量级内存数据库，使用 Flex/Bison 解析 SQL，支持多线程并发访问、事务（ACID）、持久化存储和网络服务。

## ✨ 功能特性

- **SQL 解析**：支持基本的 DDL/DML 语句
  - `CREATE TABLE` 创建表（支持 INT/VARCHAR 类型）
  - `INSERT` 插入数据
  - `SELECT` 查询（支持投影、条件、聚合函数、排序）
  - `UPDATE` / `DELETE` 更新/删除数据
  - `DROP TABLE` 删除表
- **聚合函数**：`COUNT`、`SUM`、`AVG`、`MIN`、`MAX`
- **排序**：`ORDER BY`（支持 ASC/DESC）
- **事务**：`BEGIN` / `COMMIT` / `ROLLBACK`，基于 UNDO 日志实现原子性
- **并发控制**：表级读写锁（共享锁/排他锁），支持多线程并发访问
- **持久化**：内存快照（snapshot）+ 预写日志（WAL），崩溃恢复
- **网络服务**：多线程 TCP 服务器，支持多个客户端同时连接
- **自定义客户端**：提供简单的命令行客户端连接服务器

## 编译
```bash
git clone https://github.com/yourname/minidb.git
cd minidb
make
```

编译后将生成三个可执行文件：
- `minidb`：单机命令行版本
- `minidb_server`：网络服务器
- `minidb_client`：客户端程序

### 单机模式运行
```bash
./minidb
MiniDB> CREATE TABLE users (id INT, name VARCHAR);
Table 'users' created successfully.
MiniDB> INSERT INTO users VALUES (1, 'Alice');
Inserted 1 row into 'users'.
MiniDB> SELECT * FROM users;
id | name
---+------
1 | 'Alice'
1 row(s) returned.
```

### 网络模式运行
**启动服务器**（在一个终端）：
```bash
./minidb_server
```

**启动客户端**（另一个终端）：
```bash
./minidb_client
Connected to MiniDB server at 127.0.0.1:8888
minidb> CREATE TABLE test (id INT, name VARCHAR);
Table 'test' created successfully.
minidb> INSERT INTO test VALUES (1, 'Alice');
Inserted 1 row into 'test'.
minidb> SELECT * FROM test;
id | name
---+------
1 | 'Alice'
1 row(s) returned.
minidb> exit
```

## 使用示例

### 创建表和插入数据
```sql
CREATE TABLE products (id INT, name VARCHAR, price INT);
INSERT INTO products VALUES (1, 'apple', 100);
INSERT INTO products VALUES (2, 'banana', 80);
```

### 查询与投影
```sql
SELECT name, price FROM products;
SELECT * FROM products WHERE price > 90;
```

### 聚合函数
```sql
SELECT COUNT(*) FROM products;
SELECT AVG(price), MIN(price), MAX(price) FROM products;
```

### 排序
```sql
SELECT * FROM products ORDER BY price DESC;
SELECT name FROM products ORDER BY name ASC;
```

### 事务
```sql
BEGIN;
UPDATE products SET price = 110 WHERE name = 'apple';
INSERT INTO products VALUES (3, 'cherry', 120);
COMMIT;
-- 或 ROLLBACK 撤销
```

### 删除表
```sql
DROP TABLE products;
```

## 架构设计

- **词法/语法分析**：Flex + Bison 生成解析器，将 SQL 转换为内部操作。
- **存储引擎**：每个表由 `Table` 类管理，数据存储在 `std::unordered_map<int, std::vector<Value>>` 中（哈希表），支持动态添加行。
- **并发控制**：`LockManager` 实现表级读写锁，使用 `std::shared_mutex`。事务中采用两阶段锁（2PL）。
- **事务**：每个线程拥有独立的 `Transaction` 对象（`thread_local`），记录 UNDO 日志，支持回滚。事务提交时释放锁。
- **持久化**：
  - 快照：全量保存所有表的数据和结构到 `minidb.snapshot` 文件（文本格式）。
  - WAL：事务提交时将 REDO 日志追加到 `minidb.log`，恢复时重放已提交的事务。
- **网络层**：TCP Socket 服务器，每个客户端连接独立线程处理，通过重定向标准输出捕获结果返回。

## 编译依赖

- C++17 编译器（g++ / clang）
- Flex / Bison（生成词法/语法分析器）
- pthread 库（多线程支持）

在 Ubuntu/Debian 上安装依赖：
```bash
sudo apt install g++ flex bison
```

