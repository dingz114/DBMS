#include <iostream>
#include <thread>
#include <vector>
#include <chrono>
#include "dbms.h"

extern DBMS dbms; 

void reader(DBMS& dbms, int id) {
    // 构造 SELECT * 投影项
    std::vector<ProjectionItem*> proj;
    ProjectionItem* item = new ProjectionItem;
    item->isAgg = false;
    item->star = true;
    proj.push_back(item);

    auto start = std::chrono::steady_clock::now();
    dbms.selectFrom("users", proj, nullptr, nullptr);
    auto end = std::chrono::steady_clock::now();

    std::cout << "Reader " << id << " took "
              << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count()
              << " ms\n";

    delete item;
}

void writer(DBMS& dbms, int id) {
    // 构造更新条件：id = (id%3 + 1)
    Condition cond("id", Op::EQ, Value(id % 3 + 1));
    std::vector<std::pair<std::string, Value>> assignments;
    assignments.emplace_back("name", Value("UpdatedBy" + std::to_string(id)));

    auto start = std::chrono::steady_clock::now();
    dbms.update("users", assignments, &cond);
    auto end = std::chrono::steady_clock::now();

    std::cout << "Writer " << id << " took "
              << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count()
              << " ms\n";
}

int main() {
    dbms.dropTable("users");
    // 创建测试表并插入初始数据
    std::vector<ColumnDef> cols = { {"id", DataType::INT}, {"name", DataType::VARCHAR} };
    dbms.createTable("users", cols);
    for (int i = 1; i <= 3; ++i) {
        dbms.insertInto("users", { Value(i), Value("User" + std::to_string(i)) });
    }

    // 启动 3 个读线程和 2 个写线程
    std::vector<std::thread> threads;
    for (int i = 0; i < 3; ++i)
        threads.emplace_back(reader, std::ref(dbms), i);
    for (int i = 0; i < 2; ++i)
        threads.emplace_back(writer, std::ref(dbms), i);

    for (auto& t : threads)
        t.join();

    return 0;
}