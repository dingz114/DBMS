#include "dbms.h"
#include <iostream>
#include <iomanip>

Table* DBMS::getTable(const std::string& name) {
    auto it = tables.find(name);
    if (it == tables.end()) {
        std::cerr << "ERROR: Table '" << name << "' does not exist." << std::endl;
        return nullptr;
    }
    return &(it->second);
}

void DBMS::createTable(const std::string& name, const std::vector<ColumnDef>& columns) {
    // 检查表是否已存在
    if (tables.find(name) != tables.end()) {
        std::cerr << "ERROR: Table '" << name << "' already exists." << std::endl;
        return;
    }
    // 创建新表并加入管理容器
    tables.emplace(name, Table(columns));
    std::cout << "Table '" << name << "' created successfully." << std::endl;
}

void DBMS::insertInto(const std::string& tableName, const std::vector<Value>& values) {
    Table* table = getTable(tableName);
    if (!table) return;

    if (table->insert(values)) {
        std::cout << "Inserted 1 row into '" << tableName << "'." << std::endl;
    } else {
        std::cerr << "Insert failed." << std::endl;
    }
}

void DBMS::selectFrom(const std::string& tableName, const Condition* cond) {
    Table* table = getTable(tableName);
    if (!table) return;

    auto results = table->select(cond);
    if (results.empty()) {
        std::cout << "No rows found." << std::endl;
        return;
    }

    // 获取表的列定义
    const auto& columns = table->getColumns();

    // 打印列名
    for (size_t i = 0; i < columns.size(); ++i) {
        if (i > 0) std::cout << " | ";
        std::cout << columns[i].name;
    }
    std::cout << std::endl;

    // 打印分隔线
    for (size_t i = 0; i < columns.size(); ++i) {
        if (i > 0) std::cout << "-+-";
        std::cout << "---";
    }
    std::cout << std::endl;

    // 打印每一行数据
    for (const auto& row : results) {
        for (size_t i = 0; i < row.size(); ++i) {
            if (i > 0) std::cout << " | ";
            if (row[i].type == Value::INT) {
                std::cout << row[i].intVal;
            } else {
                std::cout << "'" << row[i].strVal << "'";
            }
        }
        std::cout << std::endl;
    }

    std::cout << results.size() << " row(s) returned." << std::endl;
}

void DBMS::update(const std::string& tableName,
                  const std::vector<std::pair<std::string, Value>>& assignments,
                  const Condition* cond) {
    Table* table = getTable(tableName);
    if (!table) return;

    if (table->update(cond, assignments)) {
        std::cout << "Update successful." << std::endl;
    } else {
        std::cerr << "Update failed." << std::endl;
    }
}

void DBMS::deleteFrom(const std::string& tableName, const Condition* cond) {
    Table* table = getTable(tableName);
    if (!table) return;

    if (table->remove(cond)) {
        std::cout << "Delete successful." << std::endl;
    } else {
        std::cerr << "Delete failed." << std::endl;
    }
}

DBMS dbms;