#pragma once
#include <string>
#include <unordered_map>
#include <vector>
#include <utility>
#include "storage.h"

class DBMS {
public:
    void createTable(const std::string& name, const std::vector<ColumnDef>& columns);
    void insertInto(const std::string& tableName, const std::vector<Value>& values);
    void selectFrom(const std::string& tableName, const Condition* cond);  //cond:查询条件
    void update(const std::string& tableName, const std::vector<std::pair<std::string, Value>>& assignments, const Condition* cond); //cond:更新条件，assignments：赋值列表
    void deleteFrom(const std::string& tableName, const Condition* cond); //cond：删除条件

private:
    std::unordered_map<std::string, Table> tables;
    Table* getTable(const std::string& name);
};