#pragma once
#include <string>
#include <unordered_map>
#include <vector>
#include <utility>
#include "storage.h"

// 日志记录类型
enum class LogType { INSERT, UPDATE, DELETE };

struct LogRecord {
    LogType type;
    std::string tableName;
    int rowId;                     // 受影响的行ID（INSERT 时为0，提交后实际分配）
    std::vector<Value> oldRow;      // 更新/删除前的行数据
    std::vector<Value> newRow;      // 插入/更新后的行数据
};

// 投影项结构
struct ProjectionItem {
    bool isAgg;
    std::string colName;
    std::string funcName;
    bool star;
};

class DBMS {
public:
    DBMS(); //构造函数中加载快照
    void begin();
    void commit();
    void rollback();
    void createTable(const std::string& name, const std::vector<ColumnDef>& columns);
    void dropTable(const std::string& name);
    void insertInto(const std::string& tableName, const std::vector<Value>& values);
    void update(const std::string& tableName, const std::vector<std::pair<std::string, Value>>& assignments, const Condition* cond); //cond:更新条件，assignments：赋值列表
    void deleteFrom(const std::string& tableName, const Condition* cond); //cond：删除条件
    void selectFrom(const std::string& tableName,const std::vector<ProjectionItem*>& proj,
                const Condition* cond,const std::vector<std::pair<std::string, bool>>* orderBy = nullptr);
private:
    std::unordered_map<std::string, Table> tables;
    Table* getTable(const std::string& name);
    bool inTxn = false;
    std::unordered_map<std::string, Table> txnSnapshot;  // 事务快照
     // 持久化相关
    static constexpr const char* SNAPSHOT_FILE = "minidb.snapshot";
    void saveSnapshot();   // 将 tables 写入文件
    void loadSnapshot();   // 从文件加载到 tables
};

