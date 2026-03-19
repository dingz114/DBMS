#pragma once
#include <string>
#include <unordered_map>
#include <vector>
#include <utility>
#include <shared_mutex>
#include <mutex>
#include "storage.h"

// 锁管理器类，提供表级读写锁
class LockManager {
public:
    // 加共享锁（读锁）
    void lockShared(const std::string& tableName) {
        std::unique_lock<std::mutex> lock(mapMutex);
        auto& mtx = locks[tableName];   // 如果不存在，创建新的 shared_mutex
        lock.unlock();
        mtx.lock_shared();
    }
    // 加排他锁（写锁）
    void lockExclusive(const std::string& tableName) {
        std::unique_lock<std::mutex> lock(mapMutex);
        auto& mtx = locks[tableName];
        lock.unlock();
        mtx.lock();
    }
    // 释放共享锁
    void unlockShared(const std::string& tableName) {
        std::unique_lock<std::mutex> lock(mapMutex);
        auto it = locks.find(tableName);
        if (it != locks.end()) {
            lock.unlock();
            it->second.unlock_shared();
        }
    }
    // 释放排他锁
    void unlockExclusive(const std::string& tableName) {
        std::unique_lock<std::mutex> lock(mapMutex);
        auto it = locks.find(tableName);
        if (it != locks.end()) {
            lock.unlock();
            it->second.unlock();
        }
    }
private:
    std::unordered_map<std::string, std::shared_mutex> locks;
    std::mutex mapMutex;   // 保护 locks 的并发访问
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
     // 持久化相关
    static constexpr const char* SNAPSHOT_FILE = "minidb.snapshot";
    void saveSnapshot();   // 将 tables 写入文件
    void loadSnapshot();   // 从文件加载到 tables
    std::shared_mutex tablesMutex;      // 保护 tables 容器
    LockManager lockMgr;                // 表级锁管理器
};

