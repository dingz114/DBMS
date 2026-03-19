#pragma once
#include <string>
#include <unordered_map>
#include <vector>
#include <utility>
#include <shared_mutex>
#include <mutex>
#include <thread>
#include <atomic>
#include <list>
#include <fstream>
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


enum class TxnStatus { ACTIVE, COMMITTED, ABORTED };
enum class LogType { INSERT, UPDATE, DELETE };
enum class RedoLogType { TXN_BEGIN, INSERT, UPDATE, DELETE, TXN_COMMIT, TXN_ABORT };

struct LogRecord {
    LogType type;
    std::string tableName;
    int rowId;                  // 受影响的行ID
    std::vector<Value> oldRow;   // 用于 UNDO
    std::vector<Value> newRow;   // 用于 REDO（可选，暂不使用）
};

struct RedoLogRecord {
    RedoLogType type;
    int txnId;
    std::string tableName;
    int rowId;
    std::vector<Value> newRow;   // 用于 INSERT 和 UPDATE
};


struct Transaction {
    int txnId;
    TxnStatus status;
    std::vector<LogRecord> undoLog;                     // 撤销日志
    std::vector<RedoLogRecord> redoLog; 
    std::vector<std::pair<std::string, bool>> locksHeld; // 持有的表锁，bool=true为排他锁
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
    // 线程局部事务指针
    static thread_local std::unique_ptr<Transaction> currentTxn;
    std::atomic<int> nextTxnId{1};
    void lockTableInTxn(const std::string& tableName, bool exclusive);
    static constexpr const char* LOG_FILE = "minidb.log";
    std::mutex logMutex;
    void appendRedoLog(const RedoLogRecord& rec);   // 写入单条日志（实际在 commit 中批量写入）
    void recoverFromLog();                           // 恢复函数
};
extern DBMS dbms;
