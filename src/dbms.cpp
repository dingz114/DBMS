#include "dbms.h"
#include <iostream>
#include <iomanip>
#include <fstream>
#include <sstream>
#include <cmath>
#include <limits>
#include<algorithm>

DBMS dbms;

thread_local std::unique_ptr<Transaction> DBMS::currentTxn = nullptr;

struct AggResult {
    long long count = 0;
    double sum = 0;
    double avg = 0;
    int minInt = std::numeric_limits<int>::max();
    int maxInt = std::numeric_limits<int>::min();
    std::string minStr;
    std::string maxStr;
    bool hasValue = false;
};

DBMS::DBMS() {
    loadSnapshot();
}

void DBMS::lockTableInTxn(const std::string& tableName, bool exclusive) {
    if (!currentTxn) return; // 不应调用

    // 查找当前事务是否已持有该表的锁
    auto it = std::find_if(currentTxn->locksHeld.begin(), currentTxn->locksHeld.end(),
        [&](const auto& lock){ return lock.first == tableName; });

    if (it == currentTxn->locksHeld.end()) {
        // 未加锁，直接加
        if (exclusive) {
            lockMgr.lockExclusive(tableName);
            currentTxn->locksHeld.emplace_back(tableName, true);
        } else {
            lockMgr.lockShared(tableName);
            currentTxn->locksHeld.emplace_back(tableName, false);
        }
    } else {
        // 已持有锁，检查类型是否满足需求
        bool hasExclusive = it->second;
        if (exclusive && !hasExclusive) {
            // 需要升级：先释放共享锁，再加排他锁
            lockMgr.unlockShared(tableName);
            currentTxn->locksHeld.erase(it);
            lockMgr.lockExclusive(tableName);
            currentTxn->locksHeld.emplace_back(tableName, true);
        }
        // 如果已持有排他锁，或已持有共享锁且需求也是共享锁，则什么都不做
    }
}

void DBMS::begin() {
    if (currentTxn) {
        std::cerr << "ERROR: Already in a transaction." << std::endl;
        return;
    }
    currentTxn = std::make_unique<Transaction>();
    currentTxn->txnId = nextTxnId++;
    currentTxn->status = TxnStatus::ACTIVE;
    std::cout << "Transaction " << currentTxn->txnId << " started." << std::endl;
}

void DBMS::commit() {
    if (!currentTxn) {
        std::cerr << "ERROR: No active transaction." << std::endl;
        return;
    }
    // 释放所有锁
    for (const auto& lock : currentTxn->locksHeld) {
        if (lock.second) // 排他锁
            lockMgr.unlockExclusive(lock.first);
        else
            lockMgr.unlockShared(lock.first);
    }
    currentTxn->status = TxnStatus::COMMITTED;
    currentTxn.reset();
    std::cout << "Transaction committed." << std::endl;
}

void DBMS::rollback() {
    if (!currentTxn) {
        std::cerr << "ERROR: No active transaction." << std::endl;
        return;
    }
    // 逆序应用UNDO日志
    for (auto it = currentTxn->undoLog.rbegin(); it != currentTxn->undoLog.rend(); ++it) {
        Table* table = getTable(it->tableName);
        if (!table) continue;
        switch (it->type) {
            case LogType::INSERT:
                table->removeRow(it->rowId);
                break;
            case LogType::DELETE:
                table->insertWithId(it->rowId, it->oldRow);
                break;
            case LogType::UPDATE:
                table->updateRow(it->rowId, it->oldRow);
                break;
        }
    }
    // 释放所有锁
    for (const auto& lock : currentTxn->locksHeld) {
        if (lock.second)
            lockMgr.unlockExclusive(lock.first);
        else
            lockMgr.unlockShared(lock.first);
    }
    currentTxn->status = TxnStatus::ABORTED;
    currentTxn.reset();
    std::cout << "Transaction rolled back." << std::endl;
}

Table* DBMS::getTable(const std::string& name) {
    std::shared_lock<std::shared_mutex> lock(tablesMutex);
    auto it = tables.find(name);
    if (it == tables.end()) {
        return nullptr;
    }
    return &it->second;
}

void DBMS::createTable(const std::string& name, const std::vector<ColumnDef>& columns) {
    {
        std::unique_lock<std::shared_mutex> lock(tablesMutex);
    // 检查表是否已存在
    if (tables.find(name) != tables.end()) {
        std::cerr << "ERROR: Table '" << name << "' already exists." << std::endl;
        return;
    }
    // 创建新表并加入管理容器
    tables.emplace(name, Table(columns));
    std::cout << "Table '" << name << "' created successfully." << std::endl;
    }
    saveSnapshot();   // 表结构改变，保存
}

void DBMS::dropTable(const std::string& name) {
    {
        std::unique_lock<std::shared_mutex> lock(tablesMutex);
        auto it = tables.find(name);
        if (it == tables.end()) {
            std::cerr << "ERROR: Table '" << name << "' does not exist." << std::endl;
            return;
        }   
        tables.erase(it);
        std::cout << "Table '" << name << "' dropped." << std::endl;
    }
    saveSnapshot();
}

void DBMS::insertInto(const std::string& tableName, const std::vector<Value>& values) {
    if (currentTxn) {
        lockTableInTxn(tableName, true); // 需要排他锁

        Table* table = getTable(tableName);
        if (!table) {
            lockMgr.unlockExclusive(tableName);
            currentTxn->locksHeld.pop_back();
            return;
        }

        int newId = table->insertAndGetId(values);
        if (newId >= 0) {
            LogRecord rec{LogType::INSERT, tableName, newId, {}, values};
            currentTxn->undoLog.push_back(rec);
            std::cout << "Inserted (txn) 1 row into '" << tableName << "'." << std::endl;
        } else {
            std::cerr << "Insert failed." << std::endl;
        }
    } else {
        // 自动提交模式
        lockMgr.lockExclusive(tableName);
        Table* table = getTable(tableName);
        if (table && table->insert(values)) {
            std::cout << "Inserted 1 row into '" << tableName << "'." << std::endl;
            saveSnapshot();
        } else {
            std::cerr << "Insert failed." << std::endl;
        }
        lockMgr.unlockExclusive(tableName);
    }
}

void DBMS::update(const std::string& tableName,
                  const std::vector<std::pair<std::string, Value>>& assignments,
                  const Condition* cond) {
    if (currentTxn) {
        lockTableInTxn(tableName, true);

        Table* table = getTable(tableName);
        if (!table) {
            lockMgr.unlockExclusive(tableName);
            currentTxn->locksHeld.pop_back();
            return;
        }

        auto rows = table->selectWithIds(cond);
        if (rows.empty()) {
            std::cout << "No rows matched." << std::endl;
            return;
        }

        for (const auto& [id, oldRow] : rows) {
            std::vector<Value> newRow = oldRow;
            for (const auto& [colName, newVal] : assignments) {
                auto it = table->getColumnIndex().find(colName);
                if (it == table->getColumnIndex().end()) {
                    std::cerr << "ERROR: Unknown column '" << colName << "'." << std::endl;
                    return;
                }
                newRow[it->second] = newVal;
            }

            LogRecord rec{LogType::UPDATE, tableName, id, oldRow, newRow};
            currentTxn->undoLog.push_back(rec);
            table->updateRow(id, newRow);
        }
        std::cout << "Updated (txn) " << rows.size() << " row(s)." << std::endl;
    } else {
        lockMgr.lockExclusive(tableName);
        Table* table = getTable(tableName);
        if (table && table->update(cond, assignments)) {
            std::cout << "Updated row(s)." << std::endl;
            saveSnapshot();
        } else {
            std::cerr << "Update failed." << std::endl;
        }
        lockMgr.unlockExclusive(tableName);
    }
}

void DBMS::deleteFrom(const std::string& tableName, const Condition* cond) {
    if (currentTxn) {
        lockTableInTxn(tableName, true);

        Table* table = getTable(tableName);
        if (!table) {
            lockMgr.unlockExclusive(tableName);
            currentTxn->locksHeld.pop_back();
            return;
        }

        auto rows = table->selectWithIds(cond);
        if (rows.empty()) {
            std::cout << "No rows matched." << std::endl;
            return;
        }

        for (const auto& [id, oldRow] : rows) {
            LogRecord rec{LogType::DELETE, tableName, id, oldRow, {}};
            currentTxn->undoLog.push_back(rec);
            table->removeRow(id);
        }
        std::cout << "Deleted (txn) " << rows.size() << " row(s)." << std::endl;
    } else {
        lockMgr.lockExclusive(tableName);
        Table* table = getTable(tableName);
        if (table && table->remove(cond)) {
            std::cout << "Deleted row(s)." << std::endl;
            saveSnapshot();
        } else {
            std::cerr << "Delete failed." << std::endl;
        }
        lockMgr.unlockExclusive(tableName);
    }
}

// 将 Value 转换为字符串（用于保存）
std::string valueToString(const Value& v) {
    if (v.type == Value::INT)
        return std::to_string(v.intVal);
    else
        return "'" + v.strVal + "'";
}

// 从字符串解析 Value（根据类型）
Value stringToValue(const std::string& str, DataType type) {
    if (type == DataType::INT) {
        return Value(std::stoi(str));
    } else {
        // 去掉单引号
        if (str.size() >= 2 && str.front() == '\'' && str.back() == '\'')
            return Value(str.substr(1, str.size()-2));
        else
            return Value(str); // 容错
    }
}

void DBMS::saveSnapshot() {
    std::string tmpFile = std::string(SNAPSHOT_FILE) + ".tmp";
    std::ofstream ofs(tmpFile);
    if (!ofs) {
        std::cerr << "ERROR: Cannot open snapshot file for writing." << std::endl;
        return;
    }

    // 保护 tables 的读取
    std::shared_lock<std::shared_mutex> lock(tablesMutex);
    for (const auto& [name, table] : tables) {
        ofs << "#TABLE " << name;
        const auto& cols = table.getColumns();
        for (size_t i = 0; i < cols.size(); ++i) {
            ofs << (i == 0 ? ":" : ",") << cols[i].name << ":"
                << (cols[i].type == DataType::INT ? "INT" : "VARCHAR");
        }
        ofs << "\n";
        auto rows = table.select(nullptr);
        for (const auto& row : rows) {
            ofs << "#ROW";
            for (const auto& val : row) {
                ofs << ":" << valueToString(val);
            }
            ofs << "\n";
        }
    }
    ofs.close();
    lock.unlock();

    if (rename(tmpFile.c_str(), SNAPSHOT_FILE) != 0) {
        std::cerr << "ERROR: Failed to commit snapshot." << std::endl;
    }
}

void DBMS::loadSnapshot() {
    std::ifstream ifs(SNAPSHOT_FILE);
    if (!ifs) return;

    std::string line;
    std::string currentTable;
    std::vector<ColumnDef> currentColumns;

    // 需要排他锁修改 tables
    std::unique_lock<std::shared_mutex> lock(tablesMutex);
    while (std::getline(ifs, line)) {
        if (line.empty()) continue;
        if (line[0] == '#') {
            if (line.substr(0, 7) == "#TABLE ") {
                size_t pos = 7;
                size_t colon = line.find(':', pos);
                currentTable = line.substr(pos, colon - pos);
                std::string colPart = line.substr(colon + 1);
                std::istringstream colss(colPart);
                std::string colDef;
                currentColumns.clear();
                while (std::getline(colss, colDef, ',')) {
                    size_t nameColon = colDef.find(':');
                    std::string colName = colDef.substr(0, nameColon);
                    std::string typeStr = colDef.substr(nameColon + 1);
                    DataType type = (typeStr == "INT") ? DataType::INT : DataType::VARCHAR;
                    currentColumns.emplace_back(colName, type);
                }
                tables.emplace(currentTable, Table(currentColumns));
            } else if (line.substr(0, 5) == "#ROW:" && !currentTable.empty()) {
                auto it = tables.find(currentTable);
                if (it == tables.end()) continue;
                std::string data = line.substr(5);
                std::istringstream dss(data);
                std::string token;
                std::vector<Value> row;
                const auto& cols = it->second.getColumns();
                size_t colIdx = 0;
                while (std::getline(dss, token, ':')) {
                    if (colIdx >= cols.size()) break;
                    row.push_back(stringToValue(token, cols[colIdx].type));
                    ++colIdx;
                }
                it->second.insert(row);
            }
        }
    }
    std::cout << "Loaded snapshot from " << SNAPSHOT_FILE << std::endl;
}

void DBMS::selectFrom(const std::string& tableName,
                      const std::vector<ProjectionItem*>& proj,
                      const Condition* cond,
                      const std::vector<std::pair<std::string, bool>>* orderBy)
{
    bool inTxn = (currentTxn != nullptr);
    if (inTxn) {
        lockTableInTxn(tableName, true);
    } else {
        lockMgr.lockShared(tableName);
    }

    Table* table = getTable(tableName);
    if (!table) {
        if (!inTxn) lockMgr.unlockShared(tableName);
        return;
    }

    auto rows = table->select(cond);
    if (rows.empty()) {
        std::cout << "No rows found." << std::endl;
        lockMgr.unlockShared(tableName);
        return;
    }

    const auto& cols = table->getColumns();
    const auto& colIndex = table->getColumnIndex();

    // 检查投影列合法性
    for (auto item : proj) {
        if (!item->star && !item->isAgg) {
            if (colIndex.find(item->colName) == colIndex.end()) {
                std::cerr << "Error: Unknown column '" << item->colName << "'." << std::endl;
                lockMgr.unlockShared(tableName);
                return;
            }
        }
    }

    bool hasAgg = false;
    for (auto item : proj) {
        if (item->isAgg) {
            hasAgg = true;
            break;
        }
    }

    std::vector<std::vector<Value>> resultRows;

    if (hasAgg) {
        // 聚合查询（无GROUP BY）
        std::vector<Value> resultRow;
        for (auto item : proj) {
            if (!item->isAgg) {
                std::cerr << "Error: Mixed column and aggregation without GROUP BY." << std::endl;
                lockMgr.unlockShared(tableName);
                return;
            }
            if (item->star) {
                resultRow.emplace_back(static_cast<int>(rows.size()));
                continue;
            }
            size_t colIdx = colIndex.at(item->colName);
            if (item->funcName == "COUNT") {
                resultRow.emplace_back(static_cast<int>(rows.size()));
            } else if (item->funcName == "SUM") {
                long long sum = 0;
                for (const auto& row : rows)
                    if (row[colIdx].type == Value::INT)
                        sum += row[colIdx].intVal;
                resultRow.emplace_back(static_cast<int>(sum));
            } else if (item->funcName == "AVG") {
                double avg = 0;
                for (const auto& row : rows)
                    if (row[colIdx].type == Value::INT)
                        avg += row[colIdx].intVal;
                avg /= rows.size();
                resultRow.emplace_back(static_cast<int>(avg));
            } else if (item->funcName == "MIN") {
                int minInt = std::numeric_limits<int>::max();
                std::string minStr;
                for (const auto& row : rows) {
                    if (row[colIdx].type == Value::INT) {
                        if (row[colIdx].intVal < minInt) minInt = row[colIdx].intVal;
                    } else {
                        if (minStr.empty() || row[colIdx].strVal < minStr) minStr = row[colIdx].strVal;
                    }
                }
                if (rows[0][colIdx].type == Value::INT)
                    resultRow.emplace_back(minInt);
                else
                    resultRow.emplace_back(minStr);
            } else if (item->funcName == "MAX") {
                int maxInt = std::numeric_limits<int>::min();
                std::string maxStr;
                for (const auto& row : rows) {
                    if (row[colIdx].type == Value::INT) {
                        if (row[colIdx].intVal > maxInt) maxInt = row[colIdx].intVal;
                    } else {
                        if (maxStr.empty() || row[colIdx].strVal > maxStr) maxStr = row[colIdx].strVal;
                    }
                }
                if (rows[0][colIdx].type == Value::INT)
                    resultRow.emplace_back(maxInt);
                else
                    resultRow.emplace_back(maxStr);
            }
        }
        resultRows.push_back(resultRow);
    } else {
        // 普通投影
        std::vector<size_t> colIndices;
        if (proj.size() == 1 && proj[0]->star) {
            for (size_t i = 0; i < cols.size(); ++i) colIndices.push_back(i);
        } else {
            for (auto item : proj) {
                if (item->star) continue;
                auto it = colIndex.find(item->colName);
                if (it == colIndex.end()) {
                    std::cerr << "Error: Unknown column '" << item->colName << "'." << std::endl;
                    lockMgr.unlockShared(tableName);
                    return;
                }
                colIndices.push_back(it->second);
            }
        }
        for (const auto& row : rows) {
            std::vector<Value> resultRow;
            for (size_t idx : colIndices) {
                resultRow.push_back(row[idx]);
            }
            resultRows.push_back(resultRow);
        }
    }

    // 排序（如果指定了 ORDER BY）
    if (orderBy && !orderBy->empty() && resultRows.size() > 1) {
        std::vector<std::pair<int, bool>> sortCols;
        for (const auto& item : *orderBy) {
            int idx = -1;
            for (size_t i = 0; i < proj.size(); ++i) {
                if (!proj[i]->isAgg && proj[i]->colName == item.first) {
                    idx = i;
                    break;
                }
            }
            if (idx == -1) {
                std::cerr << "Error: ORDER BY column '" << item.first
                          << "' not found in SELECT list or is an aggregate." << std::endl;
                lockMgr.unlockShared(tableName);
                return;
            }
            sortCols.emplace_back(idx, item.second);
        }

        std::sort(resultRows.begin(), resultRows.end(),
            [&sortCols](const std::vector<Value>& a, const std::vector<Value>& b) {
                for (const auto& sc : sortCols) {
                    int col = sc.first;
                    bool asc = sc.second;
                    if (a[col] == b[col]) continue;
                    if (asc) return a[col] < b[col];
                    else return b[col] < a[col];
                }
                return false;
            });
    }

    // 输出结果
    for (size_t i = 0; i < proj.size(); ++i) {
        if (i > 0) std::cout << " | ";
        if (proj[i]->star) {
            if (proj.size() == 1 && proj[0]->star) {
                for (size_t j = 0; j < cols.size(); ++j) {
                    if (j > 0) std::cout << " | ";
                    std::cout << cols[j].name;
                }
                break;
            } else {
                std::cout << "*";
            }
        } else if (proj[i]->isAgg) {
            std::cout << proj[i]->funcName << "(" << proj[i]->colName << ")";
        } else {
            std::cout << proj[i]->colName;
        }
    }
    std::cout << std::endl;
    for (size_t i = 0; i < proj.size(); ++i) {
        if (i > 0) std::cout << "-+-";
        std::cout << "---";
    }
    std::cout << std::endl;
    for (const auto& row : resultRows) {
        for (size_t i = 0; i < row.size(); ++i) {
            if (i > 0) std::cout << " | ";
            if (row[i].type == Value::INT)
                std::cout << row[i].intVal;
            else
                std::cout << "'" << row[i].strVal << "'";
        }
        std::cout << std::endl;
    }
    std::cout << resultRows.size() << " row(s) returned." << std::endl;

    if (!inTxn) {
        lockMgr.unlockShared(tableName);
    }
    // 事务中锁会在 commit/rollback 时释放
}