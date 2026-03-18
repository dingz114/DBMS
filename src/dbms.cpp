#include "dbms.h"
#include <iostream>
#include <iomanip>
#include <fstream>
#include <sstream>
#include <cmath>
#include <limits>
#include<algorithm>

DBMS dbms;

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

void DBMS::begin() {
    if (inTxn) {
        std::cerr << "ERROR: Already in a transaction." << std::endl;
        return;
    }
    // 深拷贝所有表
    txnSnapshot = tables;   
    inTxn = true;
    std::cout << "Transaction started." << std::endl;
}

void DBMS::commit() {
    if (!inTxn) {
        std::cerr << "ERROR: No active transaction." << std::endl;
        return;
    }
    // 用快照替换原表
    tables = std::move(txnSnapshot);
    txnSnapshot.clear();
    inTxn = false;
    std::cout << "Transaction committed." << std::endl;
    saveSnapshot();   // 提交后持久化
}

void DBMS::rollback() {
    if (!inTxn) {
        std::cerr << "ERROR: No active transaction." << std::endl;
        return;
    }
    // 直接丢弃快照
    txnSnapshot.clear();
    inTxn = false;
    std::cout << "Transaction rolled back." << std::endl;
}

Table* DBMS::getTable(const std::string& name) {
    auto& map = inTxn ? txnSnapshot : tables;
    auto it = map.find(name);
    if (it == map.end()) return nullptr;
    return &it->second;
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
    saveSnapshot();   // 表结构改变，保存
}

void DBMS::dropTable(const std::string& name) {
    if (inTxn) {
        std::cerr << "ERROR: Cannot DROP TABLE within a transaction (simplified)." << std::endl;
        return;
    }
    auto it = tables.find(name);
    if (it == tables.end()) {
        std::cerr << "ERROR: Table '" << name << "' does not exist." << std::endl;
        return;
    }
    tables.erase(it);
    std::cout << "Table '" << name << "' dropped." << std::endl;
    saveSnapshot();   // 立即持久化
}

void DBMS::insertInto(const std::string& tableName, const std::vector<Value>& values) {
    Table* table = getTable(tableName);
    if (!table) return;
    if (table->insert(values)) {
        std::cout << "Inserted " << (inTxn ? "(txn) " : "") << "1 row into '" << tableName << "'." << std::endl;
        if (!inTxn) saveSnapshot();
    } else {
        std::cerr << "Insert failed." << std::endl;
    }
}


void DBMS::update(const std::string& tableName,
                  const std::vector<std::pair<std::string, Value>>& assignments,
                  const Condition* cond) {
    Table* table = getTable(tableName);
    if (!table) return;
    if (table->update(cond, assignments)) {
        std::cout << "Updated " << (inTxn ? "(txn) " : "") << "row(s)." << std::endl;
        if (!inTxn) saveSnapshot();
    } else {
        std::cerr << "Update failed." << std::endl;
    }
}

void DBMS::deleteFrom(const std::string& tableName, const Condition* cond) {
    Table* table = getTable(tableName);
    if (!table) return;
    if (table->remove(cond)) {
        std::cout << "Deleted " << (inTxn ? "(txn) " : "") << "row(s)." << std::endl;
        if (!inTxn) saveSnapshot();
    } else {
        std::cerr << "Delete failed." << std::endl;
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
    // 先写入临时文件，成功后 rename
    std::string tmpFile = std::string(SNAPSHOT_FILE) + ".tmp";
    std::ofstream ofs(tmpFile);
    if (!ofs) {
        std::cerr << "ERROR: Cannot open snapshot file for writing." << std::endl;
        return;
    }

    for (const auto& [name, table] : tables) {
        // 写入表定义
        ofs << "#TABLE " << name;
        const auto& cols = table.getColumns();
        for (size_t i = 0; i < cols.size(); ++i) {
            ofs << (i == 0 ? ":" : ",") << cols[i].name << ":"
                << (cols[i].type == DataType::INT ? "INT" : "VARCHAR");
        }
        ofs << "\n";

        // 写入所有行
        auto rows = table.select(nullptr);  // 无条件获取所有行
        for (const auto& row : rows) {
            ofs << "#ROW";
            for (const auto& val : row) {
                ofs << ":" << valueToString(val);
            }
            ofs << "\n";
        }
    }
    ofs.close();

    // 原子替换
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
    bool inTable = false;

    while (std::getline(ifs, line)) {
        if (line.empty()) continue;
        if (line[0] == '#') {
            if (line.substr(0, 7) == "#TABLE ") {
                // 解析表定义
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
                inTable = true;
            } else if (line.substr(0, 5) == "#ROW:" && inTable) {
                // 使用 find 获取表，避免 operator[] 的默认构造
                auto it = tables.find(currentTable);
                if (it == tables.end()) {
                    std::cerr << "Snapshot error: row before table definition." << std::endl;
                    continue;
                }
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
    Table* table = getTable(tableName);
    if (!table) return;

    auto rows = table->select(cond);
    if (rows.empty()) {
        std::cout << "No rows found." << std::endl;
        return;
    }

    const auto& cols = table->getColumns();
    const auto& colIndex = table->getColumnIndex(); 

    // 检查投影列合法性
    for (auto item : proj) {
        if (!item->star && !item->isAgg) {
            if (colIndex.find(item->colName) == colIndex.end()) {
                std::cerr << "Error: Unknown column '" << item->colName << "'." << std::endl;
                return;
            }
        }
    }

    // 判断是否有聚合函数
    bool hasAgg = false;
    for (auto item : proj) {
        if (item->isAgg) {
            hasAgg = true;
            break;
        }
    }

    std::vector<std::vector<Value>> resultRows; // 最终结果行

    if (hasAgg) {
        // ---------- 聚合查询（无GROUP BY，整表一组）----------
        std::vector<Value> resultRow;
        for (auto item : proj) {
            if (!item->isAgg) {
                std::cerr << "Error: Mixed column and aggregation without GROUP BY." << std::endl;
                return;
            }
            if (item->star) {
                // COUNT(*)
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
                resultRow.emplace_back(static_cast<int>(avg)); // 简化，丢失精度
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
        // ---------- 普通投影（无聚合）----------
        std::vector<size_t> colIndices;
        if (proj.size() == 1 && proj[0]->star) {
            // 选择所有列
            for (size_t i = 0; i < cols.size(); ++i) colIndices.push_back(i);
        } else {
            for (auto item : proj) {
                if (item->star) continue;
                auto it = colIndex.find(item->colName);
                if (it == colIndex.end()) {
                    std::cerr << "Error: Unknown column '" << item->colName << "'." << std::endl;
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

    // ---------- 排序 ----------
    if (orderBy && !orderBy->empty() && resultRows.size() > 1) {
        // 构建排序列在结果行中的索引
        std::vector<std::pair<int, bool>> sortCols;
        for (const auto& item : *orderBy) {
            int idx = -1;
            // 排序列必须在SELECT列表中且为非聚合列
            for (size_t i = 0; i < proj.size(); ++i) {
                if (!proj[i]->isAgg && proj[i]->colName == item.first) {
                    idx = i;
                    break;
                }
            }
            if (idx == -1) {
                std::cerr << "Error: ORDER BY column '" << item.first
                          << "' not found in SELECT list or is an aggregate." << std::endl;
                return;
            }
            sortCols.emplace_back(idx, item.second); // true表示ASC
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
                return false; // 相等
            });
    }

    // ---------- 输出结果 ----------
    // 打印列名
    for (size_t i = 0; i < proj.size(); ++i) {
        if (i > 0) std::cout << " | ";
        if (proj[i]->star) {
            // 若投影为 *，已展开为所有列，此处需输出所有列名
            if (proj.size() == 1 && proj[0]->star) {
                for (size_t j = 0; j < cols.size(); ++j) {
                    if (j > 0) std::cout << " | ";
                    std::cout << cols[j].name;
                }
                // 跳过后续分隔符循环
                i = proj.size(); // 退出循环
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
    // 打印分隔线
    for (size_t i = 0; i < proj.size(); ++i) {
        if (i > 0) std::cout << "-+-";
        std::cout << "---";
    }
    std::cout << std::endl;
    // 打印行
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
}