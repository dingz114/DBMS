#include "storage.h"
#include <algorithm>
#include <stdexcept>
#include <iostream>

bool Value::operator==(const Value& other) const {
    if (type != other.type) return false;
    if (type == INT) return intVal == other.intVal;
    else return strVal == other.strVal;
}

bool Value::operator<(const Value& other) const {
    if (type != other.type) {
        // INT 永远小于 STRING（仅用于避免未定义行为）
        return type == INT && other.type == STRING;
    }
    if (type == INT) return intVal < other.intVal;
    else return strVal < other.strVal;
}

Table::Table(const std::vector<ColumnDef>& cols) : columns(cols) {
    // 构建列名到索引的映射
    for (size_t i = 0; i < columns.size(); ++i) {
        columnIndex[columns[i].name] = i;
    }
}

bool Table::insert(const std::vector<Value>& row) {
    // 检查列数是否匹配
    if (row.size() != columns.size()) {
        std::cerr << "ERROR: Column count mismatch. Expected " << columns.size()
                  << ", got " << row.size() << std::endl;
        return false;
    }
    // 类型检查
    for (size_t i = 0; i < row.size(); ++i) {
        DataType expected = columns[i].type;
        if ((expected == DataType::INT && row[i].type != Value::INT) ||
            (expected == DataType::VARCHAR && row[i].type != Value::STRING)) {
            std::cerr << "ERROR: Type mismatch for column '" << columns[i].name << "'" << std::endl;
            return false;
        }
    }
    rows[nextRowId++] = row;
    return true;
}

std::vector<std::vector<Value>> Table::select(const Condition* cond) const {
    std::vector<std::vector<Value>> result;
    for (const auto& [id, row] : rows) {
        if (matchRow(row, cond)) {
            result.push_back(row);
        }
    }
    return result;
}

bool Table::update(const Condition* cond, const std::vector<std::pair<std::string, Value>>& assignments) {
    // 收集满足条件的行号
    std::vector<int> matchedIds;
    for (const auto& [id, row] : rows) {
        if (matchRow(row, cond)) {
            matchedIds.push_back(id);
        }
    }
    // 对每个满足条件的行进行更新
    for (int id : matchedIds) {
        std::vector<Value> newRow = rows[id];  // 复制旧行
        // 应用每个赋值
        for (const auto& [colName, newVal] : assignments) {
            auto it = columnIndex.find(colName);
            if (it == columnIndex.end()) {
                std::cerr << "ERROR: Unknown column '" << colName << "' in SET clause" << std::endl;
                return false;
            }
            size_t idx = it->second;
            // 类型检查
            DataType expected = columns[idx].type;
            if ((expected == DataType::INT && newVal.type != Value::INT) ||
                (expected == DataType::VARCHAR && newVal.type != Value::STRING)) {
                std::cerr << "ERROR: Type mismatch for column '" << colName << "' in SET clause" << std::endl;
                return false;
            }
            newRow[idx] = newVal;
        }
        rows[id] = newRow;  // 替换旧行
    }
    return true;
}

bool Table::remove(const Condition* cond) {
    std::vector<int> matchedIds;
    for (const auto& [id, row] : rows) {
        if (matchRow(row, cond)) {
            matchedIds.push_back(id);
        }
    }
    for (int id : matchedIds) {
        rows.erase(id);
    }
    return true;
}

bool Table::matchRow(const std::vector<Value>& row, const Condition* cond) const {
    if (!cond) return true;  // 无条件则匹配所有行
    // 查找条件中指定的列
    auto it = columnIndex.find(cond->column);
    if (it == columnIndex.end()) {
        std::cerr << "ERROR: Unknown column '" << cond->column << "' in condition" << std::endl;
        return false;
    }
    size_t colIdx = it->second;

    const Value& rowVal = row[colIdx];
    const Value& condVal = cond->value;

    // 类型检查：条件值的类型应与列定义一致
    DataType colType = columns[colIdx].type;
    if ((colType == DataType::INT && condVal.type != Value::INT) ||
        (colType == DataType::VARCHAR && condVal.type != Value::STRING)) {
        std::cerr << "ERROR: Type mismatch in condition for column '" << cond->column << "'" << std::endl;
        return false;
    }

    // 根据操作符比较
    switch (cond->op) {
        case Op::EQ: return rowVal == condVal;
        case Op::NE: return !(rowVal == condVal);
        case Op::LT: return rowVal < condVal;
        case Op::GT: return condVal < rowVal;
        case Op::LE: return !(condVal < rowVal);
        case Op::GE: return !(rowVal < condVal);
        default: return false;
    }
}

int Table::insertAndGetId(const std::vector<Value>& row) {
    if (row.size() != columns.size()) {
        std::cerr << "ERROR: Column count mismatch." << std::endl;
        return -1;
    }
    for (size_t i = 0; i < row.size(); ++i) {
        DataType expected = columns[i].type;
        if ((expected == DataType::INT && row[i].type != Value::INT) ||
            (expected == DataType::VARCHAR && row[i].type != Value::STRING)) {
            std::cerr << "ERROR: Type mismatch for column '" << columns[i].name << "'" << std::endl;
            return -1;
        }
    }
    int id = nextRowId++;
    rows[id] = row;
    return id;
}

std::vector<Value> Table::getRow(int rowId) const {
    auto it = rows.find(rowId);
    if (it != rows.end()) return it->second;
    return {};
}

bool Table::updateRow(int rowId, const std::vector<Value>& newRow) {
    auto it = rows.find(rowId);
    if (it == rows.end()) return false;
    it->second = newRow;
    return true;
}

bool Table::removeRow(int rowId) {
    auto count = rows.erase(rowId);
    if (count == 0) {
        std::cerr << "Debug: removeRow failed, rowId=" << rowId << " not found." << std::endl;
    }
    return count > 0;
}

std::vector<std::pair<int, std::vector<Value>>> Table::selectWithIds(const Condition* cond) const {
    std::vector<std::pair<int, std::vector<Value>>> result;
    for (const auto& [id, row] : rows) {
        if (matchRow(row, cond)) result.push_back({id, row});
    }
    return result;
}

bool Table::insertWithId(int rowId, const std::vector<Value>& row) {
    if (rows.count(rowId) > 0) return false;
    rows[rowId] = row;
    return true;
}