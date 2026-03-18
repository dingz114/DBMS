#pragma once
#include <string>
#include <vector>
#include <unordered_map>
#include <memory>

enum class DataType { INT, VARCHAR };
enum class Op { EQ, NE, LT, GT, LE, GE };

struct Value {
    enum { INT, STRING } type;
    int intVal;
    std::string strVal;

    Value() = default;
    explicit Value(int v) : type(INT), intVal(v) {}
    explicit Value(const std::string& v) : type(STRING), strVal(v) {}
    bool operator==(const Value& other) const;
    bool operator<(const Value& other) const;
};

struct ColumnDef {  //列定义
    std::string name;
    DataType type;
    ColumnDef(const std::string& n, DataType t) : name(n), type(t) {}
};

struct Condition {  //查询条件
    std::string column;
    Op op;
    Value value;
    Condition(const std::string& c, Op o, const Value& v) : column(c), op(o), value(v) {}
};

class Table {
public:
    Table(const std::vector<ColumnDef>& cols);
    bool insert(const std::vector<Value>& row);
    std::vector<std::vector<Value>> select(const Condition* cond) const;
    bool update(const Condition* cond, const std::vector<std::pair<std::string, Value>>& assignments);
    bool remove(const Condition* cond);
    const std::vector<ColumnDef>& getColumns() const { return columns; }
private:
    std::vector<ColumnDef> columns;
    std::unordered_map<int, std::vector<Value>> rows;
    int nextRowId = 0;
    std::unordered_map<std::string, size_t> columnIndex; 
    bool matchRow(const std::vector<Value>& row, const Condition* cond) const;
};
