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
    Table() = default; 
    Table(const std::vector<ColumnDef>& cols);
    // 拷贝构造函数（深拷贝）
    Table(const Table& other)
        : columns(other.columns),
          rows(other.rows),          // unordered_map 自动深拷贝 value
          nextRowId(other.nextRowId),
          columnIndex(other.columnIndex) {}
    int insertAndGetId(const std::vector<Value>& row);// 插入并返回新行的ID（-1表示失败)
    bool insert(const std::vector<Value>& row);  
    std::vector<Value> getRow(int rowId) const;  // 根据ID获取行
    std::vector<std::vector<Value>> select(const Condition* cond) const;
    bool updateRow(int rowId, const std::vector<Value>& newRow); // 根据ID更新行
    bool update(const Condition* cond, const std::vector<std::pair<std::string, Value>>& assignments);
    bool removeRow(int rowId); // 根据ID删除行
    bool matchRow(const std::vector<Value>& row, const Condition* cond) const;
    bool remove(const Condition* cond);
    const std::vector<ColumnDef>& getColumns() const { return columns; }
    std::vector<std::pair<int, std::vector<Value>>> selectWithIds(const Condition* cond) const;  // 根据条件获取所有匹配的行ID及对应数据
    bool insertWithId(int rowId, const std::vector<Value>& row); //使用指定ID插入行（用于回滚）
    const std::unordered_map<std::string, size_t>& getColumnIndex() const { return columnIndex; } //获取列名到索引的映射（用于UPDATE中定位列）
private:
    std::vector<ColumnDef> columns;
    std::unordered_map<int, std::vector<Value>> rows;
    int nextRowId = 0;
    std::unordered_map<std::string, size_t> columnIndex; 
};
