%code requires {
    #include <string>
    #include <vector>
    #include <utility>
    #include "storage.h"
    #include "dbms.h"
    struct AggFunction {
        std::string name;
        std::string arg;           // 列名或 "*"
    };
}

%union {
    int integer;
    std::string* string;
    std::vector<ColumnDef*>* col_list;
    ColumnDef* col_def;
    std::vector<Value*>* val_list;
    Value* value;
    Condition* cond;
    std::vector<std::pair<std::string, Value>>* set_list;
    std::pair<std::string, Value>* set_item;
    std::vector<std::string*>* id_list;
    struct ProjectionItem* proj_item;   // 自定义投影项结构
    std::vector<ProjectionItem*>* proj_list;
    struct AggFunction* agg_func;        // 聚合函数结构
    std::vector<std::pair<std::string, bool>>* order_list;   // 排序项列表，bool为true表示ASC
    std::pair<std::string, bool>* order_item;
}

%{
#include <stdio.h>
#include <stdlib.h>
#include "dbms.h"

extern int yylex();
void yyerror(const char *s);

extern DBMS dbms;
%}

/* 终结符声明 */
%token CREATE TABLE INSERT INTO VALUES SELECT FROM WHERE UPDATE SET DELETE DROP
%token INT_TYPE VARCHAR_TYPE AND OR
%token EQ NE LT GT LE GE
%token COMMA SEMICOLON LPAREN RPAREN STAR
%token KW_BEGIN KW_COMMIT KW_ROLLBACK  
%token COUNT SUM AVG MIN MAX 
%token ORDER BY ASC DESC

%token <integer> INTEGER
%token <string> IDENTIFIER STRING

%type <string> table_name column_name
%type <col_list> column_def_list
%type <col_def> column_def
%type <val_list> value_list
%type <value> value
%type <cond> condition where_opt
%type <set_list> set_list
%type <set_item> set_item
%type <proj_list> select_list
%type <proj_item> select_item
%type <agg_func> agg_func
%type <order_list> order_by_opt order_by_clause
%type <order_item> order_by_item


%start program

%%

program: statement_list ;

statement_list:
      statement
    | statement_list statement
    ;

statement:
      create_stmt SEMICOLON
    | drop_stmt SEMICOLON 
    | insert_stmt SEMICOLON
    | select_stmt SEMICOLON
    | update_stmt SEMICOLON
    | delete_stmt SEMICOLON
    | KW_BEGIN SEMICOLON {
          dbms.begin();
      }
    | KW_COMMIT SEMICOLON {
          dbms.commit();
      }
    | KW_ROLLBACK SEMICOLON {
          dbms.rollback();
      }
    ;

create_stmt:
      CREATE TABLE table_name LPAREN column_def_list RPAREN {
          std::vector<ColumnDef> cols;
          for (auto col : *$5) cols.push_back(*col);
          dbms.createTable(*$3, cols);
          delete $3;
          for (auto col : *$5) delete col;
          delete $5;
      }
    ;

drop_stmt:
      DROP TABLE table_name {
          dbms.dropTable(*$3);
          delete $3;
      }
    ;

column_def_list:
      column_def {
          $$ = new std::vector<ColumnDef*>();
          $$->push_back($1);
      }
    | column_def_list COMMA column_def {
          $1->push_back($3);
          $$ = $1;
      }
    ;

column_def:
      column_name INT_TYPE {
          $$ = new ColumnDef(*$1, DataType::INT);
          delete $1;
      }
    | column_name VARCHAR_TYPE {
          $$ = new ColumnDef(*$1, DataType::VARCHAR);
          delete $1;
      }
    ;

insert_stmt:
      INSERT INTO table_name VALUES LPAREN value_list RPAREN {
          std::vector<Value> vals;
          for (auto val : *$6) vals.push_back(*val);
          dbms.insertInto(*$3, vals);
          delete $3;
          for (auto val : *$6) delete val;
          delete $6;
      }
    ;

value_list:
      value {
          $$ = new std::vector<Value*>();
          $$->push_back($1);
      }
    | value_list COMMA value {
          $1->push_back($3);
          $$ = $1;
      }
    ;

value:
      INTEGER {
          $$ = new Value($1);
      }
    | STRING {
          $$ = new Value(*$1);
          delete $1;
      }
    ;

select_stmt:
      SELECT select_list FROM table_name where_opt order_by_opt {
          dbms.selectFrom(*$4, *$2, $5, $6);
          delete $4;
          for (auto item : *$2) delete item;
          delete $2;
          delete $5;
          if ($6) {
              for (auto& p : *$6) ; 
              delete $6;
          }
      }
    ;

order_by_opt:
      /* empty */ { $$ = nullptr; }
    | ORDER BY order_by_clause { $$ = $3; }
    ;

order_by_clause:
      order_by_item {
          $$ = new std::vector<std::pair<std::string, bool>>();
          $$->push_back(*$1);
          delete $1;
      }
    | order_by_clause COMMA order_by_item {
          $1->push_back(*$3);
          delete $3;
          $$ = $1;
      }
    ;

order_by_item:
      column_name {
          $$ = new std::pair<std::string, bool>(*$1, true); // 默认ASC
          delete $1;
      }
    | column_name ASC {
          $$ = new std::pair<std::string, bool>(*$1, true);
          delete $1;
      }
    | column_name DESC {
          $$ = new std::pair<std::string, bool>(*$1, false);
          delete $1;
      }
    ;

select_list:
      STAR {
          $$ = new std::vector<ProjectionItem*>();
          ProjectionItem* item = new ProjectionItem;
          item->isAgg = false;
          item->star = true;
          $$->push_back(item);
      }
    | select_item {
          $$ = new std::vector<ProjectionItem*>();
          $$->push_back($1);
      }
    | select_list COMMA select_item {
          $1->push_back($3);
          $$ = $1;
      }
    ;
  
select_item:
      column_name {
          $$ = new ProjectionItem;
          $$->isAgg = false;
          $$->colName = *$1;
          $$->star = false;
          delete $1;
      }
    | agg_func {
          $$ = new ProjectionItem;
          $$->isAgg = true;
          $$->funcName = $1->name;
          $$->colName = $1->arg;
          $$->star = ($1->arg == "*");
          delete $1;
      }
    ;

agg_func:
      COUNT LPAREN STAR RPAREN {
          $$ = new AggFunction;
          $$->name = "COUNT";
          $$->arg = "*";
      }
    | COUNT LPAREN column_name RPAREN {
          $$ = new AggFunction;
          $$->name = "COUNT";
          $$->arg = *$3;
          delete $3;
      }
    | SUM LPAREN column_name RPAREN {
          $$ = new AggFunction;
          $$->name = "SUM";
          $$->arg = *$3;
          delete $3;
      }
    | AVG LPAREN column_name RPAREN {
          $$ = new AggFunction;
          $$->name = "AVG";
          $$->arg = *$3;
          delete $3;
      }
    | MIN LPAREN column_name RPAREN {
          $$ = new AggFunction;
          $$->name = "MIN";
          $$->arg = *$3;
          delete $3;
      }
    | MAX LPAREN column_name RPAREN {
          $$ = new AggFunction;
          $$->name = "MAX";
          $$->arg = *$3;
          delete $3;
      }
    ;

where_opt:
      /* empty */ { $$ = nullptr; }
    | WHERE condition { $$ = $2; }
    ;

condition:
      column_name EQ value {
          $$ = new Condition(*$1, Op::EQ, *$3);
          delete $1; delete $3;
      }
    | column_name NE value {
          $$ = new Condition(*$1, Op::NE, *$3);
          delete $1; delete $3;
      }
    | column_name LT value {
          $$ = new Condition(*$1, Op::LT, *$3);
          delete $1; delete $3;
      }
    | column_name GT value {
          $$ = new Condition(*$1, Op::GT, *$3);
          delete $1; delete $3;
      }
    | column_name LE value {
          $$ = new Condition(*$1, Op::LE, *$3);
          delete $1; delete $3;
      }
    | column_name GE value {
          $$ = new Condition(*$1, Op::GE, *$3);
          delete $1; delete $3;
      }
    | condition AND condition {
          delete $1; delete $3;
          $$ = nullptr;
          fprintf(stderr, "Warning: AND condition not supported, ignored.\n");
      }
    | condition OR condition {
          delete $1; delete $3;
          $$ = nullptr;
          fprintf(stderr, "Warning: OR condition not supported, ignored.\n");
      }
    ;

update_stmt:
      UPDATE table_name SET set_list where_opt {
          dbms.update(*$2, *$4, $5);
          delete $2; delete $4; delete $5;
      }
    ;

set_list:
      set_item {
          $$ = new std::vector<std::pair<std::string, Value>>();
          $$->push_back(*$1);
          delete $1;
      }
    | set_list COMMA set_item {
          $1->push_back(*$3);
          delete $3;
          $$ = $1;
      }
    ;

set_item:
      column_name EQ value {
          $$ = new std::pair<std::string, Value>(*$1, *$3);
          delete $1;
          delete $3;
      }
    ;

delete_stmt:
      DELETE FROM table_name where_opt {
          dbms.deleteFrom(*$3, $4);
          delete $3; delete $4;
      }
    ;

table_name: IDENTIFIER { $$ = $1; }
column_name: IDENTIFIER { $$ = $1; }

%%

void yyerror(const char *s) {
    fprintf(stderr, "Syntax error: %s\n", s);
}