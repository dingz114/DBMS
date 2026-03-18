%code requires {
    #include <string>
    #include <vector>
    #include <utility>
    #include "storage.h"
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
}

%{
#include <stdio.h>
#include <stdlib.h>
#include "dbms.h"

extern int yylex();
void yyerror(const char *s);

extern DBMS dbms;
%}

%token CREATE TABLE INSERT INTO VALUES SELECT FROM WHERE UPDATE SET DELETE
%token INT_TYPE VARCHAR_TYPE AND OR
%token EQ NE LT GT LE GE
%token COMMA SEMICOLON LPAREN RPAREN STAR

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
%type <id_list> column_list

%start program

%%

program: statement_list ;

statement_list:
      statement
    | statement_list statement
    ;

statement:
      create_stmt SEMICOLON
    | insert_stmt SEMICOLON
    | select_stmt SEMICOLON
    | update_stmt SEMICOLON
    | delete_stmt SEMICOLON
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
      SELECT select_list FROM table_name where_opt {
          dbms.selectFrom(*$4, $5);
          delete $4;
          delete $5;
      }
    ;

select_list:
      STAR
    | column_list {
          delete $1;
      }
    ;

column_list:
      column_name {
          $$ = new std::vector<std::string*>();
          $$->push_back($1);
      }
    | column_list COMMA column_name {
          $1->push_back($3);
          $$ = $1;
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