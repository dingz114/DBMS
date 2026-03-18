%option noyywrap
%{
#include <string>
#include <cstdlib>
#include "parser.h"
%}

%%

[ \t\n\r]+           ;
"CREATE"             { return CREATE; }
"TABLE"              { return TABLE; }
"INSERT"             { return INSERT; }
"INTO"               { return INTO; }
"VALUES"             { return VALUES; }
"SELECT"             { return SELECT; }
"FROM"               { return FROM; }
"WHERE"              { return WHERE; }
"UPDATE"             { return UPDATE; }
"SET"                { return SET; }
"DELETE"             { return DELETE; }
"INT"                { return INT_TYPE; }
"VARCHAR"            { return VARCHAR_TYPE; }
"AND"                { return AND; }
"OR"                 { return OR; }
"="                  { return EQ; }
"!="                 { return NE; }
"<"                  { return LT; }
">"                  { return GT; }
"<="                 { return LE; }
">="                 { return GE; }
","                  { return COMMA; }
";"                  { return SEMICOLON; }
"("                  { return LPAREN; }
")"                  { return RPAREN; }
"*"                  { return STAR; }

[0-9]+               { yylval.integer = atoi(yytext); return INTEGER; }
'[^']*'              {
    yylval.string = new std::string(yytext+1, strlen(yytext)-2);
    return STRING;
}
[a-zA-Z_][a-zA-Z0-9_]* {
    yylval.string = new std::string(yytext);
    return IDENTIFIER;
}

.                    ;
%%