#include <iostream>
#include <string>
#include "lexer.h"   
#include "dbms.h"

extern int yyparse();
DBMS dbms;

int main() {
    std::string line;
    std::cout << "MiniDB> ";
    while (std::getline(std::cin, line)) {
        YY_BUFFER_STATE buffer = yy_scan_string(line.c_str());
        yyparse();
        yy_delete_buffer(buffer);
        std::cout << "MiniDB> ";
    }
    return 0;
}