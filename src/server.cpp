#include <iostream>
#include <string>
#include <cstring>
#include <thread>
#include <sstream>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include "dbms.h"
#include "lexer.h"

extern int yyparse();
extern DBMS dbms;

const int PORT = 8888;
const int BUFFER_SIZE = 4096;

void handleClient(int client_sock) {
    char buffer[BUFFER_SIZE];
    while (true) {
        int bytes_read = read(client_sock, buffer, sizeof(buffer) - 1);
        if (bytes_read <= 0) {
            close(client_sock);
            return;
        }
        buffer[bytes_read] = '\0';
        std::string sql(buffer);

        // 重定向标准输出到字符串流
        std::stringstream output;
        std::streambuf* old_cout = std::cout.rdbuf(output.rdbuf());

        // 解析并执行 SQL
        YY_BUFFER_STATE buf = yy_scan_string(sql.c_str());
        yyparse();
        yy_delete_buffer(buf);

        // 恢复标准输出
        std::cout.rdbuf(old_cout);

        // 发送结果回客户端
        std::string response = output.str();
        write(client_sock, response.c_str(), response.length());
    }
}

int main() {
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        std::cerr << "Failed to create socket\n";
        return 1;
    }

    int opt = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        std::cerr << "setsockopt failed\n";
        return 1;
    }

    struct sockaddr_in address;
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(PORT);

    if (bind(server_fd, (struct sockaddr*)&address, sizeof(address)) < 0) {
        std::cerr << "Bind failed\n";
        return 1;
    }

    if (listen(server_fd, 5) < 0) {
        std::cerr << "Listen failed\n";
        return 1;
    }

    std::cout << "MiniDB server listening on port " << PORT << std::endl;

    while (true) {
        int addrlen = sizeof(address);
        int client_sock = accept(server_fd, (struct sockaddr*)&address, (socklen_t*)&addrlen);
        if (client_sock < 0) {
            std::cerr << "Accept failed\n";
            continue;
        }
        std::cout << "New client connected\n";
        std::thread(handleClient, client_sock).detach();
    }

    close(server_fd);
    return 0;
}