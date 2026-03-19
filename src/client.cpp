#include <iostream>
#include <string>
#include <cstring>
#include <unistd.h>
#include <sys/socket.h>
#include <arpa/inet.h>

const int PORT = 8888;
const char* SERVER_IP = "127.0.0.1";
const int BUFFER_SIZE = 4096;

int main() {
    // 创建 socket
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        std::cerr << "Failed to create socket" << std::endl;
        return 1;
    }

    // 配置服务器地址
    struct sockaddr_in server_addr;
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(PORT);
    if (inet_pton(AF_INET, SERVER_IP, &server_addr.sin_addr) <= 0) {
        std::cerr << "Invalid address / Address not supported" << std::endl;
        return 1;
    }

    // 连接服务器
    if (connect(sock, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        std::cerr << "Connection failed" << std::endl;
        return 1;
    }

    std::cout << "Connected to MiniDB server at " << SERVER_IP << ":" << PORT << std::endl;
    std::cout << "Enter SQL commands (or 'exit' to quit):" << std::endl;

    std::string line;
    char buffer[BUFFER_SIZE];

    while (true) {
        std::cout << "minidb> ";
        std::getline(std::cin, line);
        if (line == "exit" || line == "quit") {
            break;
        }
        if (line.empty()) continue;

        // 发送 SQL 语句（服务器期望以换行结束）
        line += "\n";
        send(sock, line.c_str(), line.size(), 0);

        // 接收服务器响应
        ssize_t bytes_read = recv(sock, buffer, BUFFER_SIZE - 1, 0);
        if (bytes_read <= 0) {
            std::cerr << "Server disconnected" << std::endl;
            break;
        }
        buffer[bytes_read] = '\0';
        std::cout << buffer;
    }

    close(sock);
    std::cout << "Disconnected." << std::endl;
    return 0;
}