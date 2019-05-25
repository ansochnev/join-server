#include <iostream>
#include <memory>

#include "protocol.h"
#include "handler.h"

int main(int argc, char* argv[]) 
{
    if (argc != 2) {
        std::cout << "too few arguments" << std::endl;
        return 1;
    }

    try {
        int port = std::stoi(argv[1]);
        proto::Server server(port, new Handler());
        server.run();
    }
    catch(std::exception& e) {
        std::cout << e.what();
    }

    return 0;
}
