#include <iostream>
#include <memory>

#include "protocol.h"
#include "joiner.h"
#include "memstore/memstore.h"


int main(int argc, char* argv[]) 
{
    if (argc != 2) {
        std::cout << "too few arguments" << std::endl;
        return 1;
    }

    sql::IDBConnection *db = mem::open();

    try {
        int port = std::stoi(argv[1]);
        proto::Server server(port, new Joiner(db));
        server.run();
    }
    catch(std::exception& e) {
        std::cout << e.what();
    }

    db->close();
    delete db;

    return 0;
}
