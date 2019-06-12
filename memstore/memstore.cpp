#include "memstore.h"
#include "storage.h"
#include "statement.h"

class Connection : public sql::IDBConnection
{
    Memstore m_db;
public:
    sql::IStatement* createStatement() override {
        return new Statement(&m_db);
    }

    void close() override {}
};


sql::IDBConnection* mem::open() {
    return new Connection();
}

