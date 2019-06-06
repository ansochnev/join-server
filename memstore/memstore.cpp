#include <string>
#include <vector>
#include <map>
#include <sstream>

#include "memstore.h"
#include "table.h"


sql::IDBConnection* mem::open();

class Connection;
class Statement;
class FullTableSelection;
class Memstore;

void assertEq(const std::string& have, const std::string& expect);
Schema parseSchema(const std::string& s);
std::vector<DataObject> parseValues(const std::string& s, const Schema& schema);



class FullTableSelection : public sql::ISelection
{
    Table::iterator m_currentRow;
    Table::iterator m_end;
public:
    FullTableSelection(const Table::iterator& begin, 
                       const Table::iterator& end)
        : m_currentRow(begin), m_end(end) {}

    void next() override { ++m_currentRow; }
    bool end()  override { return m_currentRow == m_end; }
    
    bool isNull(std::size_t columnIndex) override {
        return (*m_currentRow).isNull(columnIndex);
    }

    long getLong(std::size_t columnIndex) override {
        return (*m_currentRow).getLong(columnIndex);
    }

    std::string getString(std::size_t columnIndex) override {
        return (*m_currentRow).getString(columnIndex);
    }

    void close() override {}
};



class Memstore
{
    std::map<std::string, Table*> m_tables;

public:
    void createTable(const std::string& tableName, const Schema& schema) {
        m_tables.emplace(tableName, new Table(schema));
    }

    bool hasTable(const std::string& tableName) const {
        return m_tables.find(tableName) != m_tables.end();
    }

    const Schema& tableSchema(const std::string& tableName) {
        return m_tables[tableName]->schema();
    }

    void insert(const std::string& tableName, std::vector<DataObject>&& row) {
        m_tables[tableName]->insert(std::move(row));
    }

    FullTableSelection* selectAll(const std::string& tableName) {
        Table *tab = m_tables[tableName];
        return new FullTableSelection(tab->begin(), tab->end());
    }
};



class Statement : public sql::IStatement
{
    Memstore        *m_db;
    sql::ISelection *m_selection;

public:
    Statement(Memstore* db) : m_db(db), m_selection(nullptr) {}
    ~Statement() { close(); }

    void modify(const std::string& query) override {
        execute(query);
    }

    sql::ISelection* select(const std::string& query) override {
        execute(query);
        return m_selection;
    }

    void close() override { if (m_selection) delete m_selection; }

private:
    void execute(const std::string& query)
    {
        std::istringstream sq(query);
        std::string command;

        sq >> command;
        command = toUpper(command);

        if (command == "CREATE") {
            executeCreate(sq);
        }
        else if (command == "INSERT") {
            executeInsert(sq);
        }
        else if (command == "DELETE") {
            executeDelete(sq);
        } 
        else if (command == "SELECT") {
            executeSelect(sq);
        }
    }


    void executeCreate(std::istringstream& query)
    {
        std::string token;

        query >> token;
        assertEq(toUpper(token), "TABLE");

        std::string tableName;
        query >> tableName;

        if (m_db->hasTable(tableName)) {
            throw sql::Exception(
                fmt::sprintf("table %v already exists", tableName));
        }

        std::string sch;
        while(query >> token) {
            sch.append(token + " ");
        }

        Schema schema = parseSchema(sch);
        m_db->createTable(tableName, schema);
    }


    void executeInsert(std::istringstream& query)
    {
        std::string token;

        query >> token;
        assertEq(toUpper(token), "INTO");

        std::string tableName;
        query >> tableName;

        if (!m_db->hasTable(tableName)) {
            throw sql::Exception(
                fmt::sprintf("table %v does not exist", tableName));
        }

        query >> token;
        assertEq(token, "VALUES");

        std::string values;
        while(query >> token) {
            values.append(token + " ");
        }

        auto row = parseValues(values, m_db->tableSchema(tableName));
        m_db->insert(tableName, std::move(row));
    }
    

    void executeDelete(std::istringstream& query [[maybe_unused]])
    {
    }


    void executeSelect(std::istringstream& query)
    {
        std::vector<std::string> tokens;
        
        std::string token;
        while(query >> token) {
            tokens.push_back(std::move(token));
        }

        if (tokens.size() == 3) {
            executeSelectAll(std::move(tokens));
        } 
        else {
            throw sql::Exception("select anything");
        }
    }


    void executeSelectAll(std::vector<std::string>&& tokens)
    {
        assertEq(tokens[0], "*");
        assertEq(toUpper(tokens[1]), "FROM");

        std::string tableName = trimRight(tokens[2], ";");

        if (!m_db->hasTable(tableName)) {
            throw sql::Exception(
                fmt::sprintf("table %v does not exist", tableName));
        }

        m_selection = m_db->selectAll(tableName);
    }
};



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
