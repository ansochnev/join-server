#include "memstore.h"
#include "parse.h"
#include "db.h"

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