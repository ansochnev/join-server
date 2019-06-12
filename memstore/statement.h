#ifndef STATEMENT_H
#define STATEMENT_H

#include "memstore.h"
#include "storage.h"

void assertEq(const std::string& have, const std::string& expect);
Schema parseSchema(const std::string& s);
std::vector<DataObject> parseValues(const std::string& s, const Schema& schema);


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
    void execute(const std::string& query);
    void executeCreate(std::istringstream& query);
    void executeInsert(std::istringstream& query);
    void executeDelete(std::istringstream& query);
    void executeSelect(std::istringstream& query);
    void executeSelectAll(std::vector<std::string>&& tokens);
    void executeSelectWithJoin(std::vector<std::string>&& tokens);
    void executeSelectWithJoinWithWhere(std::vector<std::string>&& tokens);
};


void Statement::execute(const std::string& query)
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


void Statement::executeCreate(std::istringstream& query)
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


void Statement::executeInsert(std::istringstream& query)
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


void Statement::executeDelete(std::istringstream& query)
{
    std::string token;
    query >> token;
    assertEq(token, "FROM");

    query >> token;
    std::string tableName = trimRight(token, ";");
    m_db->truncate(tableName);
}


void Statement::executeSelect(std::istringstream& query)
{
    std::vector<std::string> tokens;
    
    std::string token;
    while(query >> token) {
        tokens.push_back(std::move(token));
    }

    if (tokens.size() == 3) {
        executeSelectAll(std::move(tokens));
    } 
    else if (tokens.size() == 9) {
        executeSelectWithJoin(std::move(tokens));
    }
    else if (tokens.size() == 19) {
        executeSelectWithJoinWithWhere(std::move(tokens));
    }
    else {
        throw sql::Exception("bad select");
    }
}


void Statement::executeSelectAll(std::vector<std::string>&& tokens)
{
    assertEq(tokens[0], "*");
    assertEq(toUpper(tokens[1]), "FROM");

    std::string tableName = trimRight(tokens[2], ";");

    if (!m_db->hasTable(tableName)) {
        throw sql::Exception(
            fmt::sprintf("table %v does not exist", tableName));
    }

    if (m_selection) delete m_selection;
    m_selection = m_db->selectAll(tableName);
}


void Statement::executeSelectWithJoin(std::vector<std::string>&& tokens)
{
    std::string table1 = tokens[2];
    std::string table2 = tokens[4];
    std::string column1 = split(tokens[6], '.')[1];
    std::string column2 = trimRight(split(tokens[8], '.')[1], ";");
    
    if (m_selection) delete m_selection;
    m_selection = m_db->getInnerJoin(table1, table2, column1, column2);
}


void Statement::executeSelectWithJoinWithWhere(std::vector<std::string>&& tokens)
{
    std::string table1 = tokens[2];
    std::string table2 = tokens[6];
    std::string column1 = split(tokens[8], '.')[1];
    std::string column2 = trimRight(split(tokens[10], '.')[1], ";");
    
    if (m_selection) delete m_selection;
    m_selection = m_db->getFullOuterJoin(table1, table2, column1, column2);
}


#endif // STATEMENT_H