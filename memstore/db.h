#ifndef MEM_DB_H
#define MEM_DB_H

#include <string>
#include <vector>
#include <map>

#include "memstore.h"
#include "schema.h"


class Selection : public sql::ISelection
{
    
public:
    bool hasNext() override;
    void next() override;
    
    bool isNull(std::size_t columnIndex) override;
    long getLong(std::size_t columIndex) override;
    std::string getString(std::size_t columnIndex) override;
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

    Selection selectAll(const std::string& tableName);
};

#endif // MEM_DB_H