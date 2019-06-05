#ifndef MEM_DB_H
#define MEM_DB_H

#include <string>
#include <vector>
#include <map>

#include "table.h"

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

#endif // MEM_DB_H