#ifndef MEM_DB_H
#define MEM_DB_H

#include <string>
#include <vector>
#include <map>

#include "memstore.h"
#include "schema.h"

class Table;

class DataObject
{
    friend Table;
    sql::DataType  m_type;
    void          *m_value;

public:
    DataObject(sql::DataType t) 
        : m_type(t), m_value(nullptr) {}

    DataObject(long val) 
        : m_type(sql::DataType::INTEGER), m_value(new long(val)) {}

    DataObject(const std::string& val) 
        : m_type(sql::DataType::TEXT), m_value(new std::string(val)) {}

    DataObject(const DataObject& other) 
        : m_type(other.m_type), m_value(nullptr) 
    { 
        *this = other; 
    }

    DataObject(DataObject&& other) 
        : m_type(other.m_type), m_value(other.m_value) 
    {
        other.m_value = nullptr;
    }
        
    ~DataObject();

    DataObject& operator= (const DataObject& rhs);
    DataObject& operator= (DataObject&& rhs);

    sql::DataType type() const noexcept {
        return m_type;
    }
    
    bool isNull() const noexcept { 
        return !m_value;
    }
    
    long getLong() const;
    std::string getString() const;
};


class Table
{    
    Schema                           m_schema;
    std::vector<std::vector<void*>>  m_rows;

public:
    using RowID = std::size_t;

    Table(const Schema& s) : m_schema(s) {}
    
    const Schema& schema() const noexcept { return m_schema; }

    RowID insert(std::vector<DataObject>&& row);
    void  remove(RowID row);

    void begin() const;
    void end() const;

private:
    bool equal(void* lhs, void* rhs, std::size_t columnIndex);
    bool less(void* lhs, void* rhs, std::size_t columnIndex);
    bool greater(void* lhs, void* rhs, std::size_t columnIndex);

    class Row
    {
        const Table  *m_table;
        RowID         m_row;
    public:
        bool isNull(int column) const {
            return m_table->m_rows[m_row][column] == nullptr;
        }

        long getLong(int column) const 
        {
            if (isNull(column)) {
                throw std::invalid_argument("DataObject::getLong: null");
            }
            return *reinterpret_cast<long*>(m_table->m_rows[m_row][column]); 
        }

        std::string getString(int column) const 
        {
            if (isNull(column)) {
                throw std::invalid_argument("DataObject::getLong: null");
            }
            return *reinterpret_cast<std::string*>(m_table->m_rows[m_row][column]); 
        }
    };

    class ConstIterator 
    {
    public:
        void operator++ ();
    };
};


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