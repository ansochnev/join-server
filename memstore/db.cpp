#include <stdexcept>
#include "db.h"

DataObject::~DataObject()
{
    if (m_type == sql::DataType::INTEGER) {
        delete reinterpret_cast<long*>(m_value);
    }
    else if (m_type == sql::DataType::TEXT) {
        delete reinterpret_cast<std::string*>(m_value);
    }
}


long DataObject::getLong() const 
{ 
    if (!m_value) {
        throw std::invalid_argument("DataObject::getLong: null");
    }
    return *reinterpret_cast<long*>(m_value); 
}


std::string DataObject::getString() const 
{ 
    if (!m_value) {
        throw std::invalid_argument("DataObject::getString: null");
    }
    return *reinterpret_cast<std::string*>(m_value); 
}


DataObject& DataObject::operator= (const DataObject& rhs)
{
    if (m_type != rhs.m_type) {
        throw std::invalid_argument("DataObject: type mismatch");
    }

    if (rhs.isNull()) {
        return *this;
    }

    if (m_type == sql::DataType::INTEGER) {
        m_value = new long(rhs.getLong());
    }
    else if (m_type == sql::DataType::TEXT) {
        m_value = new std::string(rhs.getString());
    }
    else {
        throw "DataObject: unreachable code";
    }

    return *this;
}


DataObject& DataObject::operator= (DataObject&& rhs)
{
    if (m_type != rhs.m_type) {
        throw std::invalid_argument("type mismatch");
    }
    m_value = rhs.m_value;
    rhs.m_value = nullptr;
    return *this;
}


Table::RowID Table::insert(std::vector<DataObject>&& row)
{
    std::vector<void*> r;
    r.reserve(row.size());

    std::size_t primaryKeyIndex;
    for (std::size_t i = 0; i < m_schema.size(); ++i) {
        if (m_schema[i].isPrimaryKey()) {
            primaryKeyIndex = i;
        }
    }

    for (auto r : m_rows) {
        if (equal(row[primaryKeyIndex].m_value, 
                  r[primaryKeyIndex], primaryKeyIndex))
        {
            throw sql::Exception("duplicate");
        }
    }

    for (DataObject d : row) {
        r.push_back(d.m_value);
        d.m_value = nullptr;
    }

    m_rows.push_back(std::move(r));
    return m_rows.size() - 1;
}

bool Table::less(void* lhs, void* rhs, std::size_t columnIndex)
{
    ColumnInfo col = m_schema[columnIndex];
    if (col.type() == sql::DataType::INTEGER) {
        return *reinterpret_cast<long*>(lhs) < 
               *reinterpret_cast<long*>(rhs);
    }
    else if (col.type() == sql::DataType::TEXT) {
        return *reinterpret_cast<std::string*>(lhs) < 
               *reinterpret_cast<std::string*>(rhs);
    }
    else {
        throw sql::Exception("less: unreachable code");
    }
}

bool Table::greater(void* lhs, void* rhs, std::size_t columnIndex) {
    return less(rhs, lhs, columnIndex);
}

bool Table::equal(void* lhs, void* rhs, std::size_t columnIndex) {
    return !less(lhs, rhs, columnIndex) && !greater(lhs, rhs, columnIndex);
}

Selection Memstore::selectAll(const std::string& tableName)
{
    for (auto row : m_tables[tableName]) {
        if (row["id"] == 28) {
            row["name"] == "hello";
        }
    }

    auto table = m_tables[tableName];
    auto row = table.getRow(28);
    row["id"] == 29;
}