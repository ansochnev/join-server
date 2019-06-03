#include "memstore.h"
#include "table.h"

DataObject::DataObject(sql::DataType t) 
        : m_type(t), m_value(nullptr) {}

DataObject::DataObject(long val) 
    : m_type(sql::DataType::INTEGER), m_value(new long(val)) {}

DataObject::DataObject(const std::string& val) 
    : m_type(sql::DataType::TEXT), m_value(new std::string(val)) {}

DataObject::DataObject(const DataObject& other) 
    : m_type(other.m_type), m_value(nullptr) 
{ 
    *this = other; 
}

DataObject::DataObject(DataObject&& other) 
    : m_type(other.m_type), m_value(other.m_value) 
{
    other.m_value = nullptr;
}
    
DataObject::~DataObject() {}


Table::Table(const Schema& s) : m_schema(s) {}
Table::Table(Schema&& s)      : m_schema(std::move(s)) {}


Table::RowID Table::insert(std::vector<DataObject>&& values)
{
    std::size_t i = m_schema.primaryKeyIndex();

    // Check whether the record with the same primary key exists.
    switch (m_schema[i].type())
    {
    case sql::DataType::INTEGER:
        for (auto& row : m_rows) {
            if (row[i].getLong() == values[i].getLong()) {
                throw sql::Exception("duplicate");
            }
        }   
        break;

    case sql::DataType::TEXT:
        for (auto& row : m_rows) {
            if (row[i].getString() == values[i].getString()) {
                throw sql::Exception("duplicate");
            }
        }   
        break;
    }

    std::vector<Cell> row;
    row.reserve(values.size());

    for (std::size_t i = 0; i < values.size(); ++i)
        row[i] = values[i];

    return m_rows.size() - 1;
}
