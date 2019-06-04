#include "memstore.h"
#include "table.h"

Table::Table(const Schema& s) : m_schema(s) {}
Table::Table(Schema&& s)      : m_schema(std::move(s)) {}


bool Table::isSatisfySchema(const std::vector<DataObject>& values)
{
    if (m_schema.size() != values.size()) {
        return false;
    }
   
    for (std::size_t i = 0; i < m_schema.size(); ++i) {
        if (m_schema[i].type() != values[i].type()) {
            return false;
        }
    }
    return true;
}


bool Table::isUnique(const std::vector<DataObject>& values)
{
    std::size_t i = m_schema.primaryKeyIndex();

    switch (m_schema[i].type())
    {
    case sql::DataType::INTEGER:
        for (auto& row : m_rows) {
            if (row[i].getLong() == values[i].getLong()) {
                return false;
            }
        }   
        break;

    case sql::DataType::TEXT:
        for (auto& row : m_rows) {
            std::cout << row[i].getString() << " " << values[i].getString();
            if (row[i].getString() == values[i].getString()) {
                return false;
            }
        }   
        break;
    }
    return true;
}


Table::RowID Table::insert(std::vector<DataObject>&& values)
{
    if (!isSatisfySchema(values)) {
        throw sql::Exception("Table: mismatch schema");
    }
    
    if (!isUnique(values)) {
        throw sql::Exception("duplicate");
    }
    
    std::vector<Cell> row;
    row.resize(values.size());

    for (std::size_t i = 0; i < values.size(); ++i) {
        row[i] = values[i];
    }

    m_rows.push_back(std::move(row));
    return m_rows.size() - 1;
}


/*
void  remove(RowID row);
*/


Table::Cell& Table::Cell::operator= (Cell&& other)
{
    if (m_holder) delete m_holder;
    m_holder = other.m_holder;
    other.m_holder = nullptr;
    return *this;
}


Table::Cell& Table::Cell::operator= (long n) 
{ 
    if (m_holder) this->cast<long>() = n; 
    else m_holder = new Holder<long>(n);
    return *this;
}


Table::Cell& Table::Cell::operator= (const std::string& s) 
{
    if (m_holder) this->cast<std::string>() = s;
    else m_holder = new Holder<std::string>(s);
    return *this;
}


Table::Cell& Table::Cell::operator= (std::string&& s) 
{ 
    if (m_holder) this->cast<std::string>() = std::move(s);
    else m_holder = new Holder<std::string>(std::move(s));
    return *this;
}


Table::Cell& Table::Cell::operator= (const DataObject& d)
{
    switch (d.type())
    {
    case sql::DataType::INTEGER:
        *this = d.getLong();
        break;

    case sql::DataType::TEXT:
        *this = d.getString();
        break;
    }
    return *this;
}