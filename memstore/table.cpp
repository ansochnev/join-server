#include "memstore.h"
#include "table.h"

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