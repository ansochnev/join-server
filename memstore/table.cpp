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
