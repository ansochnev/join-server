#include "schema.h"

std::size_t Schema::addColumn(const ColumnInfo& ci)
{
    for (auto& col : m_columns) {
        if (col.name() == ci.name()) {
            throw sql::Exception("column " + col.name() + " already exists");
        }
    }
    m_columns.push_back(ci);
    return m_columns.size() - 1;
}

const ColumnInfo& Schema::operator[] (const std::string& name) const
{
    for (const ColumnInfo& col : *this) {
        if (col.name() == name) {
            return col;
        }
    }
    throw sql::Exception("column " + name + " does not exist");
}

std::size_t Schema::primaryKeyIndex() const
{
    for (std::size_t i = 0; i < m_columns.size(); ++i) {
        if (m_columns[i].isPrimaryKey()) {
            return i;
        }
    }
}