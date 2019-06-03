#ifndef SCHEMA_H
#define SCHEMA_H

#include <vector>
#include "memstore.h"

class ColumnInfo 
{
    std::string   m_name;
    sql::DataType m_type;
    bool          m_pkey;

public:
    ColumnInfo(const std::string& name, sql::DataType t, bool primaryKey) 
        : m_name(name), m_type(t), m_pkey(primaryKey) {}

    sql::DataType type() const      { return m_type; }
    const std::string& name() const { return m_name; }
    bool isPrimaryKey() const       { return m_pkey; }
};


class Schema
{
    std::vector<ColumnInfo> m_columns;

public:
    Schema() = default;

    std::size_t addColumn(const ColumnInfo& ci);
    std::size_t primaryKeyIndex() const;

    const ColumnInfo& operator[] (int index) const { return m_columns[index]; }
    const ColumnInfo& operator[] (const std::string& name) const;

    std::size_t size() const { return m_columns.size(); }

    using const_iterator = std::vector<ColumnInfo>::const_iterator;
    const_iterator begin() const noexcept { return m_columns.cbegin(); }
    const_iterator end()   const noexcept { return m_columns.cend();   }
};

#endif // SCHEMA_H