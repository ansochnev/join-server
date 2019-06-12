#include "memstore.h"
#include "table.h"

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
    throw sql::Exception("Schema: primary key not found");
}


bool Schema::contains(const std::string& columnName) const
{
    for (const ColumnInfo& column : m_columns) {
        if (column.name() == columnName) {
            return true;
        }
    }
    return false;
}


std::size_t Schema::indexOf(const std::string& columnName) const
{
    for (std::size_t i = 0; i < m_columns.size(); ++i) {
        if (m_columns[i].name() == columnName) {
            return i;
        }
    }
    throw sql::Exception("Schema: column '" + columnName + "' does not exist");
}


sql::DataType Schema::typeOf(std::size_t columnIndex) const 
{
    return m_columns[columnIndex].type();
}


sql::DataType Schema::typeOf(const std::string& columnName) const
{
    std::size_t i = indexOf(columnName);
    return m_columns[i].type();
}


Table::Table(const Schema& s) : m_schema(s) 
{
    m_indices.reserve(m_schema.size());
    for (std::size_t i = 0; i < m_schema.size(); ++i) {
        m_indices.push_back(nullptr);
    }

    switch (m_schema.typeOf(m_schema.primaryKeyIndex())) 
    {
    case sql::DataType::INTEGER:
        m_indices[m_schema.primaryKeyIndex()] = new Index<long>();
        break;
    case sql::DataType::TEXT:
        m_indices[m_schema.primaryKeyIndex()] = new Index<std::string>();
        break;
    }
}


Table::Table(Schema&& s) : m_schema(std::move(s)) 
{
    m_indices.reserve(m_schema.size());
    for (std::size_t i = 0; i < m_schema.size(); ++i) {
        m_indices.push_back(nullptr);
    }

    switch (m_schema.typeOf(m_schema.primaryKeyIndex())) 
    {
    case sql::DataType::INTEGER:
        m_indices[m_schema.primaryKeyIndex()] = new Index<long>();
        break;
    case sql::DataType::TEXT:
        m_indices[m_schema.primaryKeyIndex()] = new Index<std::string>();
        break;
    }
}


Table::~Table()
{
    for (AbstractIndex* index : m_indices) {
        if (index) delete index;
    }
}


bool Table::isSatisfySchema(const std::vector<DataObject>& values) const
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


bool Table::isUnique(const std::vector<DataObject>& values) const
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
            if (row[i].getString() == values[i].getString()) {
                return false;
            }
        }   
        break;
    }
    return true;
}


Table::RowID Table::insert(Record&& values)
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
    std::size_t rowID =  m_rows.size() - 1;

    std::size_t pkey = m_schema.primaryKeyIndex();
    switch (m_schema.typeOf(pkey)) 
    {
    case sql::DataType::INTEGER:
        {
            Index<long> *pLongIndex = static_cast<Index<long>*>(m_indices[pkey]);
            pLongIndex->insert(m_rows[rowID][pkey].getLong(), rowID);
        }
        break;

    case sql::DataType::TEXT:
        {
            Index<std::string> *pStringIndex = 
                static_cast<Index<std::string>*>(m_indices[pkey]);
            pStringIndex->insert(m_rows[rowID][pkey].getString(), rowID);
        }
        break;
    }

    return rowID;
}


void Table::truncate()
{
    m_rows.clear();
    for (AbstractIndex* index : m_indices) {
        if (index) index->clear();
    }
}

Record Table::makeRecord(RowID row) const
{
    Record record;
    record.reserve(m_schema.size());

    if (row == std::size_t(-1)) {
        for (const ColumnInfo& col : m_schema) {
            record.emplace_back(DataObject(col.type()));
        }
        return record;
    }


    for (std::size_t i = 0; i < m_schema.size(); ++i) {
        record.emplace_back(m_rows[row][i].toDataObject(m_schema[i].type()));
    }
    return record;
}


std::vector<Record> Table::select(const std::vector<RowID>& rows) const
{
    std::vector<Record> records;
    records.reserve(rows.size());
    for (RowID row : rows) {
        records.push_back(makeRecord(row));
    }
    return records;
}


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


DataObject Table::Cell::toDataObject(sql::DataType type) const
{
    if (this->isNull()) return DataObject(type);

    switch (type)
    {
    case sql::DataType::INTEGER:
        return DataObject(this->cast<long>());
    case sql::DataType::TEXT:
        return DataObject(this->cast<std::string>());
    default:
        throw sql::Exception("Cell: unsupported type");
    } 
}