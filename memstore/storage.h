#ifndef STORAGE_H
#define STORAGE_H

#include <map>
#include <mutex>
#include <shared_mutex>

#include "memstore.h"
#include "table.h"
#include "selection.h"


class Memstore : public ITableLocker
{
    struct table_t 
    {
        Table *table;
        std::shared_mutex mutex;
    };

    std::shared_mutex m_tablesMutex;
    std::map<std::string, table_t> m_tables;

public:
    void createTable(const std::string& tableName, const Schema& schema) 
    {
        std::unique_lock<std::shared_mutex> lock(m_tablesMutex);
        m_tables[tableName].table = new Table(schema);
    }

    bool hasTable(const std::string& tableName)
    {
        std::shared_lock<std::shared_mutex> lock(m_tablesMutex);
        return m_tables.find(tableName) != m_tables.end();
    }

    const Schema& tableSchema(const std::string& tableName) 
    {
        std::shared_lock<std::shared_mutex> lock(m_tablesMutex);
        return m_tables[tableName].table->schema();
    }

    void insert(const std::string& tableName, std::vector<DataObject>&& row) 
    {
        std::shared_lock<std::shared_mutex> lockStorage(m_tablesMutex);
        std::unique_lock<std::shared_mutex> lockTable(m_tables[tableName].mutex);
        m_tables[tableName].table->insert(std::move(row));
    }

    void truncate(const std::string& tableName) 
    {
        std::shared_lock<std::shared_mutex> lockStorage(m_tablesMutex);
        std::unique_lock<std::shared_mutex> lockTable(m_tables[tableName].mutex);
        m_tables[tableName].table->truncate();
    }

    void lock_shared(const std::string& tableName)
    {
        m_tablesMutex.lock_shared();
        m_tables[tableName].mutex.lock_shared();
    }

    void unlock_shared(const std::string& tableName) override
    {
        m_tables[tableName].mutex.unlock_shared();
        m_tablesMutex.unlock_shared();
    }

    FullTableSelection* selectAll(const std::string& tableName) 
    {
        this->lock_shared(tableName);
        Table *tab = m_tables[tableName].table;
        return new FullTableSelection(this, tableName, tab->begin(), tab->end());
    }

    Selection* getInnerJoin(const std::string& table1,  
                            const std::string& table2, 
                            const std::string& column1, 
                            const std::string& column2);

    Selection* getFullOuterJoin(const std::string& table1,
                                const std::string& table2,
                                const std::string& column1,
                                const std::string& column2);
};


template<typename T>
std::vector<std::pair<Table::RowID, Table::RowID>> 
findEqualRowsOnColumn(Table* tab1, Table* tab2, 
                      std::size_t col1, std::size_t col2)
{
    std::vector<std::pair<Table::RowID, Table::RowID>> ids;
    for (auto row1 : *tab1) 
    {
        if (row1.isNull(col1)) {
            continue;
        }
        for (auto row2 : *tab2) 
        {
            if (row2.isNull(col2)) {
                continue;
            }
            if (row1.cast<T>(col1) == row2.cast<T>(col2)) {
                ids.emplace_back(row1.id(), row2.id());
            }
        }
    }
    return ids;
}


template<typename T>
std::vector<std::pair<Table::RowID, Table::RowID>>
findEqualRowsByIndex(Table* tab1, Table* tab2,
                     std::size_t indexed_col1, std::size_t indexed_col2)
{
    std::vector<std::pair<Table::RowID, Table::RowID>> rowPairs;

    auto index1 = tab1->index<T>(indexed_col1);
    auto index2 = tab2->index<T>(indexed_col2);

    for (const T& val : *index1) {
        auto iter = index2->find(val);
        if (iter != index2->end()) {
            for (Table::RowID id1 : index1->rows(val)) {
                for (Table::RowID id2 : index2->rows(val)) {
                    rowPairs.emplace_back(id1, id2);
                }
            }
        }
    }
    return rowPairs;
}


Selection* Memstore::getInnerJoin(const std::string& table1,  
                                  const std::string& table2, 
                                  const std::string& column1, 
                                  const std::string& column2)
{
    std::shared_lock<std::shared_mutex> lockStorage(m_tablesMutex);
    std::shared_lock<std::shared_mutex> lockTable1(m_tables[table1].mutex);
    std::shared_lock<std::shared_mutex> lockTable2(m_tables[table2].mutex);

    Table *tab1 = m_tables[table1].table;
    Table *tab2 = m_tables[table2].table;

    std::size_t col1 = tab1->schema().indexOf(column1);
    std::size_t col2 = tab2->schema().indexOf(column2);

    sql::DataType type = tab1->schema().typeOf(col1);

    std::vector<std::pair<Table::RowID, Table::RowID>> rowPairs;
    switch (type)
    {
    case sql::DataType::INTEGER:
        if (tab1->hasIndex(col1) && tab2->hasIndex(col2)) {
            rowPairs = findEqualRowsByIndex<long>(tab1, tab2, 
                                                  col1, col2);
        } 
        else {
            rowPairs = findEqualRowsOnColumn<long>(tab1, tab2, 
                                                   col1, col2);
        }
        break;

    case sql::DataType::TEXT:
        if (tab1->hasIndex(col1) && tab2->hasIndex(col2)) {
            rowPairs = findEqualRowsByIndex<std::string>(tab1, tab2, 
                                                         col1, col2);
        } 
        else {
            rowPairs = findEqualRowsOnColumn<std::string>(tab1, tab2, 
                                                          col1, col2);
        }
        break;
    }

    std::vector<Table::RowID> rows1;
    std::vector<Table::RowID> rows2;

    for (auto pair : rowPairs) {
        rows1.push_back(pair.first);
        rows2.push_back(pair.second);
    }

    Selection::Info selectionInfo;

    selectionInfo.tables = {tab1, tab2};
    selectionInfo.rows   = {rows1, rows2};

    for (const ColumnInfo& column : tab1->schema()) 
    {
        Selection::Info::Column selcol;
        selcol.name = table1 + "." + column.name();
        selcol.type = column.type();
        selcol.tableIndex = 0;
        selcol.tableColumnIndex = tab1->schema().indexOf(column.name());
        selectionInfo.columns.push_back(selcol);
    }

    for (const ColumnInfo& column : tab2->schema()) 
    {
        Selection::Info::Column selcol;
        selcol.name = table2 + "." + column.name();
        selcol.type = column.type();
        selcol.tableIndex = 1;
        selcol.tableColumnIndex = tab2->schema().indexOf(column.name());
        selectionInfo.columns.push_back(selcol);
    }

    return new Selection(selectionInfo);
}


template<typename T>
bool contains(Table* table, std::size_t columnIndex, const T& value)
{
    for (Table::Row row : *table) 
    {
        if (row.isNull(columnIndex)) {
            continue;
        }
        if (row.cast<T>(columnIndex) == value) {
            return true;
        }
    }
    return false;
}


template<typename T>
std::vector<std::pair<Table::RowID, Table::RowID>> 
findNonPairedRowsOnColumn(Table* tab1, Table* tab2, 
                          std::size_t col1, std::size_t col2)
{
    std::vector<std::pair<Table::RowID, Table::RowID>> ids;
    for (auto row1 : *tab1) 
    {
        if (row1.isNull(col1)) {
            continue;
        }
        T value = row1.cast<T>(col1);
        if (contains(tab2, col2, value)) {
            continue;
        }
        ids.emplace_back(row1.id(), std::size_t(-1));
    }

    // The same for the other table.
    for (auto row2 : *tab2) 
    {
        if (row2.isNull(col2)) {
            continue;
        }
        T value = row2.cast<T>(col2);
        if (contains(tab1, col1, value)) {
            continue;
        }
        ids.emplace_back(std::size_t(-1), row2.id());
    }
    return ids;
}


template<typename T>
std::vector<std::pair<Table::RowID, Table::RowID>>
findNonPairedRowsByIndex(Table* tab1, Table* tab2,
                         std::size_t col1, std::size_t col2)
{
    auto index1 = tab1->index<T>(col1);
    auto index2 = tab2->index<T>(col2);

    std::multimap<T, std::pair<Table::RowID, Table::RowID>> ids;

    for (const T& val1 : *index1) 
    {
        auto iter = index2->find(val1);
        if (iter == index2->end()) 
        {
            for (Table::RowID id1 : index1->rows(val1)) {
                ids.insert(std::make_pair(val1, std::make_pair(id1, std::size_t(-1))));
            }
        }
    }

    // The same for the other table.
    for (const T& val2 : *index2) 
    {
        auto iter = index1->find(val2);
        if (iter == index1->end()) 
        {
            for (Table::RowID id2 : index2->rows(val2)) {
                ids.insert(std::make_pair(val2, std::make_pair(std::size_t(-1), id2)));
            }
        }
    }

    std::vector<std::pair<Table::RowID, Table::RowID>> ret;
    ret.reserve(ids.size());
    for (auto pair : ids) {
        ret.push_back(pair.second);
    }
    return ret;
}


Selection* Memstore::getFullOuterJoin(const std::string& table1,  
                                      const std::string& table2, 
                                      const std::string& column1, 
                                      const std::string& column2)
{
    std::shared_lock<std::shared_mutex> lockStorage(m_tablesMutex);
    std::shared_lock<std::shared_mutex> lockTable1(m_tables[table1].mutex);
    std::shared_lock<std::shared_mutex> lockTable2(m_tables[table2].mutex);
    
    Table *tab1 = m_tables[table1].table;
    Table *tab2 = m_tables[table2].table;

    std::size_t col1 = tab1->schema().indexOf(column1);
    std::size_t col2 = tab2->schema().indexOf(column2);

    sql::DataType type = tab1->schema().typeOf(col1);

    std::vector<std::pair<Table::RowID, Table::RowID>> rowPairs;
    switch (type)
    {
    case sql::DataType::INTEGER:
        if (tab1->hasIndex(col1) && tab2->hasIndex(col2)) {
            rowPairs = findNonPairedRowsByIndex<long>(tab1, tab2, col1, col2);
        }
        else {
            rowPairs = findNonPairedRowsOnColumn<long>(tab1, tab2, col1, col2);
        }
        break;

    case sql::DataType::TEXT:
        if (tab1->hasIndex(col1) && tab2->hasIndex(col2)) {
            rowPairs = findNonPairedRowsByIndex<std::string>(tab1, tab2, 
                                                             col1, col2);
        }
        else {
            rowPairs = findNonPairedRowsOnColumn<std::string>(tab1, tab2,
                                                              col1, col2);
        }
        break;
    }

    std::vector<Table::RowID> rows1;
    std::vector<Table::RowID> rows2;

    for (auto pair : rowPairs) {
        rows1.push_back(pair.first);
        rows2.push_back(pair.second);
    }

    Selection::Info selectionInfo;

    selectionInfo.tables = {tab1, tab2};
    selectionInfo.rows   = {rows1, rows2};

    for (const ColumnInfo& column : tab1->schema()) 
    {
        Selection::Info::Column selcol;
        selcol.name = table1 + "." + column.name();
        selcol.type = column.type();
        selcol.tableIndex = 0;
        selcol.tableColumnIndex = tab1->schema().indexOf(column.name());
        selectionInfo.columns.push_back(selcol);
    }

    for (const ColumnInfo& column : tab2->schema()) 
    {
        Selection::Info::Column selcol;
        selcol.name = table2 + "." + column.name();
        selcol.type = column.type();
        selcol.tableIndex = 1;
        selcol.tableColumnIndex = tab2->schema().indexOf(column.name());
        selectionInfo.columns.push_back(selcol);
    }

    return new Selection(selectionInfo);
}

#endif // STORAGE_H
