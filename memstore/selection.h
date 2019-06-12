#ifndef SELECTION_H
#define SELECTION_H

#include "memstore.h"
#include "table.h"

class FullTableSelection : public sql::ISelection
{
    Table::iterator m_currentRow;
    Table::iterator m_end;
public:
    FullTableSelection(const Table::iterator& begin, 
                       const Table::iterator& end)
        : m_currentRow(begin), m_end(end) {}

    void next() override { ++m_currentRow; }
    bool end()  override { return m_currentRow == m_end; }
    
    bool isNull(std::size_t columnIndex) override {
        return (*m_currentRow).isNull(columnIndex);
    }

    long getLong(std::size_t columnIndex) override {
        return (*m_currentRow).getLong(columnIndex);
    }

    std::string getString(std::size_t columnIndex) override {
        return (*m_currentRow).getString(columnIndex);
    }

    void close() override {}
};



class Selection : public sql::ISelection
{
public:
    struct Info;

private:
    std::vector<Record> m_records;
    std::size_t         m_currentRecordIndex;

public:
    Selection(const Info& selectionInfo);

    void next() override;

    bool end()  override { 
        return m_currentRecordIndex == std::size_t(-1); 
    }
    
    bool isNull(std::size_t columnIndex) override {
        return m_records[m_currentRecordIndex][columnIndex].isNull();
    }

    long getLong(std::size_t columnIndex) override {
        return m_records[m_currentRecordIndex][columnIndex].getLong();
    }

    std::string getString(std::size_t columnIndex) override {
        return m_records[m_currentRecordIndex][columnIndex].getString();
    }

    void close() override {}

public:
    struct Info 
    {
        struct Column 
        {
            std::string    name;
            sql::DataType  type;
            std::size_t    tableIndex;
            std::size_t    tableColumnIndex;
        };

        std::vector<Column> columns;
        std::vector<Table*> tables;
        std::vector<std::vector<Table::RowID>> rows;
    };
};


Selection::Selection(const Info& selectionInfo) : m_currentRecordIndex(-1)
{
    std::vector<std::vector<Record>> tableRecordMatrix;

    for (std::size_t i = 0; i < selectionInfo.tables.size(); ++i) 
    {
        Table *table = selectionInfo.tables[i];
        const std::vector<Table::RowID> &rowIds = selectionInfo.rows[i];
        std::vector<Record> tableRecords = table->select(rowIds);
        tableRecordMatrix.push_back(std::move(tableRecords));
    }

    std::size_t rowCount = tableRecordMatrix[0].size();
    m_records.resize(rowCount);
    std::size_t table, col;
    for (std::size_t row = 0; row < rowCount; ++row) 
    {
        m_records[row].reserve(selectionInfo.columns.size());
        for (const Info::Column& column : selectionInfo.columns) 
        {
            table = column.tableIndex;
            col   = column.tableColumnIndex;
            m_records[row].push_back(tableRecordMatrix[table][row][col]);
        }
    }

    if (!m_records.empty()) {
        m_currentRecordIndex = 0;
    }
}


void Selection::next()
{
    if (++m_currentRecordIndex == m_records.size()) {
        m_currentRecordIndex = -1;
    }
}

#endif // SELECTION_H