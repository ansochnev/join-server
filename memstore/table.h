#include <vector>
#include <variant>
#include "schema.h"
#include "data_object.h"

class Table
{
    struct Cell;
    class Row;
    class Iterator;

    Schema                         m_schema;
    std::vector<std::vector<Cell>> m_rows;

public:
    Table(const Schema& s);
    Table(Schema&& s);

    const Schema& schema() const noexcept { return m_schema; }

    using RowID = std::size_t;
    RowID insert(std::vector<DataObject>&& values);
    void  remove(RowID row);

    Row operator[] (RowID r);

    using iterator = Iterator;
    iterator begin();
    iterator end();

private:
    class Cell
    {
    public:
        Cell& operator= (long n);
        Cell& operator= (const std::string& s);
        Cell& operator= (std::string& s);

        Cell& operator= (const DataObject& d);
        
        long getLong() const;
        const std::string& getString() const;
    };


    class Row
    {
        Table* m_table;
        RowID  m_row;
    public:
        bool isNull(int column);
        long getLong(int column);
        std::string getString(int column);

        void update(int column, long value);
        void update(int column, const std::string& value);
        void update(int column, std::string&& value);
    };
    

    class Iterator
    {
    public:
        bool operator!= (const Iterator& rhs);
        Iterator& operator++ ();
        Row  operator* ();
    };
};