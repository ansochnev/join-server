#include <vector>
#include <variant>
#include "schema.h"

class DataObject
{
    sql::DataType  m_type;
    void          *m_value;

public:
    explicit DataObject(sql::DataType t);
    explicit DataObject(long val);
    explicit DataObject(const std::string& val); 
    explicit DataObject(std::string&& val);

    DataObject(const DataObject& other);
    DataObject(DataObject&& other);
        
    ~DataObject();

    DataObject& operator= (const DataObject& rhs);
    DataObject& operator= (DataObject&& rhs);

    sql::DataType type() const noexcept {
        return m_type;
    }
    
    bool isNull() const noexcept { 
        return m_value == nullptr;
    }
    
    long getLong() const;
    std::string getString() const;
};


class Table
{
    struct Cell;
    class Row;
    class Iterator;

    Schema                        m_schema;
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
        std::variant<long, std::string> value;
    public:
        Cell& operator= (long n);
        Cell& operator= (const std::string& s);
        Cell& operator= (std::string& s);

        Cell& operator= (const DataObject& d);
        
        long getLong() const { return std::get<long>(value); }
        const std::string& getString() const { return std::get<std::string>(value); }
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