#include <vector>
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
    explicit Table(const Schema& s);
    explicit Table(Schema&& s);

    const Schema& schema() const noexcept { return m_schema; }

    using RowID = std::size_t;
    RowID insert(std::vector<DataObject>&& values);
    void  remove(RowID row);

    Row operator[] (RowID r) { return Row(this, r); }

    using iterator = Iterator;
    iterator begin() { return !m_rows.empty() ? Iterator(this, 0) : end(); }
    iterator end()   { return Iterator(this, -1); }

private:
    class Cell
    {
        struct AbstractHolder {
            virtual ~AbstractHolder() {}
        };

        template<typename T>
        struct Holder : public AbstractHolder {
            T value;
            Holder(T&& t) : value(std::forward<T>(t)) {}
        };

        template<typename T>
        T& cast(AbstractHolder& h) {
            return static_cast<Holder<T>>(h).value;
        }

        AbstractHolder *m_holder;

    public:
        Cell() : m_holder(nullptr) {}

        explicit Cell(long n) 
            : m_holder(new Holder<long>(std::move(n))) {}
        
        explicit Cell(std::string&& s)
            : m_holder(new Holder<std::string>(std::move(s))) {}
        
        explicit Cell(const DataObject& d);

        Cell(const Cell&) = delete;
        Cell(Cell&& other) 
            : m_holder(other.m_holder) { other.m_holder = nullptr; }

        ~Cell() { if (m_holder) delete m_holder; }

        Cell& operator= (const Cell&) = delete;
        Cell& operator= (Cell&& c);

        Cell& operator= (long n);
        Cell& operator= (const std::string& s);
        Cell& operator= (std::string&& s);
        Cell& operator= (const DataObject& d);
        
        bool isNull() const;
        long getLong() const;
        const std::string& getString() const;
    };


    class Row
    {
        Table* m_table;
        RowID  m_row;
    public:
        Row(Table* table, RowID row) : m_table(table), m_row(row) {}

        bool isNull(int column) const { 
            return m_table->m_rows[m_row][column].isNull(); 
        }

        long getLong(int column) const {
            return m_table->m_rows[m_row][column].getLong();
        }

        const std::string& getString(int column) {
            return m_table->m_rows[m_row][column].getString();
        }

        void update(int column, long value);
        void update(int column, const std::string& value);
        void update(int column, std::string&& value);
    };
    

    class Iterator
    {
        Table* m_table;
        RowID  m_row;
    public:
        Iterator(Table* table, RowID row) : m_table(table), m_row(row) {}

        Iterator(const Iterator&) = default;
        Iterator(Iterator&&)      = default;
        
        ~Iterator() = default;

        Iterator& operator= (const Iterator&) = default;
        Iterator& operator= (Iterator&&)      = default;

        bool operator!= (const Iterator& rhs) const { 
            return m_table == rhs.m_table && 
                   m_row   == rhs.m_row; 
        }

        Iterator& operator++ () { return (++m_row, *this);    }
        Row       operator*  () { return Row(m_table, m_row); }
    };
};