#include <vector>
#include "schema.h"
#include "data_object.h"

class Table
{
    class Cell;
    class Row;
    class Iterator;

    Schema                         m_schema;
    std::vector<std::vector<Cell>> m_rows;

public:
    explicit Table(const Schema& s);
    explicit Table(Schema&& s);

    ~Table() = default;

    const Schema& schema() const noexcept { return m_schema; }

    using RowID = std::size_t;
    RowID insert(std::vector<DataObject>&& values);
    void  remove(RowID row);

    Row operator[] (RowID r) { return Row(this, r); }

    using iterator = Iterator;
    iterator begin() { return !m_rows.empty() ? Iterator(this, 0) : end(); }
    iterator end()   { return Iterator(this, -1); }

private:
    bool isSatisfySchema(const std::vector<DataObject>& row);
    bool isUnique(const std::vector<DataObject>& row);

private:
    class Cell
    {
        struct AbstractHolder {
            virtual ~AbstractHolder() {}
        };

        template<typename T>
        struct Holder : public AbstractHolder 
        {
            T value;
            explicit Holder(const T& t) : value(t) {}
            explicit Holder(T&& t)      : value(std::move(t)) {}
        };

        template<typename T>
        T& cast() const {
            return static_cast<Holder<T>*>(m_holder)->value;
        }

        AbstractHolder *m_holder;

    public:
        Cell() : m_holder(nullptr) {}
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
        
        bool isNull() const                  { return m_holder == nullptr; }
        long getLong() const                 { return cast<long>();        }
        const std::string& getString() const { return cast<std::string>(); }
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

        void update(int column, const DataObject& d);

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
            return m_table != rhs.m_table || 
                   m_row   != rhs.m_row; 
        }

        Iterator& operator++ () 
        {
            if (++m_row == m_table->m_rows.size()) {
                m_row = -1;
            }
            return *this;
        }
        
        Row       operator*  () { return Row(m_table, m_row); }
    };
};