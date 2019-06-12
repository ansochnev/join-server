#ifndef TABLE_H
#define TABLE_H

#include <vector>
#include <map>
#include <set>
#include "data_object.h"

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

    bool contains(const std::string& columnName) const;
    std::size_t indexOf(const std::string& columnName) const;
    std::size_t primaryKeyIndex() const;

    sql::DataType typeOf(std::size_t columnIndex) const;
    sql::DataType typeOf(const std::string& columnName) const;

    const ColumnInfo& operator[] (int index) const { return m_columns[index]; }
    const ColumnInfo& operator[] (const std::string& name) const;

    std::size_t size() const { return m_columns.size(); }

    using const_iterator = std::vector<ColumnInfo>::const_iterator;
    const_iterator begin() const noexcept { return m_columns.cbegin(); }
    const_iterator end()   const noexcept { return m_columns.cend();   }
};


class Table
{
private:
    class Cell;
    class Row;
    class AbstractIndex;
public:
    class Iterator;
    template<typename T> class Index;
    
private:
    Schema                         m_schema;
    std::vector<AbstractIndex*>    m_indices;
    std::vector<std::vector<Cell>> m_rows;

public:
    explicit Table(const Schema& s);
    explicit Table(Schema&& s);

    ~Table();

    const Schema& schema() const noexcept { return m_schema; }

    bool hasIndex(std::size_t col) const { 
        return m_schema.primaryKeyIndex() == col; 
    }

    template<typename T>
    const Index<T>* index(std::size_t col) const { 
        return static_cast<Index<T>*>(m_indices[col]); 
    }

    using RowID = std::size_t;

    RowID insert(Record&& values);
    void  remove(RowID row);
    void  truncate(); 

    Row operator[] (RowID r) { return Row(this, r); }
    std::vector<Record> select(const std::vector<RowID>& rows) const;

    using iterator = Iterator;
    iterator begin() { return !m_rows.empty() ? Iterator(this, 0) : end(); }
    iterator end()   { return Iterator(this, -1); }

private:
    bool isSatisfySchema(const std::vector<DataObject>& row) const;
    bool isUnique(const std::vector<DataObject>& row) const;
    Record makeRecord(RowID row) const;

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
        
        template<typename T>
        T& cast() const {
            return static_cast<Holder<T>*>(m_holder)->value;
        }

        bool isNull() const                  { return m_holder == nullptr; }
        long getLong() const                 { return cast<long>();        }
        const std::string& getString() const { return cast<std::string>(); }
        DataObject toDataObject(sql::DataType) const;
    };


    class Row
    {
        Table* m_table;
        RowID  m_row;
    public:
        Row(Table* table, RowID row) : m_table(table), m_row(row) {}

        RowID id() const { return m_row; }

        bool isNull(int column) const { 
            return m_table->m_rows[m_row][column].isNull(); 
        }

        template<typename T>
        T cast(int column) const {
            return m_table->m_rows[m_row][column].cast<T>();
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
    

    class AbstractIndex 
    {
    public:
        virtual void clear() = 0; 
        virtual ~AbstractIndex() {}
    };


public:
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

        bool operator== (const Iterator& rhs) const { 
            return m_table == rhs.m_table && 
                   m_row   == rhs.m_row; 
        }
                
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
        
        Row operator*  () { return Row(m_table, m_row); }
    };


    template<typename T>
    class Index : public AbstractIndex
    {
        using Container = std::map<T, std::set<RowID>>;
        Container m_map;

        class ConstIterator
        {
            typename Container::const_iterator m_iter;
        public:
            ConstIterator(const typename Container::const_iterator& iter) 
                : m_iter(iter) {}

            bool operator!= (const ConstIterator& rhs) const {
                return m_iter != rhs.m_iter;
            }

            bool operator== (const ConstIterator& rhs) const {
                return m_iter == rhs.m_iter;
            }

            ConstIterator& operator++ () { return (++m_iter, *this); }
            const T& operator* () const  { return m_iter->first;     }
        };

    public:
        Index() = default;

        void insert(const T& val, RowID row) { m_map[val].insert(row); }
        void remove(RowID row);
        void clear() override { m_map.clear(); }

        std::vector<RowID> rows(const T& val) const
        {
            std::vector<RowID> ret;
            auto found = m_map.find(val);
            ret.reserve(found->second.size());
            for (RowID row : found->second) {
                ret.push_back(row);
            }
            return ret;
        }

        using const_iterator = ConstIterator;
        const_iterator begin() const { return ConstIterator(m_map.begin()); }
        const_iterator end() const   { return ConstIterator(m_map.end());   }

        const_iterator find(const T& val) const {
            return ConstIterator(m_map.find(val));
        }
    };
};

#endif // TABLE_H