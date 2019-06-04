#include "memstore.h"

// DataObject represents a value of a type from [long, std::string].
// It can also be a NULL.
class DataObject
{
    union Value
    {
        long num;
        std::string str;
        
        Value() {}
        Value(const DataObject& d) {}
        Value(DataObject&& d) {}
        ~Value() {}

        void operator= (const Value& rhs) {}
        void operator= (Value&& rhs) {}
    };

    sql::DataType m_type;
    Value         m_value;
    bool          m_null;

public:
    DataObject() = delete; 

    explicit DataObject(sql::DataType t) 
        : m_type(t), m_null(true) {}
    
    explicit DataObject(long n) 
        : m_type(sql::DataType::INTEGER), m_null(false) { m_value.num = n; }

    explicit DataObject(const std::string& s) 
        : m_type(sql::DataType::TEXT), m_null(false) 
    { 
        new (&m_value.str) std::string(s); 
    }

    explicit DataObject(std::string&& s)
        : m_type(sql::DataType::TEXT), m_null(false) 
    { 
        new (&m_value.str) std::string(std::move(s));
    }

    DataObject(const DataObject& other) 
    {
        if (m_type == sql::DataType::TEXT)
            new (&m_value.str) std::string(other.m_value.str);
    }

    DataObject(DataObject&& other) 
    {
        if (m_type == sql::DataType::INTEGER)
            new (&m_value.str) std::string(std::move(m_value.str));
    }
        
    ~DataObject() 
    { 
        if (m_type == sql::DataType::TEXT) 
            m_value.str.~basic_string();
    }

    DataObject& operator= (const DataObject& rhs) 
    {
        if (m_type == sql::DataType::TEXT) {
            m_value.str = rhs.m_value.str;
        }
        return *this;
    }
    DataObject& operator= (DataObject&& rhs) 
    {
        if (m_type == sql::DataType::TEXT) {
            m_value.str = std::move(rhs.m_value.str);
        }
        return *this;
    }

    sql::DataType type() const noexcept { return m_type; }
    bool isNull() const noexcept        { return m_null; }
    
    long getLong() const                 { return m_value.num; }
    const std::string& getString() const { return m_value.str; }
};