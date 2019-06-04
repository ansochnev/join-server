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

    explicit DataObject(sql::DataType t);
    explicit DataObject(long n); 
    explicit DataObject(const std::string& s); 
    explicit DataObject(std::string&& s);

    DataObject(const DataObject& other);
    DataObject(DataObject&& other);
        
    ~DataObject();

    DataObject& operator= (const DataObject& rhs);
    DataObject& operator= (DataObject&& rhs); 

    sql::DataType type() const noexcept { return m_type; }
    bool isNull() const noexcept        { return m_null; }
    
    long getLong() const                 { return m_value.num; }
    const std::string& getString() const { return m_value.str; }
};