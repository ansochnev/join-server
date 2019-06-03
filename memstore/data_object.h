#include "memstore.h"

// DataObject represents a value of a type from [long, std::string].
// It can also be a NULL.
class DataObject
{

public:
    DataObject() = delete; 

    explicit DataObject(sql::DataType t);
    explicit DataObject(long val);
    explicit DataObject(const std::string& val); 
    explicit DataObject(std::string&& val);

    DataObject(const DataObject& other) = default;
    DataObject(DataObject&& other) = default;
        
    ~DataObject() = default;

    DataObject& operator= (const DataObject& rhs) = default;
    DataObject& operator= (DataObject&& rhs) = default;

    sql::DataType type() const noexcept;
    bool isNull() const noexcept;
    
    long getLong() const;
    const std::string& getString() const;
};