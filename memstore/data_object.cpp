#include "data_object.h"

DataObject::DataObject(sql::DataType t) 
    : m_type(t), m_null(true) {}


DataObject::DataObject(long n) 
    : m_type(sql::DataType::INTEGER), m_null(false) 
{ 
    m_value.num = n; 
}


DataObject::DataObject(const std::string& s) 
    : m_type(sql::DataType::TEXT), m_null(false) 
{ 
    new (&m_value.str) std::string(s); 
}


DataObject::DataObject(std::string&& s)
    : m_type(sql::DataType::TEXT), m_null(false) 
{ 
    new (&m_value.str) std::string(std::move(s));
}


DataObject::DataObject(const DataObject& other) 
    : m_type(other.m_type), m_null(other.m_null)
{
    if (m_null) return;
    
    if (m_type == sql::DataType::TEXT) {
        new (&m_value.str) std::string(other.m_value.str);
    }
    else {
        m_value.num = other.m_value.num;
    }
}


DataObject::DataObject(DataObject&& other) 
    : m_type(other.m_type), m_null(other.m_null) 
{
    if (m_null) return;

    if (m_type == sql::DataType::TEXT) {
        new (&m_value.str) std::string(std::move(other.m_value.str));
    }
    else {
        m_value.num = other.m_value.num;
    }
    other.m_null = true;
}


DataObject::~DataObject() 
{ 
    if (!m_null && m_type == sql::DataType::TEXT) 
        m_value.str.~basic_string();
}


// FIXME
DataObject& DataObject::operator= (const DataObject& rhs) 
{
    if (m_type != rhs.m_type) {
        throw sql::Exception("DataObject: type mismatch");
    }
    
    if (!m_null) 
    {
        switch (m_type)
        {
        case sql::DataType::INTEGER:
            m_value.num = rhs.m_value.num;
            break;
        case sql::DataType::TEXT:
            m_value.str = rhs.m_value.str;
        }
        m_value = rhs.m_value;
        
        return *this;
    }
    
    if (m_type == sql::DataType::TEXT) {
        new (&m_value.str) std::string(rhs.m_value.str);
    }
    else {
        m_value.num = rhs.m_value.num;
    }
    
    return *this;
}


// FIXME
DataObject& DataObject::operator= (DataObject&& rhs) 
{
    if (m_type != rhs.m_type)
        throw sql::Exception("DataObject: type mismatch");
    
    if (!m_null) 
    {
        switch (m_type)
        {
        case sql::DataType::INTEGER:
            m_value.num = rhs.m_value.num;
            break;
        case sql::DataType::TEXT:
            m_value.str = std::move(rhs.m_value.str);
        }
        m_value = rhs.m_value;
        
        return *this;
    }
    
    if (m_type == sql::DataType::TEXT) {
        new (&m_value.str) std::string(std::move(rhs.m_value.str));
    }
    else {
        m_value.num = rhs.m_value.num;
    }
    
    return *this;
}