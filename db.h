#ifndef SQL_DB_CONN_H
#define SQL_DB_CONN_H

#include <string>
#include <exception>

namespace sql
{
enum class DataType;
class IDBConnection;
class IStatement;
class ISelection;
class Exception;
}

namespace sql
{
class Exception : public std::exception
{
    std::string m_message;
public:
    Exception(std::string&& msg) : m_message(msg) {}
    const char* what() const noexcept override { return m_message.c_str(); }
};


enum class DataType 
{
    TEXT,
    INTEGER,
};


class IDBConnection
{
public:
    virtual IStatement* createStatement() = 0;
    virtual void close() = 0;
    virtual ~IDBConnection() {}
};


class IStatement
{
public:
    virtual void modify(const std::string& query) = 0;
    virtual ISelection* select(const std::string& query) = 0;
    virtual void close() = 0;
    virtual ~IStatement() {}
};


class ISelection
{
public:
    virtual void next() = 0;
    virtual bool end()  = 0;
    
    virtual bool isNull(std::size_t columnIndex) = 0;
    virtual long getLong(std::size_t columIndex) = 0;
    virtual std::string getString(std::size_t columnIndex) = 0;

    virtual void close() = 0;
    virtual ~ISelection() {}
};

} // namespace sql

#endif // SQL_DB_CONN_H