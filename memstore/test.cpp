#include <iostream>
#include <variant>
#include <any>
#include <memory>
#include "schema.h"
#include "table.h"

class Any
{
    void* m_data;
    sql::DataType type;

public:
};


union U
{
    std::string s;
};


int main()
{
    Schema schema;
    schema.addColumn(ColumnInfo("id", sql::DataType::INTEGER, true));
    schema.addColumn(ColumnInfo("name", sql::DataType::TEXT, false));

    std::cout << sizeof(std::variant<int>) << std::endl;
    std::cout << sizeof(std::variant<long>) << std::endl;
    std::cout << sizeof(std::variant<std::string>) << std::endl;
    std::cout << sizeof(std::variant<std::string, double>) << std::endl;
    std::cout << sizeof(std::any) << std::endl;
    std::cout << sizeof(Any) << std::endl;
    std::cout << sizeof(sql::DataType) << std::endl;
    std::cout << sizeof(std::unique_ptr<void>) << std::endl;
    std::cout << sizeof(std::string) << std::endl;
}