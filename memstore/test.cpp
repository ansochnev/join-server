#include <iostream>
#include "schema.h"
#include "table.h"


int main()
{
    Schema schema;
    schema.addColumn(ColumnInfo("id", sql::DataType::INTEGER, true));
    schema.addColumn(ColumnInfo("name", sql::DataType::TEXT, false));
/*
    DataObject d1(sql::DataType::TEXT);
    DataObject d2(28);
    DataObject d3("string");

    std::string s = "str";
    DataObject d4(s);

    DataObject d5(d2);
    DataObject d6(DataObject("hello"));

    std::cout << d6.getString() << std::endl;

    DataObject d7(sql::DataType::TEXT);
*/
    Table table(schema);

    std::vector<DataObject> values;
    values.push_back(DataObject(28));
    values.push_back(DataObject("hello"));
    table.insert(std::move(values));

    std::vector<DataObject> values2;
    values2.push_back(DataObject(36));
    values2.push_back(DataObject("world"));
    table.insert(std::move(values2));

    for (auto row : table) {
        std::cout << row.getLong(0) << " " << row.getString(1) << std::endl;
    }
}