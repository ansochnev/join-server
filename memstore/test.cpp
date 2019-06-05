#include <iostream>
#include "table.h"
#include "db.h"

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
/*
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

    for (auto& rec : table.select({0, 1})) {
        std::cout << rec[0].getLong() << " " << rec[1].getString() << std::endl;
    }
*/
    Memstore mem;
    mem.createTable("A", schema);

    std::vector<DataObject> values;
    values.push_back(DataObject(28));
    values.push_back(DataObject("hello"));
    mem.insert("A", std::move(values));

    std::vector<DataObject> values2;
    values2.push_back(DataObject(36));
    values2.push_back(DataObject("world"));
    mem.insert("A", std::move(values2));

    auto selection = mem.selectAll("A");

    while(true) 
    {
        if (selection->end()) break;

        std::cout << fmt::sprintf("%v,%v", 
            
            selection->isNull(0) ? "" 
                : fmt::sprintf("%v", selection->getLong(0)),

            selection->isNull(1) ? "" 
                : fmt::sprintf("%v", selection->getString(1))
        ) << std::endl;

        selection->next();
    }

    delete selection;
}