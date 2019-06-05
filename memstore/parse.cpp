#include "parse.h"

void assertEq(const std::string& have, const std::string& expect) 
{
    if (have != expect) {
        throw sql::Exception("unexpected token " + have);
    }
}


sql::DataType parseType(const std::string& s) 
{
    if (toUpper(s) == "INTEGER") {
        return sql::DataType::INTEGER;
    } 
    else if (toUpper(s) == "TEXT") {
        return sql::DataType::TEXT;
    }
    else {
        throw sql::Exception("cannot parse type: " + s);
    }
}


ColumnInfo parseColumn(const std::string& s)
{
    std::istringstream ss(s);

    std::string name;
    ss >> name;

    std::string tmp;
    ss >> tmp;
    sql::DataType type = parseType(tmp);

    bool primaryKey = false;
    if (ss >> tmp && toUpper(tmp) == "PRIMARY" && 
        ss >> tmp && toUpper(tmp) == "KEY")
    {
        primaryKey = true;
    }

    return ColumnInfo(name, type, primaryKey);
}


Schema parseSchema(const std::string& s)
{
    std::string trimmed = trim(s, " \t\r\n();");
    auto tokens = split(trimmed, ',');

    Schema schema;
    for (std::string token : tokens) {
        schema.addColumn(parseColumn(token));
    }

    return schema;
}


DataObject parseValue(const std::string& s, const ColumnInfo& column)
{
    if (column.type() == sql::DataType::INTEGER) {
        try {
            return DataObject(std::stol(s));
        }
        catch(std::exception& e) {
            throw sql::Exception(
                fmt::sprintf("cannot parse '%v' as INTEGER: %v", s, e.what()));
        }
    }
    else if (column.type() == sql::DataType::TEXT) {
        return DataObject(trim(s, "\""));
    }
    else {
        throw sql::Exception("parseValue: unreachable code");
    }
}

std::vector<DataObject> parseValues(const std::string& s, const Schema& schema)
{
    std::string trimmed = trim(s, " \t\r\n();");
    auto tokens = split(trimmed, ' ');

    if (schema.size() != tokens.size()) 
    {
        std::ostringstream os;
        for (auto token : tokens) {
            os << token << " ";
        }
        std::string values = os.str();
        values.erase(values.size() - 1);
        throw sql::Exception(fmt::sprintf(
            "list of values '%v' does not match table schema", values));
    }

    std::vector<DataObject> ret;
    ret.reserve(tokens.size());
    
    for (std::size_t i = 0; i < tokens.size(); ++i) {
        ret.emplace_back(parseValue(tokens[i], schema[i]));
    }
    
    return ret;
}