#include <string>
#include "memstore.h"
#include "db.h"

void assertEq(const std::string& have, const std::string& expect);
Schema parseSchema(const std::string& s);
std::vector<DataObject> parseValues(const std::string& s, const Schema& schema);