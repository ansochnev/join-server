#ifndef MEMSTORE_H
#define MEMSTORE_H

#include <iostream>
#include "../db.h"
#include "../util/util.h"

namespace mem {
    sql::IDBConnection* open();
}

#endif // MEMSTORE_H