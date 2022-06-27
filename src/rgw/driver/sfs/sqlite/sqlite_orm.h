#pragma once

// TODO investigate which optimisations are causing gcc to get killed
// Disable optimisations for sqlite_orm for now
#pragma GCC push_options
#pragma GCC optimize ("O0")

#include <sqlite_orm/sqlite_orm.h>

#pragma GCC pop_options
