#pragma once

#include "sqlite_orm.h"
#include "users_definitions.h"

namespace rgw::sal::simplefile::sqlite {

// Functions that convert DB type to RGW type (and vice-versa)
DBOPUserInfo getRGWUser(const DBUser& user);
DBUser getDBUser(const DBOPUserInfo& user);

}  // namespace rgw::sal::simplefile::sqlite
