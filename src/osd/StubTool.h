#ifndef STUBTOOL_H
#define STUBTOOL_H

#include "common/debug.h"

#include "osd_types.h"
#include <string>
#include <string.h>

class StubTool {
 public:

  /* send object away */
  static bool in(const hobject_t& soid, const bufferlist& data, std::string * out_uri);

  /* get object back */
  static bool out(const std::string& uri, bufferlist * out_data);
};

#endif
