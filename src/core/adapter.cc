#include "adapter.h"

#include <dlfcn.h>

#include "logger.h"

namespace opentrade {

Adapter::~Adapter() {}

Adapter* Adapter::Load(const std::string& sofile) {
  LOG_INFO("Trying to load " << sofile);
  auto handle = dlopen(sofile.c_str(), RTLD_NOW);
  if (!handle) {
    LOG_FATAL(dlerror());
    return nullptr;
  }

  auto create_func = (Func)dlsym(handle, "create");
  if (!create_func) {
    LOG_FATAL(dlerror());
    return nullptr;
  }

  auto out = (*create_func)();
  if (out) out->create_func_ = create_func;
  return out;
}

}  // namespace opentrade
