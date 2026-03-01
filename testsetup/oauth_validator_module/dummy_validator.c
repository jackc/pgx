#include "postgres.h"
#include "fmgr.h"
#include "libpq/oauth.h"

PG_MODULE_MAGIC;

bool validate(const ValidatorModuleState *state, const char *token,
              const char *role, ValidatorModuleResult *result) {

  elog(LOG, "accept token '%s' for role '%s'", token, role);
  char *authn_id = pstrdup(token);
  result->authn_id = authn_id;
  result->authorized = true;
  return true;
}

const OAuthValidatorCallbacks callbacks = {
    .magic = PG_OAUTH_VALIDATOR_MAGIC,
    .startup_cb = NULL,
    .shutdown_cb = NULL,
    .validate_cb = validate,
};

const OAuthValidatorCallbacks *_PG_oauth_validator_module_init() {
  return &callbacks;
}
