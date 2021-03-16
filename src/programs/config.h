#ifndef __clusterd_config_H__
#define __clusterd_config_H__

#include <sys/types.h>

#define CLUSTERD_CONFIG_NS_UID_RANGE_KEY "ns_uid_range"
#define CLUSTERD_CONFIG_GROUP_NAME       "group"

typedef int(*config_func_t)(const char *, const char*);

/**
 * Read the system config in CLUSTERD_CONFIG_DIR. This specifies
 * administrator-level configuration. The file must be owned by and
 * writable only by root.
 *
 * The argument is a config_func_t that figures out what to do with
 * each line
 */
int clusterd_read_system_config(config_func_t func);

/**
 * Parse a uid range line (for ns_uid_range and sub_uid_range)
 */
int clusterd_parse_uid_range(const char *range, uid_t *lower, unsigned int *count);

/**
 * Parse a group by name or gid
 */
int clusterd_parse_group(const char *group, gid_t *gid);

#endif
