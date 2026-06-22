/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "metadb/inode_table.h"

const char *InodeTableName = "falcon_inode_table";

void ConstructCreateInodeTableCommand(StringInfo command, const char *name)
{
    appendStringInfo(command,
                     "CREATE TABLE falcon.%s(name			   varchar(256),"
                     "st_ino			   bigint,"
                     "parentid_partid   bigint,"
                     "st_dev			   bigint,"
                     "st_mode		   int,"
                     "st_nlink		   bigint,"
                     "st_uid			   int,"
                     "st_gid			   int,"
                     "st_rdev		   bigint,"
                     "st_size		   bigint,"
                     "st_blksize		   bigint,"
                     "st_blocks		   bigint,"
                     "st_atim		   timestamptz,"
                     "st_mtim		   timestamptz,"
                     "st_ctim		   timestamptz,"
                     "etag			   text,"
                     "update_version	   bigint,"
                     "primary_nodeid	   int,"
                     "backup_nodeid	   int,"
                     "lease_count	   bigint,"
                     "lease_expire_at  timestamptz);"
                     "CREATE UNIQUE INDEX %s_index ON falcon.%s USING btree(parentid_partid, name);"
                     "ALTER TABLE falcon.%s SET SCHEMA pg_catalog;"
                     "GRANT SELECT ON pg_catalog.%s TO public;"
                     "ALTER EXTENSION falcon ADD TABLE %s;",
                     name,
                     name,
                     name,
                     name,
                     name,
                     name);
}
