#include "metadb/meta_handle_helper.h"

#include "string.h"
#include "sys/stat.h"

#include "access/genam.h"
#include "access/table.h"
#include "access/xact.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"

#include "perf_counter/falcon_per_request_stat.h"
#include "utils/error_log.h"
#include "utils/utils.h"

uint16_t HashPartId(const char *fileName)
{
    uint16_t hashValue = 0;
    for (int i = 0; i < strnlen(fileName, FILENAMELENGTH); ++i) {
        hashValue = hashValue * 31 + fileName[i];
    }
    return hashValue & PART_ID_MASK;
}

uint64_t CombineParentIdWithPartId(uint64_t parent_id, uint16_t part_id)
{
    return (parent_id << PART_ID_BIT_COUNT) | part_id;
}

Oid GetRelationOidByName_FALCON(const char *relationName)
{
    Oid res = InvalidOid;
    res = get_relname_relid(relationName, PG_CATALOG_NAMESPACE);
    if (res == InvalidOid) {
        FALCON_ELOG_ERROR_EXTENDED(PROGRAM_ERROR, "cannot find relation %s.", relationName);
    }
    return res;
}

bool SearchAndUpdateInodeTableInfo(const char *workerInodeRelationName,
                                   Relation workerInodeRelation,
                                   const char *workerInodeRelationIndexName,
                                   Oid workerInodeIndexOid,
                                   const uint64_t parentId_partId,
                                   const char *fileName,
                                   const bool doUpdate,
                                   uint64_t *inodeId,
                                   int64_t *size,
                                   const int64_t *newSize,
                                   uint64_t *updateVersion,
                                   uint64_t *nlink,
                                   const int nlinkChangeNum,
                                   mode_t *mode,
                                   const mode_t *newExecMode,
                                   int modeCheckType,
                                   uint32_t *newUid,
                                   uint32_t *newGid,
                                   const char *newEtag,
                                   TimestampTz *newAtime,
                                   TimestampTz *newMtime,
                                   int32_t *primaryNodeId,
                                   int32_t *newPrimaryNodeId,
                                   int32_t *backupNodeId,
                                   const uint64_t *expectedInodeId,
                                   const InodeSearchStatContext *statContext)
{
    ScanKeyData scanKey[2];
    int scanKeyCount = 2;
    SysScanDesc scanDescriptor = NULL;
    HeapTuple heapTuple;
    TupleDesc tupleDesc;
    Relation workerInodeRel = workerInodeRelation;

    SetUpScanCaches();

    // set scan arguments
    scanKey[0] = InodeTableScanKey[INODE_TABLE_PARENT_ID_PART_ID_EQ];
    scanKey[0].sk_argument = UInt64GetDatum(parentId_partId);
    scanKey[1] = InodeTableScanKey[INODE_TABLE_NAME_EQ];
    scanKey[1].sk_argument = CStringGetTextDatum(fileName);

    bool needCatalogTupleUpdate = false;
    if (!workerInodeRelation) {
        workerInodeRel = table_open(GetRelationOidByName_FALCON(workerInodeRelationName),
                                    doUpdate ? RowExclusiveLock : AccessShareLock);
    }

    if (workerInodeIndexOid == InvalidOid)
        workerInodeIndexOid = GetRelationOidByName_FALCON(workerInodeRelationIndexName);
    if (statContext) {
        STAT_CKPT(statContext->statArrayIndex, statContext->requestStartCheckpoint);
    }
    scanDescriptor =
        systable_beginscan(workerInodeRel, workerInodeIndexOid, true, GetTransactionSnapshot(), scanKeyCount, scanKey);
    heapTuple = systable_getnext(scanDescriptor);
    tupleDesc = RelationGetDescr(workerInodeRel);

    if (!HeapTupleIsValid(heapTuple)) {
        if (statContext) {
            STAT_CKPT(statContext->statArrayIndex, statContext->opDoneCheckpoint);
        }
        systable_endscan(scanDescriptor);
        if (!workerInodeRelation) {
            table_close(workerInodeRel, doUpdate ? RowExclusiveLock : AccessShareLock);
        }
        return false;
    }

    Datum updateDatumArray[Natts_pg_dfs_inode_table];
    bool isNullArray[Natts_pg_dfs_inode_table];
    bool doUpdateArray[Natts_pg_dfs_inode_table];
    memset(doUpdateArray, false, sizeof(doUpdateArray));
    bool isNull;
    if (inodeId) {
        *inodeId = DatumGetUInt64(heap_getattr(heapTuple, Anum_pg_dfs_file_st_ino, tupleDesc, &isNull));
    }
    if (size) {
        *size = DatumGetInt64(heap_getattr(heapTuple, Anum_pg_dfs_file_st_size, tupleDesc, &isNull));
    }
    if (expectedInodeId != NULL) {
        uint64_t currentInodeId = inodeId != NULL
                                      ? *inodeId
                                      : DatumGetUInt64(heap_getattr(heapTuple, Anum_pg_dfs_file_st_ino, tupleDesc, &isNull));
        if (currentInodeId != *expectedInodeId) {
            if (statContext) {
                STAT_CKPT(statContext->statArrayIndex, statContext->opDoneCheckpoint);
            }
            systable_endscan(scanDescriptor);
            if (!workerInodeRelation) {
                table_close(workerInodeRel, doUpdate ? RowExclusiveLock : AccessShareLock);
            }
            return true;
        }
    }
    if (doUpdate && newSize) {
        updateDatumArray[Anum_pg_dfs_file_st_size - 1] = UInt64GetDatum(*newSize);
        isNullArray[Anum_pg_dfs_file_st_size - 1] = false;
        doUpdateArray[Anum_pg_dfs_file_st_size - 1] = true;
        needCatalogTupleUpdate = true;
    }
    if (updateVersion) {
        *updateVersion = DatumGetUInt64(heap_getattr(heapTuple, Anum_pg_dfs_file_update_version, tupleDesc, &isNull));
        if (doUpdate) {
            updateDatumArray[Anum_pg_dfs_file_update_version - 1] = UInt64GetDatum((*updateVersion) + 1);
            isNullArray[Anum_pg_dfs_file_update_version - 1] = false;
            doUpdateArray[Anum_pg_dfs_file_update_version - 1] = true;
            needCatalogTupleUpdate = true;
        }
    }
    if (nlink) {
        *nlink = DatumGetUInt64(heap_getattr(heapTuple, Anum_pg_dfs_file_st_nlink, tupleDesc, &isNull));
        if (doUpdate) {
            if ((*nlink) + nlinkChangeNum == 0) // refcount changes to 0, need remove this inode row
            {
                CatalogTupleDelete(workerInodeRel, &heapTuple->t_self);
                CommandCounterIncrement();
            } else {
                updateDatumArray[Anum_pg_dfs_file_st_nlink - 1] = UInt64GetDatum((*nlink) + nlinkChangeNum);
                isNullArray[Anum_pg_dfs_file_st_nlink - 1] = false;
                doUpdateArray[Anum_pg_dfs_file_st_nlink - 1] = true;
                needCatalogTupleUpdate = true;
            }
        }
    }
    if (mode) {
        *mode = DatumGetUInt32(heap_getattr(heapTuple, Anum_pg_dfs_file_st_mode, tupleDesc, &isNull));
        if ((modeCheckType == MODE_CHECK_MUST_BE_FILE && !S_ISREG(*mode)) || 
            (modeCheckType == MODE_CHECK_MUST_BE_DIRECTORY && !S_ISDIR(*mode))) {
            if (statContext) {
                STAT_CKPT(statContext->statArrayIndex, statContext->opDoneCheckpoint);
            }
            systable_endscan(scanDescriptor);
            if (!workerInodeRelation) {
                table_close(workerInodeRel, doUpdate ? RowExclusiveLock : AccessShareLock);
            }
            // file exists, but is not the type expected
            return true;
        }
    }
    if (primaryNodeId) {
        *primaryNodeId = DatumGetUInt32(heap_getattr(heapTuple, Anum_pg_dfs_file_primary_nodeid, tupleDesc, &isNull));
    }
    if (backupNodeId) {
        *backupNodeId = DatumGetUInt32(heap_getattr(heapTuple, Anum_pg_dfs_file_backup_nodeid, tupleDesc, &isNull));
    }
    if (doUpdate && newUid) {
        updateDatumArray[Anum_pg_dfs_file_st_uid - 1] = UInt32GetDatum(*newUid);
        isNullArray[Anum_pg_dfs_file_st_uid - 1] = false;
        doUpdateArray[Anum_pg_dfs_file_st_uid - 1] = true;
        needCatalogTupleUpdate = true;
    }
    if (doUpdate && newGid) {
        updateDatumArray[Anum_pg_dfs_file_st_gid - 1] = UInt32GetDatum(*newGid);
        isNullArray[Anum_pg_dfs_file_st_gid - 1] = false;
        doUpdateArray[Anum_pg_dfs_file_st_gid - 1] = true;
        needCatalogTupleUpdate = true;
    }
    if (doUpdate && newExecMode) {
        mode_t oldMode =
            mode ? *mode : DatumGetUInt32(heap_getattr(heapTuple, Anum_pg_dfs_file_st_mode, tupleDesc, &isNull));
        mode_t newMode = (oldMode & ~0x1FFu) | *newExecMode;
        updateDatumArray[Anum_pg_dfs_file_st_mode - 1] = UInt32GetDatum(newMode);
        isNullArray[Anum_pg_dfs_file_st_mode - 1] = false;
        doUpdateArray[Anum_pg_dfs_file_st_mode - 1] = true;
        needCatalogTupleUpdate = true;
    }
    if (doUpdate && newEtag) {
        updateDatumArray[Anum_pg_dfs_file_etag - 1] = CStringGetTextDatum(newEtag);
        isNullArray[Anum_pg_dfs_file_etag - 1] = false;
        doUpdateArray[Anum_pg_dfs_file_etag - 1] = true;
        needCatalogTupleUpdate = true;
    }
    if (doUpdate && newAtime) {
        updateDatumArray[Anum_pg_dfs_file_st_atim - 1] = TimestampTzGetDatum(*newAtime);
        isNullArray[Anum_pg_dfs_file_st_atim - 1] = false;
        doUpdateArray[Anum_pg_dfs_file_st_atim - 1] = true;
        needCatalogTupleUpdate = true;
    }
    if (doUpdate && newMtime) {
        updateDatumArray[Anum_pg_dfs_file_st_mtim - 1] = TimestampTzGetDatum(*newMtime);
        isNullArray[Anum_pg_dfs_file_st_mtim - 1] = false;
        doUpdateArray[Anum_pg_dfs_file_st_mtim - 1] = true;
        needCatalogTupleUpdate = true;
    }
    if (doUpdate && newPrimaryNodeId) {
        updateDatumArray[Anum_pg_dfs_file_primary_nodeid - 1] = Int32GetDatum(*newPrimaryNodeId);
        isNullArray[Anum_pg_dfs_file_primary_nodeid - 1] = false;
        doUpdateArray[Anum_pg_dfs_file_primary_nodeid - 1] = true;
        needCatalogTupleUpdate = true;
    }

    if (doUpdate && needCatalogTupleUpdate) {
        HeapTuple updatedTuple = heap_modify_tuple(heapTuple, tupleDesc, updateDatumArray, isNullArray, doUpdateArray);
        CatalogTupleUpdate(workerInodeRel, &updatedTuple->t_self, updatedTuple);
        CommandCounterIncrement();
    }
    if (statContext) {
        STAT_CKPT(statContext->statArrayIndex, statContext->opDoneCheckpoint);
    }

    systable_endscan(scanDescriptor);
    if (!workerInodeRelation) {
        table_close(workerInodeRel, doUpdate ? RowExclusiveLock : AccessShareLock);
    }
    return true;
}

bool UpdateInodeLeaseCount(const char *workerInodeRelationName,
                           const char *workerInodeRelationIndexName,
                           const uint64_t parentId_partId,
                           const char *fileName,
                           int leaseCountChangeNum,
                           uint64_t *leaseCount)
{
    ScanKeyData scanKey[2];
    SysScanDesc scanDescriptor = NULL;
    HeapTuple heapTuple;
    TupleDesc tupleDesc;

    SetUpScanCaches();
    scanKey[0] = InodeTableScanKey[INODE_TABLE_PARENT_ID_PART_ID_EQ];
    scanKey[0].sk_argument = UInt64GetDatum(parentId_partId);
    scanKey[1] = InodeTableScanKey[INODE_TABLE_NAME_EQ];
    scanKey[1].sk_argument = CStringGetTextDatum(fileName);

    TimestampTz now = GetCurrentTimestamp();
    Relation workerInodeRel = table_open(GetRelationOidByName_FALCON(workerInodeRelationName), RowExclusiveLock);
    Oid workerInodeIndexOid = GetRelationOidByName_FALCON(workerInodeRelationIndexName);
    scanDescriptor = systable_beginscan(workerInodeRel,
                                        workerInodeIndexOid,
                                        true,
                                        GetTransactionSnapshot(),
                                        2,
                                        scanKey);
    heapTuple = systable_getnext(scanDescriptor);
    tupleDesc = RelationGetDescr(workerInodeRel);

    if (!HeapTupleIsValid(heapTuple)) {
        systable_endscan(scanDescriptor);
        table_close(workerInodeRel, RowExclusiveLock);
        return false;
    }

    if (tupleDesc->natts < Anum_pg_dfs_file_lease_expire_at) {
        FALCON_RAW_WARNING_EXTENDED(PROGRAM_ERROR,
                                   "inode lease columns missing, relation=%s, natts=%d, expected_natts=%d, parent_part=%lu, name=%s",
                                    workerInodeRelationName,
                                    tupleDesc->natts,
                                    Anum_pg_dfs_file_lease_expire_at,
                                    parentId_partId,
                                    fileName);
        if (leaseCount != NULL) {
            *leaseCount = UINT64_MAX;
        }
        systable_endscan(scanDescriptor);
        table_close(workerInodeRel, RowExclusiveLock);
        return true;
    }

    bool isNull = false;
    uint64_t currentLeaseCount = DatumGetUInt64(heap_getattr(heapTuple,
                                                            Anum_pg_dfs_file_lease_count,
                                                            tupleDesc,
                                                            &isNull));
    if (isNull) {
        currentLeaseCount = 0;
    }

    isNull = false;
    TimestampTz currentLeaseExpireAt = DatumGetTimestampTz(heap_getattr(heapTuple,
                                                                        Anum_pg_dfs_file_lease_expire_at,
                                                                        tupleDesc,
                                                                        &isNull));
    if (isNull) {
        currentLeaseExpireAt = FALCON_INODE_LEASE_EXPIRED_AT;
    }

    bool expiredLease = currentLeaseCount > 0 && currentLeaseExpireAt <= now;
    if (expiredLease) {
        currentLeaseCount = 0;
        currentLeaseExpireAt = FALCON_INODE_LEASE_EXPIRED_AT;
    }
    if (leaseCount != NULL) {
        *leaseCount = currentLeaseCount;
    }

    if (leaseCountChangeNum != 0 || expiredLease) {
        uint64_t newLeaseCount = currentLeaseCount;
        TimestampTz newLeaseExpireAt = currentLeaseExpireAt;
        if (leaseCountChangeNum > 0) {
            newLeaseCount += (uint64_t)leaseCountChangeNum;
            newLeaseExpireAt = now + FALCON_INODE_LEASE_TTL_US;
        } else if (leaseCountChangeNum < 0) {
            uint64_t releaseCount = (uint64_t)(-leaseCountChangeNum);
            newLeaseCount = currentLeaseCount > releaseCount ? currentLeaseCount - releaseCount : 0;
            newLeaseExpireAt = newLeaseCount > 0 ? now + FALCON_INODE_LEASE_TTL_US : FALCON_INODE_LEASE_EXPIRED_AT;
        } else {
            newLeaseCount = 0;
            newLeaseExpireAt = FALCON_INODE_LEASE_EXPIRED_AT;
        }

        Datum updateDatumArray[Natts_pg_dfs_inode_table];
        bool isNullArray[Natts_pg_dfs_inode_table];
        bool doUpdateArray[Natts_pg_dfs_inode_table];
        memset(updateDatumArray, 0, sizeof(updateDatumArray));
        memset(isNullArray, false, sizeof(isNullArray));
        memset(doUpdateArray, false, sizeof(doUpdateArray));
        updateDatumArray[Anum_pg_dfs_file_lease_count - 1] = UInt64GetDatum(newLeaseCount);
        isNullArray[Anum_pg_dfs_file_lease_count - 1] = false;
        doUpdateArray[Anum_pg_dfs_file_lease_count - 1] = true;
        updateDatumArray[Anum_pg_dfs_file_lease_expire_at - 1] = TimestampTzGetDatum(newLeaseExpireAt);
        isNullArray[Anum_pg_dfs_file_lease_expire_at - 1] = false;
        doUpdateArray[Anum_pg_dfs_file_lease_expire_at - 1] = true;

        HeapTuple updatedTuple = heap_modify_tuple(heapTuple, tupleDesc, updateDatumArray, isNullArray, doUpdateArray);
        CatalogTupleUpdate(workerInodeRel, &updatedTuple->t_self, updatedTuple);
        CommandCounterIncrement();
        if (leaseCount != NULL) {
            *leaseCount = newLeaseCount;
        }
    }

    systable_endscan(scanDescriptor);
    table_close(workerInodeRel, RowExclusiveLock);
    return true;
}

bool UnlinkInodeIfNoActiveLease(const char *workerInodeRelationName,
                                const char *workerInodeRelationIndexName,
                                const uint64_t parentId_partId,
                                const char *fileName,
                                const uint64_t expectedInodeId,
                                uint64_t *inodeId,
                                int64_t *size,
                                uint64_t *nlink,
                                mode_t *mode,
                                int32_t *primaryNodeId,
                                uint64_t *leaseCount,
                                bool *inodeMatched,
                                const InodeSearchStatContext *statContext)
{
    ScanKeyData scanKey[2];
    SysScanDesc scanDescriptor = NULL;
    HeapTuple heapTuple;

    SetUpScanCaches();
    scanKey[0] = InodeTableScanKey[INODE_TABLE_PARENT_ID_PART_ID_EQ];
    scanKey[0].sk_argument = UInt64GetDatum(parentId_partId);
    scanKey[1] = InodeTableScanKey[INODE_TABLE_NAME_EQ];
    scanKey[1].sk_argument = CStringGetTextDatum(fileName);

    if (inodeMatched != NULL) {
        *inodeMatched = false;
    }
    if (leaseCount != NULL) {
        *leaseCount = 0;
    }

    Relation workerInodeRel = table_open(GetRelationOidByName_FALCON(workerInodeRelationName), RowExclusiveLock);
    Oid workerInodeIndexOid = GetRelationOidByName_FALCON(workerInodeRelationIndexName);
    if (statContext) {
        STAT_CKPT(statContext->statArrayIndex, statContext->requestStartCheckpoint);
    }
    scanDescriptor = systable_beginscan(workerInodeRel,
                                        workerInodeIndexOid,
                                        true,
                                        GetTransactionSnapshot(),
                                        2,
                                        scanKey);
    heapTuple = systable_getnext(scanDescriptor);
    TupleDesc tupleDesc = RelationGetDescr(workerInodeRel);

    if (!HeapTupleIsValid(heapTuple)) {
        if (statContext) {
            STAT_CKPT(statContext->statArrayIndex, statContext->opDoneCheckpoint);
        }
        systable_endscan(scanDescriptor);
        table_close(workerInodeRel, RowExclusiveLock);
        return false;
    }

    bool isNull = false;
    uint64_t currentInodeId = DatumGetUInt64(heap_getattr(heapTuple, Anum_pg_dfs_file_st_ino, tupleDesc, &isNull));
    if (inodeId != NULL) {
        *inodeId = currentInodeId;
    }
    if (currentInodeId != expectedInodeId) {
        if (statContext) {
            STAT_CKPT(statContext->statArrayIndex, statContext->opDoneCheckpoint);
        }
        systable_endscan(scanDescriptor);
        table_close(workerInodeRel, RowExclusiveLock);
        return true;
    }
    if (inodeMatched != NULL) {
        *inodeMatched = true;
    }

    if (size != NULL) {
        *size = DatumGetInt64(heap_getattr(heapTuple, Anum_pg_dfs_file_st_size, tupleDesc, &isNull));
    }
    uint64_t currentNlink = DatumGetUInt64(heap_getattr(heapTuple, Anum_pg_dfs_file_st_nlink, tupleDesc, &isNull));
    if (nlink != NULL) {
        *nlink = currentNlink;
    }
    mode_t currentMode = DatumGetUInt32(heap_getattr(heapTuple, Anum_pg_dfs_file_st_mode, tupleDesc, &isNull));
    if (mode != NULL) {
        *mode = currentMode;
    }
    if (primaryNodeId != NULL) {
        *primaryNodeId = DatumGetUInt32(heap_getattr(heapTuple, Anum_pg_dfs_file_primary_nodeid, tupleDesc, &isNull));
    }

    if (tupleDesc->natts < Anum_pg_dfs_file_lease_expire_at) {
        FALCON_RAW_WARNING_EXTENDED(PROGRAM_ERROR,
                                   "inode lease columns missing during evict unlink, relation=%s, natts=%d, expected_natts=%d, parent_part=%lu, name=%s",
                                    workerInodeRelationName,
                                    tupleDesc->natts,
                                    Anum_pg_dfs_file_lease_expire_at,
                                    parentId_partId,
                                    fileName);
        if (leaseCount != NULL) {
            *leaseCount = UINT64_MAX;
        }
        if (statContext) {
            STAT_CKPT(statContext->statArrayIndex, statContext->opDoneCheckpoint);
        }
        systable_endscan(scanDescriptor);
        table_close(workerInodeRel, RowExclusiveLock);
        return true;
    }

    uint64_t currentLeaseCount = DatumGetUInt64(heap_getattr(heapTuple,
                                                            Anum_pg_dfs_file_lease_count,
                                                            tupleDesc,
                                                            &isNull));
    if (isNull) {
        currentLeaseCount = 0;
    }
    isNull = false;
    TimestampTz currentLeaseExpireAt = DatumGetTimestampTz(heap_getattr(heapTuple,
                                                                        Anum_pg_dfs_file_lease_expire_at,
                                                                        tupleDesc,
                                                                        &isNull));
    if (isNull) {
        currentLeaseExpireAt = FALCON_INODE_LEASE_EXPIRED_AT;
    }
    if (currentLeaseCount > 0 && currentLeaseExpireAt <= GetCurrentTimestamp()) {
        currentLeaseCount = 0;
    }
    if (leaseCount != NULL) {
        *leaseCount = currentLeaseCount;
    }
    if (currentLeaseCount > 0 || !S_ISREG(currentMode) || currentNlink != 1) {
        if (statContext) {
            STAT_CKPT(statContext->statArrayIndex, statContext->opDoneCheckpoint);
        }
        systable_endscan(scanDescriptor);
        table_close(workerInodeRel, RowExclusiveLock);
        return true;
    }

    CatalogTupleDelete(workerInodeRel, &heapTuple->t_self);
    CommandCounterIncrement();

    if (statContext) {
        STAT_CKPT(statContext->statArrayIndex, statContext->opDoneCheckpoint);
    }
    systable_endscan(scanDescriptor);
    table_close(workerInodeRel, RowExclusiveLock);
    return true;
}

StringInfo GetInodeShardName(int shardId)
{
    StringInfo inodeShardName = makeStringInfo();
    appendStringInfo(inodeShardName, "%s_%d", InodeTableName, shardId);
    return inodeShardName;
}

StringInfo GetInodeIndexShardName(int shardId)
{
    StringInfo inodeIndexShardName = makeStringInfo();
    appendStringInfo(inodeIndexShardName, "%s_%d_%s", InodeTableName, shardId, "index");
    return inodeIndexShardName;
}

StringInfo __attribute__((unused)) GetXattrShardName(int shardId)
{
    StringInfo xattrShardName = makeStringInfo();
    appendStringInfo(xattrShardName, "%s_%d", XattrTableName, shardId);
    return xattrShardName;
}

StringInfo __attribute__((unused)) GetXattrIndexShardName(int shardId)
{
    StringInfo xattrIndexShardName = makeStringInfo();
    appendStringInfo(xattrIndexShardName, "%s_%d_%s", XattrTableName, shardId, "index");
    return xattrIndexShardName;
}

StringInfo GetSliceShardName(int shardId)
{
    StringInfo sliceShardName = makeStringInfo();
    appendStringInfo(sliceShardName, "%s_%d", SliceTableName, shardId);
    return sliceShardName;
}

StringInfo GetSliceIndexShardName(int shardId)
{
    StringInfo sliceIndexShardName = makeStringInfo();
    appendStringInfo(sliceIndexShardName, "%s_%d_%s", SliceTableName, shardId, "index");
    return sliceIndexShardName;
}

StringInfo GetKvmetaShardName(int shardId)
{
    StringInfo kvmetaShardName = makeStringInfo();
    appendStringInfo(kvmetaShardName, "%s_%d", KvmetaTableName, shardId);
    return kvmetaShardName;
}

StringInfo GetKvmetaIndexShardName(int shardId)
{
    StringInfo kvmetaIndexShardName = makeStringInfo();
    appendStringInfo(kvmetaIndexShardName, "%s_%d_%s", KvmetaTableName, shardId, "index");
    return kvmetaIndexShardName;
}
