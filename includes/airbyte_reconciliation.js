/* Generates full-refresh reconciliation tables for Airbyte CDC entities. 

   For each entity in dataSchema, when params.airbyteReconciliation.enabled:
  - {entity}_airbyte_full_refreshes_{source}                       (table)
  - {entity}_airbyte_reconciliation_deletes_{source}               (incremental, protected)
  - {entity}_airbyte_reconciliation_exceeds_safe_delete_volume_{source} (assertion)
  - {entity}_airbyte_reconciliation_apply_{source}                 (operation: UPDATE)

  The apply operation folds inferred deletions into the version table the same way the builder folds observed CDC deletions: 
  by closing the last live version (valid_to / is_current / is_deleted / deleted_at). No INSERT needed.

*/

/* Shared naming/reference helper */
function reconciliationNames(params, entitySchema) {
  const suffix = params.airbyteConfig.tableSuffix || '_airbyte';
  return {
    primaryKey: entitySchema.primaryKey || params.airbyteConfig.primaryKeyField || 'id',
    sourceTable: `\`${params.bqProjectName}.${params.airbyteConfig.datasetName}.${entitySchema.entityTableName}\``,
    versionTableName: `${entitySchema.entityTableName}_version_${params.eventSourceName}${suffix}`,
    fullRefreshesTableName: `${entitySchema.entityTableName}_airbyte_full_refreshes_${params.eventSourceName}`,
    reconciliationDeletesTableName: `${entitySchema.entityTableName}_airbyte_reconciliation_deletes_${params.eventSourceName}`,
    volumeGuardAssertionName: `${entitySchema.entityTableName}_airbyte_reconciliation_exceeds_safe_delete_volume_${params.eventSourceName}`,
    applyOperationName: `${entitySchema.entityTableName}_airbyte_reconciliation_apply_${params.eventSourceName}`,
  };
}

/* Step 1: Detects completed Airbyte full-refresh snapshots for one entity using the LSN signature.
   A full refresh emits the entire table under a single _ab_cdc_lsn with zero deletes. 
   A single bulk transaction can mimic this, which is why application is gated by the volume guard assertion. 
*/
function fullRefreshDetectionQuery({ sourceTable, versionTable, minLiveFraction, minSnapshotAgeMinutes, detectionWindowDays }) {
  return `WITH live AS (
  /* Live rows are exactly valid_to IS NULL in this builder (deleted entities have valid_to set) */
  SELECT
    COUNT(*) AS live_row_count
  FROM
    ${versionTable}
  WHERE
    valid_to IS NULL
),
lsn_stats AS (
  SELECT
    _ab_cdc_lsn AS lsn,
    COUNT(*) AS row_count,
    COUNTIF(_ab_cdc_deleted_at IS NOT NULL) AS delete_count,
    COUNT(DISTINCT CAST(JSON_VALUE(_airbyte_meta, '$.sync_id') AS INT64)) AS sync_count,
    MIN(_airbyte_extracted_at) AS snapshot_started_at,
    MAX(_airbyte_extracted_at) AS snapshot_finished_at
  FROM
    ${sourceTable}
  WHERE
    _airbyte_extracted_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL ${detectionWindowDays} DAY)
  GROUP BY
    lsn
)
SELECT
  lsn_stats.lsn,
  lsn_stats.row_count,
  lsn_stats.sync_count,
  lsn_stats.snapshot_started_at,
  lsn_stats.snapshot_finished_at,
  live.live_row_count,
  SAFE_DIVIDE(lsn_stats.row_count, live.live_row_count) AS fraction_of_live
FROM
  lsn_stats
CROSS JOIN
  live
WHERE
  lsn_stats.delete_count = 0
  AND lsn_stats.row_count >= ${minLiveFraction} * live.live_row_count
  /* in-flight guard: resumable snapshots can span multiple syncs */
  AND lsn_stats.snapshot_finished_at < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL ${minSnapshotAgeMinutes} MINUTE)`;
}

/* Step 2: Append-only log of inferred deletions. 
   selfTable is non-null on incremental runs and prevents re-logging PKs already recorded for the same snapshot.
*/
function reconciliationDeletesQuery({sourceTable, detectionTable, versionTable, primaryKeyField, selfTable}) {
  const incrementalGuard = selfTable
    ? `
  AND NOT EXISTS (
    SELECT 1
    FROM ${selfTable} AS log
    WHERE log.${primaryKeyField} = version.${primaryKeyField}
      AND log.snapshot_lsn = latest_snapshot.lsn
  )`
    : '';
  return `WITH latest_snapshot AS (
  SELECT
    lsn,
    snapshot_started_at,
    snapshot_finished_at
  FROM
    ${detectionTable}
  ORDER BY
    snapshot_finished_at DESC
  LIMIT 1
),
snapshot_pks AS (
  SELECT DISTINCT
    CAST(airbyte_source.${primaryKeyField} AS STRING) AS pk
  FROM
    ${sourceTable} AS airbyte_source
  INNER JOIN
    latest_snapshot
  ON
    airbyte_source._ab_cdc_lsn = latest_snapshot.lsn
    AND airbyte_source._airbyte_extracted_at >= latest_snapshot.snapshot_started_at 
        AND latest_snapshot.snapshot_finished_at
)
SELECT
  version.${primaryKeyField} AS ${primaryKeyField},
  latest_snapshot.lsn AS snapshot_lsn,
  latest_snapshot.snapshot_started_at,
  latest_snapshot.snapshot_finished_at AS deleted_at_assumed,
  CURRENT_TIMESTAMP() AS detected_at
FROM
  ${versionTable} AS version
CROSS JOIN
  latest_snapshot
LEFT JOIN
  snapshot_pks
ON
  snapshot_pks.pk = version.${primaryKeyField}
WHERE
  version.valid_to IS NULL
  AND snapshot_pks.pk IS NULL
  /* ordering guard: never flag rows first seen during or after the snapshot */
  AND version.valid_from < latest_snapshot.snapshot_started_at${incrementalGuard}`;
}

/* Step 3: Circuit breaker assertion. 
   Returns rows (fails) when the pending reconciliation would delete more than maxDeleteFraction of live rows - the signature of a bulk transaction misclassified as a full refresh.
   forceReconcileSnapshotLsn provides a deliberate one-shot override. 
*/
function volumeGuardQuery({deletesTable, versionTable, primaryKeyField, maxDeleteFraction, forceReconcileSnapshotLsn}) {
  const overrideClause = forceReconcileSnapshotLsn
    ? `
  AND pending.snapshot_lsn != ${JSON.stringify(forceReconcileSnapshotLsn)} /* one-shot override set in airbyteReconciliation; set, run, remove */`

    : '';
  return `WITH live AS (
  SELECT
    COUNT(*) AS live_row_count
  FROM
    ${versionTable}
  WHERE
    valid_to IS NULL
),
pending AS (
  SELECT
    log.snapshot_lsn AS snapshot_lsn,

    COUNT(*) AS pending_delete_count

  FROM
    ${deletesTable} AS log
  INNER JOIN
    ${versionTable} AS version
  ON
    version.${primaryKeyField} = log.${primaryKeyField}
    AND version.valid_to IS NULL
  GROUP BY

    log.snapshot_lsn

)
SELECT
  pending.snapshot_lsn,
  pending.pending_delete_count,
  live.live_row_count,
  SAFE_DIVIDE(pending.pending_delete_count, live.live_row_count) AS delete_fraction
FROM
  pending
CROSS JOIN
  live
WHERE
  SAFE_DIVIDE(pending.pending_delete_count, live.live_row_count) > ${maxDeleteFraction}${overrideClause}`;
}

/* Step 4: apply. Folds inferred deletions into the version table exactly like observed CDC deletions: close the last live version. 
   Idempotent: valid_to IS NULL excludes already-applied rows; a real CDC delete that beat us to the same PK in the same run also drops out. 
   The ordering guard protects rows first seen during/after the snapshot. */
function applyReconciliationQuery({ versionTable, deletesTable, primaryKeyField }) {
  return `UPDATE ${versionTable} AS version
SET
  version.valid_to = log.deleted_at_assumed,
  version.deleted_at = log.deleted_at_assumed,
  version.is_current = FALSE,
  version.is_deleted = TRUE
FROM
  ${deletesTable} AS log
WHERE
  version.${primaryKeyField} = log.${primaryKeyField}
  AND version.valid_to IS NULL
  AND version.valid_from < log.snapshot_started_at`;
}

/* Publish the functions above */
module.exports = (params) => {
  if (!params.enableAirbyteSource || !params.airbyteReconciliation.enabled) return null;

  return params.dataSchema.map(entitySchema => {
    const names = reconciliationNames(params, entitySchema);

    publish(names.fullRefreshesTableName, {
      type: "table",
      tags: [params.eventSourceName.toLowerCase(), 'airbyte', 'reconciliation'],
      description: `[AIRBYTE] Full refresh snapshots of ${entitySchema.entityTableName} detected by LSN signature, used to reconcile deletions missed by CDC.`
    }).query(ctx => fullRefreshDetectionQuery({
      sourceTable: names.sourceTable,
      versionTable: ctx.ref(names.versionTableName), // ref: runs AFTER version exists - this is what makes new entities bootstrap safely
      minLiveFraction: params.airbyteReconciliation.minLiveFraction,
      minSnapshotAgeMinutes: params.airbyteReconciliation.minSnapshotAgeMinutes,
      detectionWindowDays: params.airbyteReconciliation.detectionWindowDays
    }));

    publish(names.reconciliationDeletesTableName, {
      type: "incremental",
      protected: true, // survives full refreshes: the log is the audit trail and what lets a version-table rebuild reproduce reconciled deletions
      uniqueKey: [names.primaryKey, "snapshot_lsn"],
      tags: [params.eventSourceName.toLowerCase(), 'airbyte', 'reconciliation'],
      description: `[AIRBYTE] Append-only log of ${entitySchema.entityTableName} entities inferred deleted because they were absent from an Airbyte full refresh. deleted_at_assumed is snapshot extraction time, unlike CDC deletion timestamps which are source-database time.`
    }).query(ctx => reconciliationDeletesQuery({
      sourceTable: names.sourceTable,
      detectionTable: ctx.ref(names.fullRefreshesTableName),
      versionTable: ctx.ref(names.versionTableName),
      primaryKeyField: names.primaryKey,
      selfTable: ctx.incremental() ? ctx.self() : null
    }));

    assert(names.volumeGuardAssertionName, {
      tags: [params.eventSourceName.toLowerCase(), 'airbyte', 'reconciliation'],
      description: `[AIRBYTE] Blocks the reconciliation apply step for ${entitySchema.entityTableName} if pending inferred deletions exceed airbyteReconciliation.maxDeleteFraction of live rows, which may indicate a bulk transaction misclassified as a full refresh.`
    }).query(ctx => volumeGuardQuery({
      deletesTable: ctx.ref(names.reconciliationDeletesTableName),
      versionTable: ctx.ref(names.versionTableName),
      primaryKeyField: names.primaryKey,
      maxDeleteFraction: params.airbyteReconciliation.maxDeleteFraction,
      forceReconcileSnapshotLsn: params.airbyteReconciliation.forceReconcileSnapshotLsn
    }));

    return operate(names.applyOperationName, {
      tags: [params.eventSourceName.toLowerCase(), 'airbyte', 'reconciliation'],
      dependencyTargets: [{ name: names.volumeGuardAssertionName }], // declarative circuit breaker
      description: `[AIRBYTE] Applies inferred deletions from full refresh reconciliation to the ${entitySchema.entityTableName} version table by closing the last live version.`
    }).queries(ctx => applyReconciliationQuery({
      versionTable: ctx.ref(names.versionTableName),
      deletesTable: ctx.ref(names.reconciliationDeletesTableName),
      primaryKeyField: names.primaryKey
    }));
  });
};

// Named properties for airbyte_entity_latest.js and Jest:
module.exports.reconciliationNames = reconciliationNames;
module.exports.fullRefreshDetectionQuery = fullRefreshDetectionQuery;
module.exports.reconciliationDeletesQuery = reconciliationDeletesQuery;
module.exports.volumeGuardQuery = volumeGuardQuery;
module.exports.applyReconciliationQuery = applyReconciliationQuery;
