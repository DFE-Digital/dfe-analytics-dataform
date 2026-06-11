/* Generates full-refresh reconciliation tables for Airbyte CDC entities. 

   For each entity in dataSchema, when params.airbyteReconciliation.enabled:
    - {entity}_airbyte_full_refreshes_{source}            (table)
        Completed Airbyte full-refresh snapshots, detected by LSN signature:
        a full refresh emits the entire table under a single _ab_cdc_lsn with
        zero deletes.
    - {entity}_airbyte_reconciliation_deletes_{source}    (incremental, protected)
        Append-only log of entities inferred deleted because they were absent
        from the latest full refresh. Consumed by the deletions CTE in
        airbyte_entity_version.js, and the permanent audit trail. protected:
        a version-table full refresh must REPRODUCE reconciled deletions, not
        resurrect ghost rows.
    - {entity}_airbyte_reconciliation_exceeds_safe_delete_volume_{source} (assertion)
        Circuit breaker: fails when pending inferred deletions exceed
        maxDeleteFraction of live rows (the signature of a bulk transaction
        misclassified as a full refresh). The version table depends on this
        assertion, so a trip blocks application.
*/

/* Shared naming/reference helper */
function reconciliationNames(params, entitySchema) {
  const suffix = params.airbyteConfig.tableSuffix || '_airbyte';
  return {
    primaryKey: entitySchema.primaryKey || params.airbyteConfig.primaryKeyField || 'id',
    sourceTable: `\`${params.bqProjectName}.${params.airbyteConfig.datasetName}.${entitySchema.entityTableName}\``,
    versionTableName: `${entitySchema.entityTableName}_version_${params.eventSourceName}${suffix}`,
    latestTableName: `${entitySchema.entityTableName}_latest_${params.eventSourceName}${suffix}`,
    fullRefreshesTableName: `${entitySchema.entityTableName}_airbyte_full_refreshes_${params.eventSourceName}`,
    reconciliationDeletesTableName: `${entitySchema.entityTableName}_airbyte_reconciliation_deletes_${params.eventSourceName}`,
    volumeGuardAssertionName: `${entitySchema.entityTableName}_airbyte_reconciliation_exceeds_safe_delete_volume_${params.eventSourceName}`,
  };
}

/* Step 1: Detects completed Airbyte full-refresh snapshots for one entity using the LSN signature.
   A full refresh emits the entire table under a single _ab_cdc_lsn with zero deletes. 
   A single bulk transaction can mimic this, which is why application is gated by the volume guard assertion. 
*/
function fullRefreshDetectionQuery(
    airbyteSourceTable,
    latestTable,
    minLiveFraction,
    minSnapshotAgeMinutes,
    detectionWindowDays
) {
    return `WITH live AS (
  SELECT
    COUNT(*) AS live_row_count
  FROM
    ${latestTable}
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
    ${airbyteSourceTable}
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
function reconciliationDeletesQuery(
  sourceTable,               // FQN of the raw Airbyte source table
  detectionTable,            // resolved name of the detection table
  versionTable,              // resolved name of the entity version table
  primaryKeyField,           // e.g. 'id'
  selfTable                  // resolved self name on incremental runs, else null
) {
  const incrementalGuard = selfTable
    ? `\n  AND NOT EXISTS (
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
function volumeGuardQuery(
    deletesTable,
    versionTable,
    latestTable,
    primaryKeyField,
    maxDeleteFraction,
    forceReconcileSnapshotLsn
) {
  const overrideClause = forceReconcileSnapshotLsn
    ? `\n  AND pending.snapshot_lsn != ${forceReconcileSnapshotLsn} /* one-shot override set in airbyteReconciliation */`
    : '';
  return `WITH live AS (
  SELECT
    COUNT(*) AS live_row_count
  FROM
    ${latestTable}
),
pending AS (
  SELECT
    COUNT(*) AS pending_delete_count,
    ANY_VALUE(log.snapshot_lsn) AS snapshot_lsn
  FROM
    ${deletesTable} AS log
  INNER JOIN
    ${versionTable} AS version
  ON
    version.${primaryKeyField} = log.${primaryKeyField}
    AND version.valid_to IS NULL
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

/* Publish the functions above */
module.exports = (params) => {
  if (!params.enableAirbyteSource || !params.airbyteReconciliation.enabled) return null;

  return params.dataSchema.map(entitySchema => {
    const names = reconciliationNames(params, entitySchema);

    publish(names.fullRefreshesTableName, {
      type: "table",
      tags: [params.eventSourceName.toLowerCase(), 'airbyte', 'reconciliation'],
      description: `[AIRBYTE] Full refresh snapshots of ${entitySchema.entityTableName} detected by LSN signature, used to reconcile deletions missed by CDC.`
    }).query(ctx => fullRefreshDetectionQuery(
      names.sourceTable,
      ctx.resolve(names.latestTableName), // resolve, not ref: avoids dependency cycle
      params.airbyteReconciliation.minLiveFraction,
      params.airbyteReconciliation.minSnapshotAgeMinutes,
      params.airbyteReconciliation.detectionWindowDays
    ));

    publish(names.reconciliationDeletesTableName, {
      type: "incremental",
      protected: true, // survives full refreshes: this log is what stops version-table rebuilds resurrecting reconciled ghost rows
      uniqueKey: [names.primaryKey, "snapshot_lsn"],
      tags: [params.eventSourceName.toLowerCase(), 'airbyte', 'reconciliation'],
      description: `[AIRBYTE] Append-only log of ${entitySchema.entityTableName} entities inferred deleted because they were absent from an Airbyte full refresh. Consumed by the deletions logic in the version table; also the permanent audit trail of reconciled deletions. deleted_at_assumed is the snapshot extraction time, unlike CDC deletion timestamps which are source-database time.`
    }).query(ctx => reconciliationDeletesQuery(
      names.sourceTable,
      ctx.ref(names.fullRefreshesTableName),
      ctx.resolve(names.versionTableName),
      ctx.resolve(names.latestTableName),
      names.primaryKey,
      ctx.incremental() ? ctx.self() : null
    ));

    return assert(names.volumeGuardAssertionName, {
      tags: [params.eventSourceName.toLowerCase(), 'airbyte', 'reconciliation'],
      description: `[AIRBYTE] Blocks the ${entitySchema.entityTableName} version table if pending inferred deletions exceed airbyteReconciliation.maxDeleteFraction of live rows, which may indicate a bulk transaction misclassified as a full refresh.`
    }).query(ctx => volumeGuardQuery(
      ctx.ref(names.reconciliationDeletesTableName),
      ctx.resolve(names.latestTableName),
      names.primaryKey,
      params.airbyteReconciliation.maxDeleteFraction,
      params.airbyteReconciliation.forceReconcileSnapshotLsn
    ));
  });
};

// Named properties for airbyte_entity_version.js and Jest:
module.exports.reconciliationNames = reconciliationNames;
module.exports.fullRefreshDetectionQuery = fullRefreshDetectionQuery;
module.exports.reconciliationDeletesQuery = reconciliationDeletesQuery;
module.exports.volumeGuardQuery = volumeGuardQuery;
