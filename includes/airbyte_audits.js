/* Generates audit-linkage tables/operations for Airbyte CDC entities, when params.airbyteAudit.enabled.

   For each entity in dataSchema:
   - {entity}_audits_{source}_airbyte          (table)   - keys-only projection of the raw audits table
   - {entity}_audit_link_apply_{source}        (operation: UPDATE) - fills request_uuid/audit_id/web columns
     onto {entity}_version_{source}_airbyte by nearest-time match.

   Design mirrors airbyte_reconciliation.js: detect/log once, then apply via an idempotent UPDATE
   so the versioning logic in airbyte_entity_version.js is untouched.
*/

/* Shared naming/reference helper, mirroring reconciliationNames() in airbyte_reconciliation.js */
function auditNames(params, entitySchema) {
  const suffix = params.airbyteConfig.tableSuffix || '_airbyte';
  return {
    primaryKey: entitySchema.primaryKey || params.airbyteConfig.defaultPrimaryKeyField || 'id',
    versionTableName: `${entitySchema.entityTableName}_version_${params.eventSourceName}${suffix}`,
    auditsTableName: `audit`,
    auditedWebRequestsTableName: `audited_web_requests_${params.eventSourceName}${suffix}`,
    applyOperationName: `${entitySchema.entityTableName}_audit_link_apply_${params.eventSourceName}`,
  };
}

/* Step 1: keys-only projection of the raw Airbyte-synced audits table, scoped to this entity.
   Only auditable_id, action, request_uuid and created_at are kept - the raw audits table is
   scanned once per entity per run rather than being re-read downstream. */
function auditsQuery({ auditsSourceTable, auditableType, primaryKeyField }) {
  return `SELECT
  CAST(auditable_id AS STRING) AS \`${primaryKeyField}\`,
  action,
  request_uuid,
  CAST(created_at AS TIMESTAMP) AS audit_created_at,
  CAST(id AS STRING) AS audit_id
FROM
  ${auditsSourceTable}
WHERE
  auditable_type = ${JSON.stringify(auditableType)}
  AND request_uuid IS NOT NULL`;
}

/* Step 2: nearest-time match between each version row and its audit.
   Matches on (entity, id) with the audit whose created_at is closest to the version's valid_from,
   within toleranceSeconds. action is used as a tie-breaker when two audits are equidistant
   (prefers 'update' matches for update-like versions, falls back to closest absolute value). */
function auditLinkMatchQuery({ versionTable, auditsTable, primaryKeyField, toleranceSeconds }) {
  return `SELECT
  version.${primaryKeyField} AS ${primaryKeyField},
  version.valid_from AS valid_from,
  audit.request_uuid AS request_uuid,
  audit.audit_id AS audit_id
FROM
  ${versionTable} AS version
INNER JOIN
  ${auditsTable} AS audit
ON
  version.${primaryKeyField} = audit.${primaryKeyField}
  AND ABS(TIMESTAMP_DIFF(version.valid_from, audit.audit_created_at, SECOND)) <= ${toleranceSeconds}
WHERE
  version.request_uuid IS NULL
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY version.${primaryKeyField}, version.valid_from
  ORDER BY
    ABS(TIMESTAMP_DIFF(version.valid_from, audit.audit_created_at, SECOND)) ASC,
    IF(audit.action = 'update', 0, 1) ASC
) = 1`;
}

/* Step 3: apply. Idempotent UPDATE, following the same pattern as
   airbyte_reconciliation.js's applyReconciliationQuery: request_uuid IS NULL in the WHERE clause
   excludes rows already linked, so re-running is a no-op for already-matched rows. */
function applyAuditLinkQuery({ versionTable, matchQuery, primaryKeyField }) {
  return `UPDATE ${versionTable} AS version
SET
  version.request_uuid = link.request_uuid,
  version.audit_id = link.audit_id
FROM (
  ${matchQuery}
) AS link
WHERE
  version.${primaryKeyField} = link.${primaryKeyField}
  AND version.valid_from = link.valid_from
  AND version.request_uuid IS NULL`;
}

/* Publish the functions above */
module.exports = (params) => {
  if (!params.enableAirbyteSource || !params.airbyteAudit || !params.airbyteAudit.enabled) return null;
  
  const auditsSourceTable = `\`${params.bqProjectName}.${params.airbyteConfig.datasetName}.${params.airbyteAudit.auditsTableName || 'audits'}\``;

  return params.dataSchema.map(entitySchema => {
    const names = auditNames(params, entitySchema);

    publish(names.auditsTableName, {
      type: "table",
      tags: [params.eventSourceName.toLowerCase(), 'airbyte', 'audit'],
      description: `[AIRBYTE] Keys-only projection of the raw audits table for ${entitySchema.entityTableName}, used to bridge web request context onto the Airbyte version table.`,
      columns: {
        [names.primaryKey]: `Primary key of the audited ${entitySchema.entityTableName} entity.`,
        action: "The audited gem action recorded for this audit (e.g. create, update, destroy).",
        request_uuid: "request_uuid of the web request that produced this audit, used to join to events.",
        audit_created_at: "Timestamp the audit record was created in the source database.",
        audit_id: "Primary key of the audit record itself."
      }
    }).query(ctx => auditsQuery({
      auditsSourceTable,
      auditableType: entitySchema.auditableType || entitySchema.entityTableName,
      primaryKeyField: names.primaryKey
    }));

    return operate(names.applyOperationName, {
      tags: [params.eventSourceName.toLowerCase(), 'airbyte', 'audit'],
      dependencies: [names.auditsTableName],
      description: `[AIRBYTE] Links each ${entitySchema.entityTableName} version to its nearest-time audit, filling in request_uuid and audit_id on the version table.`
    }).queries(ctx => applyAuditLinkQuery({
      versionTable: ctx.ref(names.versionTableName),
      matchQuery: auditLinkMatchQuery({
        versionTable: ctx.ref(names.versionTableName),
        auditsTable: ctx.ref(names.auditsTableName),
        primaryKeyField: names.primaryKey,
        toleranceSeconds: params.airbyteAudit.matchToleranceSeconds || 5
      }),
      primaryKeyField: names.primaryKey
    }));
  });
};

// Named properties for Jest / airbyte_entity_latest.js:
module.exports.auditNames = auditNames;
module.exports.auditsQuery = auditsQuery;
module.exports.auditLinkMatchQuery = auditLinkMatchQuery;
module.exports.applyAuditLinkQuery = applyAuditLinkQuery;