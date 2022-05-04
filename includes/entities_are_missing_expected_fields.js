module.exports = (params) => {
  return assert(params.tableSuffix + "_entities_are_missing_expected_fields", {
    ...params.defaultConfig,
    type: "assertion",
    description: "Counts the number of entities updated yesterday which did not contain an expected field, excluding updates which were before a new field was introduced partway through the day. The list is taken from the dataSchema JSON parameter passed to dfe-analytics-dataform, but the assertion will fail if that file is updated but this assertion is not in order to alert us that we need to think through the implications for analytics of losing a field. If this assertion fails, we need to ask developers why, and ask them either to fix the bug, or if the field was intentionally removed, remove the field from dataSchema and any points where it used downstream in the pipeline."
  }).query(ctx => `
WITH expected_entity_fields AS (
  SELECT
    entity_name,
    key
  FROM
    ${ctx.ref(params.tableSuffix + "_analytics_yml_latest")},
    UNNEST(keys) AS key
)
SELECT
  entity_name,
  key,
  COUNT(
    IF(
      NOT ${data_functions.keyIsInEventData("DATA", "key")},
      entity_id,
      NULL
    )
  ) AS updates_made_yesterday_without_this_key,
  COUNT(
    IF(
      ${data_functions.keyIsInEventData("DATA", "key")},
      entity_id,
      NULL
    )
  ) AS updates_made_yesterday_with_this_key,
  MIN(
    IF(
      NOT ${data_functions.keyIsInEventData("DATA", "key")},
      valid_from,
      NULL
    )
  ) AS first_update_yesterday_without_this_key_at,
  MAX(
    IF(
      NOT ${data_functions.keyIsInEventData("DATA", "key")},
      valid_from,
      NULL
    )
  ) AS last_update_yesterday_without_this_key_at,
  MIN(
    IF(
      ${data_functions.keyIsInEventData("DATA", "key")},
      valid_from,
      NULL
    )
  ) AS first_update_yesterday_with_this_key_at,
  MAX(
    IF(
      ${data_functions.keyIsInEventData("DATA", "key")},
      valid_from,
      NULL
    )
  ) AS last_update_yesterday_with_this_key_at
FROM
  ${ctx.ref(params.tableSuffix + "_entity_version")}
  JOIN expected_entity_fields ON entity_name = entity_table_name
WHERE
  DATE(valid_from) >= CURRENT_DATE - 1
  AND event_type != "delete_entity"
GROUP BY
  entity_name,
  key
HAVING
  updates_made_yesterday_without_this_key > 0
  /* Don't flag an error if the field was a new field introduced midway through yesterday. */
  AND NOT (last_update_yesterday_without_this_key_at < first_update_yesterday_with_this_key_at)
ORDER BY
  entity_name,
  key`)
}
