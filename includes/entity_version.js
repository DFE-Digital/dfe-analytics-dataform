module.exports = (params) => {
    function idField(dataSchema) {
        /* Generates SQL that extracts the primary key from the 'data' array of structs, defaulting to 'id', but using the primary_key configured for each entity_table_name if specified. */
        var sqlToReturn = 'CASE\n';
        var allPrimaryKeysAreId = true;
        dataSchema.forEach(tableSchema => {
            if (!tableSchema.primary_key) {

            } else if (tableSchema.primary_key == "id") {
                throw new Error(`primary_key for the ${tableSchema.entityTableName} table is set to 'id', which is the default value for primary_key. If id is the primary key for this table in the database, remove the primary_key configuration for this table in your dataSchema. If id is not the primary key for this table in the database, set primary_key to the correct primary key.`);
            } else {
                sqlToReturn += `WHEN entity_table_name = '${tableSchema.entityTableName}' THEN ${data_functions.eventDataExtract("data", tableSchema.primary_key)}\n`;
                allPrimaryKeysAreId = false;
            }
        })
        sqlToReturn += `ELSE ${data_functions.eventDataExtract("data", "id")}\nEND\n`;
        if (allPrimaryKeysAreId) {
            sqlToReturn = data_functions.eventDataExtract("data", "id");
        }
        return sqlToReturn;
    }
    return publish(params.eventSourceName + "_entity_version", {
            ...params.defaultConfig,
            type: "incremental",
            protected: false,
            uniqueKey: ["entity_table_name", "entity_id", "valid_from"],
            assertions: {
                uniqueKey: ["entity_table_name", "entity_id", "valid_from"],
                nonNull: ["entity_table_name", "entity_id", "valid_from"],
                rowConditions: [
                    'valid_from < valid_to OR valid_to IS NULL'
                ]
            },
            dependencies: [params.eventSourceName + "_entities_are_missing_expected_fields"],
            bigquery: {
                partitionBy: "DATE(valid_to)",
                clusterBy: ["entity_table_name"],
                updatePartitionFilter: "valid_to IS NULL",
                labels: {
                    eventsource: params.eventSourceName.toLowerCase(),
                    sourcedataset: params.bqDatasetName.toLowerCase()
                }
            },
            tags: [params.eventSourceName.toLowerCase()],
            description: "Each row represents a version of an entity in the " + params.eventSourceName + " database that was been streamed into the events table in the " + params.bqDatasetName + " dataset in the " + params.bqProjectName + " BigQuery project. Versions are valid from valid_from until just before valid_to. If valid_to is NULL then this version is the latest version of this entity. If valid_to is not NULL, but no later version exists, then this entity has been deleted.",
            columns: {
                valid_from: "Timestamp from which this version of this entity started to be valid.",
                valid_to: "Timestamp until which this version of this entity was valid.",
                event_type: "Event type of the event that provided us with this version of this entity. Either create_entity, update_entity or import_entity.",
                entity_table_name: "Indicates which table this entity version came from",
                entity_id: "Hashed (anonymised) version of the ID of this entity from the database.",
                created_at: "Timestamp this entity was first saved in the database, according to the latest version of the data received from the database.",
                updated_at: "Timestamp this entity was last updated in the database, according to the latest version of the data received from the database.",
                DATA: "ARRAY of STRUCTs containing all data stored against this entity as of the latest version we have. Some fields that are in the database may have been removed or hashed (anonymised) if they contained personally identifiable information (PII) or were not deemed to be useful for analytics. NULL if entity has been deleted from the database.",
                request_user_id: "If a user was logged in when they sent a web request event that caused this version to be created, then this is the UID of this user.",
                request_uuid: "UUID of the web request that caused this version to be created.",
                request_method: "Whether the web request that caused this version to be created was a GET or a POST request.",
                request_path: "The path, starting with a / and excluding any query parameters, of the web request that caused this version to be created.",
                request_user_agent: "The user agent of the web request that caused this version to be created. Allows a user's browser and operating system to be identified.",
                request_referer: "The URL of any page the user was viewing when they initiated the web request that caused this version to be created. This is the full URL, including protocol (https://) and any query parameters, if the browser shared these with our application as part of the web request. It is very common for this referer to be truncated for referrals from external sites.",
                request_query: "ARRAY of STRUCTs, each with a key and a value. Contains any query parameters that were sent to the application as part of the web request that caused this version to be created.",
                response_content_type: "Content type of any data that was returned to the browser following the web request that caused this version to be created. For example, 'text/html; charset=utf-8'. Image views, for example, may have a non-text/html content type.",
                response_status: "HTTP response code returned by the application in response to the web request that caused this version to be created. See https://developer.mozilla.org/en-US/docs/Web/HTTP/Status.",
                anonymised_user_agent_and_ip: "One way hash of a combination of the IP address and user agent of the user who made the web request that caused this version to be created. Can be used to identify the user anonymously, even when user_id is not set. Cannot be used to identify the user over a time period of longer than about a month, because of IP address changes and browser updates.",
                device_category: "The category of device that caused this version to be created - desktop, mobile, bot or unknown.",
                browser_name: "The name of the browser that caused this version to be created.",
                browser_version: "The version of the browser that caused this version to be created.",
                operating_system_name: "The name of the operating system that caused this version to be created.",
                operating_system_vendor: "The vendor of the operating system that caused this version to be created.",
                operating_system_version: "The version of the operating system that caused this version to be created."
            }
        }).query(ctx => `WITH entity_events AS (
  /* all entity events that have been streamed since we last ran this query, UNION ALLed with the latest events only for events that this query already processed in the past - this avoids having to process any unnecessary entity events later in the query */
  SELECT
    *
  FROM
    (
      SELECT
        event_type,
        occurred_at,
        entity_table_name,
        ${idField(params.dataSchema)} AS entity_id,
        ${data_functions.eventDataExtract("data", "created_at", false, "timestamp")} AS created_at,
        ${data_functions.eventDataExtract("data", "updated_at", false, "timestamp")} AS updated_at,
        DATA,
        request_uuid,
        request_path,
        request_user_id,
        request_method,
        request_user_agent,
        request_referer,
        request_query,
        response_content_type,
        response_status,
        anonymised_user_agent_and_ip,
        device_category,
        browser_name,
        browser_version,
        operating_system_name,
        operating_system_vendor,
        operating_system_version
      FROM
        ${ctx.ref("events_" + params.eventSourceName)}
      WHERE
        occurred_at > event_timestamp_checkpoint
        AND event_type IN (
          "create_entity",
          "update_entity",
          "delete_entity",
          "import_entity"
        )
        AND entity_table_name IS NOT NULL
        AND ${idField(params.dataSchema)} IS NOT NULL
    )
    ${ctx.when(ctx.incremental(),
    `UNION ALL (SELECT event_type, valid_from AS occurred_at, entity_table_name, entity_id, created_at, updated_at, DATA, request_uuid, request_path, request_user_id, request_method, request_user_agent, request_referer, request_query, response_content_type, response_status, anonymised_user_agent_and_ip, device_category, browser_name, browser_version, operating_system_name, operating_system_vendor, operating_system_version FROM ${ctx.self()} WHERE valid_to IS NULL AND valid_from <= event_timestamp_checkpoint)`)
    }
)
SELECT
  *
FROM
  (
    /* for each event, work out the valid_from and valid_to timestamps for the version of the entity it represents by working out the next value of occurred_at for this instance of this entity */
    SELECT
      occurred_at AS valid_from,
      FIRST_VALUE(occurred_at) OVER future_events_for_this_entity AS valid_to,
      event_type,
      entity_table_name,
      entity_id,
      created_at,
      updated_at,
      DATA,
      request_uuid,
      request_path,
      request_user_id,
      request_method,
      request_user_agent,
      request_referer,
      request_query,
      response_content_type,
      response_status,
      anonymised_user_agent_and_ip,
      device_category,
      browser_name,
      browser_version,
      operating_system_name,
      operating_system_vendor,
      operating_system_version
    FROM
      entity_events WINDOW future_events_for_this_entity AS (
        PARTITION BY entity_table_name,
        entity_id
        ORDER BY
          occurred_at ASC ROWS BETWEEN 1 FOLLOWING
          AND UNBOUNDED FOLLOWING
      )
  )
WHERE
  event_type != "delete_entity"
  AND (
    /* If we run a backfill job for entity events then occurred_at is set to the created_at date of the entity, not the timestamp when the import event happened. However this creates two entity versions at the same time, one of which is only valid for zero seconds. This excludes these from this table, ensuring that the combination of valid_from, entity_id and entity_table_name provides a unique identifier for an entity version. */
    valid_from != valid_to
    OR valid_to IS NULL
  )`).preOps(ctx => `DECLARE event_timestamp_checkpoint DEFAULT (
        ${ctx.when(ctx.incremental(), `SELECT MAX(valid_to) FROM ${ctx.self()}`, `SELECT TIMESTAMP("2018-01-01")`)}
      )`)
    .postOps(ctx => `${data_functions.setKeyConstraints(ctx, dataform, {
            primaryKey: "entity_id, valid_from, entity_table_name"
            })}
            /* On occasion there is no entity deletion event present in the events table for entities which have in fact been deleted from the application database.
            This UPDATE statement corrects this using import_entity events when it is possible to be certain that a table has been fully and accurately loaded from its post-import checksum.
            Entities with IDs which were not included in these imports but where the latest version in entity_version does not have a valid_to timestamp are assumed to have been deleted
            at the time that the checksum for the earliest complete import was calculated. */
            UPDATE
            ${ctx.self()} AS entity_version
            SET
            valid_to = apparently_deleted_before
            FROM (
            WITH
                complete_import AS (
                SELECT
                *
                FROM
                ${ctx.ref("entity_table_check_import_" + params.eventSourceName)} AS import
                WHERE
                database_checksum = bigquery_checksum
                AND ARRAY_LENGTH(import.imported_entity_ids) > 0)
            SELECT
                entity_version.entity_table_name,
                entity_version.entity_id AS id_that_was_apparently_deleted,
                MIN(import.checksum_calculated_at) AS apparently_deleted_before
            FROM
                ${ctx.self()} AS entity_version
            JOIN
                complete_import AS import
            ON
                import.entity_table_name = entity_version.entity_table_name
                AND import.checksum_calculated_at > entity_version.valid_from
            WHERE
                entity_version.valid_to IS NULL
                AND entity_version.entity_id NOT IN UNNEST(import.imported_entity_ids)
            GROUP BY
                entity_version.entity_table_name,
                entity_version.entity_id) AS apparently_deleted_entity
            WHERE
            entity_version.valid_to IS NULL
            AND entity_version.entity_table_name = apparently_deleted_entity.entity_table_name
            AND entity_version.entity_id = apparently_deleted_entity.id_that_was_apparently_deleted
    `)
}
