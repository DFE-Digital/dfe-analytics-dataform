module.exports = (params) => {
  return params.dataSchema
    // Only generate assertions for tables which have at least one foreignKeyTable parameter configured for one of the fields (keys) in the table,
    // and when referential integrity checks are enabled at either key or eventDataSource level
    .filter(tableSchema => tableSchema.keys.some(key => key.foreignKeyTable && (key.checkReferentialIntegrity || params.checkReferentialIntegrity)))
    // Generate a separate assertion with a different name for each entityname_latest_eventsourcename table called something like entityname_foreign_key_lacks_referential_integrity_eventsourcename
    .forEach(tableSchema =>
      assert(
        tableSchema.entityTableName
        + "_foreign_key_lacks_referential_integrity_"
        + params.eventSourceName,
        {
          ...params.defaultConfig
        }
      ).tags([params.eventSourceName.toLowerCase()])
        .query(ctx =>
          "SELECT * FROM\n"
          // Generate a separate subquery for each foreign key in the table that a referential integrity check is configured for
          + tableSchema.keys
            // Only generate these subqueries for keys in the table that have the foreignKeyTable parameter set and when referential integrity checks are enabled at either key or eventDataSource level
            .filter(key => key.foreignKeyTable && (key.checkReferentialIntegrity || params.checkReferentialIntegrity))
            .map(
              key =>
                "  (SELECT '"
                // Generate a human readable error message as the first column if the assertion fails
                + (key.alias || key.keyName)
                + " does not match any "
                // If foreignKeyTable is present but foreignKeyName is not, default to foreignKeyName = id
                + (key.foreignKeyName || "id")
                + " in "
                + key.foreignKeyTable
                + "_latest_"
                + params.eventSourceName
                + "' AS issue_description, id AS "
                + tableSchema.entityTableName
                + "_id, created_at AS "
                + tableSchema.entityTableName
                + "_created_at, updated_at AS "
                + tableSchema.entityTableName
                + "_updated_at, "
                + (key.alias || key.keyName)
                + " AS missing_id_in_related_table FROM "
                + ctx.ref(tableSchema.entityTableName + "_latest_" + params.eventSourceName)
                + " WHERE "
                + (key.alias || key.keyName)
                + " NOT IN (SELECT "
                + (key.foreignKeyName || "id")
                + " FROM "
                + ctx.ref(key.foreignKeyTable + "_latest_" + params.eventSourceName)
                + "))"
            )
            // UNION ALL the collection of subqueries together - this will mean that if a single instance of an entity has referential integrity failures on more than one foreign key, one row will be generated per foreign key it has a failure for
            .join('\nUNION ALL\n')
          + "\nORDER BY\n  issue_description ASC,\n  "
          + tableSchema.entityTableName
          + "_updated_at DESC"
        ))
}