/* With transformEntityEvents: false the legacy <entity>_version_<source> tables are no longer published,
   but the Airbyte legacy merge still ref()s them on full refresh. They still exist in BigQuery, frozen at
   their last build, so declare them. Entities with includeLegacyHistory: false are never referenced, so skipped.
   Declarations aren't suffixed, so dev workspaces read the frozen production tables. */

const parameterFunctions = require("./parameter_functions");

module.exports = (params) => {
    if (params.transformEntityEvents !== false || !params.enableAirbyteSource || params.enabledAirbyteLegacyMerge !== true) {
        return [];
    }

    return params.dataSchema
        .filter(tableSchema => parameterFunctions.airbyteLegacyMergeEnabledFor(params, tableSchema))
        .map(tableSchema => declare({
            name: `${tableSchema.entityTableName}_version_${params.eventSourceName}`
        }));
};