const validTopLevelParameters = ['eventSourceName',
    'bqEventsTableName',
    'urlRegex',
    'dataSchema',
    'customEventSchema',
    'bqProjectName',
    'bqDatasetName',
    'bqEventsTableNameSpace',
    'transformEntityEvents',
    'enableSessionTables',
    'enableMonitoring',
    'eventsDataFreshnessDays',
    'eventsDataFreshnessDisableDuringRange',
    'assertionDisableDuringDateRanges',
    'compareChecksums',
    'funnelDepth',
    'requestPathGroupingRegex',
    'dependencies',
    'attributionParameters',
    'attributionDomainExclusionRegex',
    'socialRefererDomainRegex',
    'searchEngineRefererDomainRegex',
    'disabled',
    'hiddenPolicyTagLocation'
];
const validDataSchemaTableParameters = ['entityTableName',
    'description',
    'keys',
    'primaryKey',
    'hidePrimaryKey',
    'dataFreshnessDays',
    'dataFreshnessDisableDuringRange',
    'materialisation'
];
const validCustomEventSchemaEventParameters = ['eventType',
    'description',
    'keys',
    'dataFreshnessDays',
    'dataFreshnessDisableDuringRange'
];
const validDataSchemaKeyParameters = ['keyName',
    'dataType',
    'isArray',
    'description',
    'alias',
    'pastKeyNames',
    'historic',
    'foreignKeyName',
    'foreignKeyTable',
    'hidden',
    'hiddenPolicyTagLocation'
];
const validCustomEventSchemaKeyParameters = ['keyName',
    'dataType',
    'isArray',
    'description',
    'alias',
    'pastKeyNames',
    'historic',
    'foreignKeyName',
    'foreignKeyTable',
    'hidden',
    'hiddenPolicyTagLocation'
];

const invalidCustomEventTypes = ['create_entity',
    'update_entity',
    'delete_entity',
    'import_entity',
    'initialise_analytics',
    'entity_table_check',
    'import_entity_table_check',
    'web_request'
];

function validateParams(params) {
    Object.keys(params).forEach(key => {
        if (!validTopLevelParameters.includes(key)) {
            throw new Error(`Invalid top level parameter passed to dfeAnalyticsDataform(): ${key}. Valid top level parameters are: ${validTopLevelParameters.sort().join(', ')}`);
        }
    });

    if (!/^[A-Za-z0-9_]*$/.test(params.eventSourceName)) {
        throw new Error(`eventSourceName ${params.eventSourceName} contains characters that are not alphanumeric or an underscore`);
    }

    // Loop through dataSchema to handle errors
    params.dataSchema.forEach(tableSchema => {
        Object.keys(tableSchema).forEach(param => {
            if (!validDataSchemaTableParameters.includes(param)) {
                throw new Error(`Invalid table level parameter in dataSchema passed to dfeAnalyticsDataform() for the ${tableSchema.entityTableName} table: ${param}. Valid table level parameters are: ${validDataSchemaTableParameters.sort().join(', ')}`);
            }
        });
        if (tableSchema.materialisation && tableSchema.materialisation != 'view' && tableSchema.materialisation != 'table') {
            throw new Error(`Value of materialisationType ${tableSchema.materialisation} for table ${tableSchema.entityTableName} in dataSchema must be either 'view' or 'table'.`);
        }
        if (tableSchema.primaryKey == "id") {
            throw new Error(`primaryKey for the ${tableSchema.entityTableName} table is set to 'id', which is the default value for primaryKey. If id is the primary key for this table in the database, remove the primaryKey configuration for this table in your dataSchema. If id is not the primary key for this table in the database, set primaryKey to the correct primary key.`);
        }
        if (tableSchema.hidePrimaryKey && !params.hiddenPolicyTagLocation) {
            throw new Error(`hiddenPolicyTagLocation not set at eventDataSource level even though hidePrimaryKey is ${tableSchema.hidePrimaryKey} for the ${tableSchema.entityTableName} table.`);
        }
        tableSchema.keys.forEach(key => {
            Object.keys(key).forEach(param => {
                if (!validDataSchemaKeyParameters.includes(param)) {
                    throw new Error(`Invalid field level parameter in dataSchema passed to dfeAnalyticsDataform() for the ${key.keyName} field in the ${tableSchema.entityTableName} table: ${param}. Valid field level parameters are: ${validDataSchemaKeyParameters.sort().join(', ')}`);
                }
            });
            if (key.dataType && !['boolean', 'timestamp', 'date', 'integer', 'integer_array', 'float', 'json', 'string'].includes(key.dataType)) {
                throw new Error(`Unrecognised dataType '${key.dataType}' for field '${key.keyName}'. dataType should be set to boolean, timestamp, date, integer, integer_array, float, json or string or not set.`);
            } else if (['id', 'created_at', 'updated_at'].includes(key.alias || key.keyName)) {
                throw new Error(`${key.keyName}' is included as a field in the ${tableSchema.entityTableName}_version_${params.eventSourceName} table generated by dfe-analytics-dataform automatically, so would produce a table with more than one column with the same name. Remove this field from your dataSchema to prevent this error. Or if you're sure that you want to include the same field more than once, use an alias by setting 'alias: "alternative_name_for_${key.keyName}"' for this field in your dataSchema.`);
            } else if (['valid_from', 'valid_to', 'event_type', 'request_uuid', 'request_path', 'request_user_id', 'request_method', 'request_user_agent', 'request_referer', 'request_query', 'response_content_type', 'response_status', 'anonymised_user_agent_and_ip', 'device_category', 'browser_name', 'browser_version', 'operating_system_name', 'operating_system_vendor', 'operating_system_version'].includes(key.alias || key.keyName)) {
                throw new Error(`'${key.keyName}' is the same as a field name in the ${tableSchema.entityTableName}_version_${params.eventSourceName} table generated by dfe-analytics-dataform, so would produce a table with two columns with the same name. Set 'alias: "alternative_name_for_${key.keyName}"' for this field in your dataSchema to prevent this error.`);
            } else if (['new_value', 'previous_value', 'key_updated', 'update_id', 'previous_occurred_at', 'seconds_since_previous_update', 'seconds_since_created', 'previous_event_type'].includes(key.alias || key.keyName)) {
                throw new Error(`'${key.keyName}' is the same as a field name in the ${tableSchema.entityTableName}_field_updates_${params.eventSourceName} table generated by dfe-analytics-dataform, so would produce a table with two columns with the same name. Set 'alias: "alternative_name_for_${key.keyName}"' for this field in your dataSchema to prevent this error.`);
            }
            if (key.hidden && !(key.hidden === true || key.hidden === false)) {
                throw new Error(`hidden for the ${key.keyName} field in the ${tableSchema.entityTableName} table is not a boolean value. Ensure it is set to true or false, and that it is not in quotes.`);
            }
            if (key.hidden && !(key.hiddenPolicyTagLocation || params.hiddenPolicyTagLocation)) {
                throw new Error(`hiddenPolicyTagLocation not set at either eventDataSource level or key level for the ${key.keyName} field in the ${tableSchema.entityTableName} table, even though hidden is ${key.hidden}`);
            }
            if ((key.keyName == tableSchema.primaryKey) && (key.hidden === true || key.hidden === false)) {
                throw new Error(`The ${key.keyName} field in the ${tableSchema.entityTableName} table has 'hidden' parameter set at field level even though it is the primary key. Set the 'hidePrimaryKey' parameter at table level for this table instead.`);
            }
        })
    });
    // Loop through customEventSchema to handle errors
    params.customEventSchema.forEach(customEvent => {
        if (invalidCustomEventTypes.includes(customEvent.eventType)) {
            throw new Error(`Custom event type ${customEvent.eventType} is an event type streamed by dfe-analytics by default, so it is not a custom event`);
        };
        Object.keys(customEvent).forEach(param => {
            if (!validCustomEventSchemaEventParameters.includes(param)) {
                throw new Error(`Invalid event level parameter in customEventSchema passed to dfeAnalyticsDataform() for the ${customEvent.eventType} custom event: ${param}. Valid event level parameters are: ${validCustomEventSchemaEventParameters.sort().join(', ')}`);
            }
        });
        customEvent.keys.forEach(key => {
            Object.keys(key).forEach(param => {
                if (!validCustomEventSchemaKeyParameters.includes(param)) {
                    throw new Error(`Invalid field level parameter in customEventSchema passed to dfeAnalyticsDataform() for the ${key.keyName} field in the ${customEvent.eventType} custom event: ${param}. Valid field level parameters are: ${validCustomEventSchemaKeyParameters.sort().join(', ')}`);
                }
            });
            if (key.dataType && !['boolean', 'timestamp', 'date', 'integer', 'integer_array', 'float', 'json', 'string'].includes(key.dataType)) {
                throw new Error(`Unrecognised dataType '${key.dataType}' for field '${key.keyName}'. dataType should be set to boolean, timestamp, date, integer, integer_array, float, json or string or not set.`);
            } else if (['occurred_at', 'request_uuid', 'request_path', 'request_user_id', 'request_method', 'request_user_agent', 'request_referer', 'request_query', 'response_content_type', 'response_status', 'anonymised_user_agent_and_ip', 'device_category', 'browser_name', 'browser_version', 'operating_system_name', 'operating_system_vendor', 'operating_system_version'].includes(key.alias || key.keyName)) {
                throw new Error(`'${key.keyName}' is the same as a field name in the ${customEvent.eventType}_${params.eventSourceName} table generated by dfe-analytics-dataform, so would produce a table with two columns with the same name. Set 'alias: "alternative_name_for_${key.keyName}"' for this field in your customEventSchema to prevent this error.`);
            }
            if (key.hidden && !(key.hidden === true || key.hidden === false)) {
                throw new Error(`hidden for the ${key.keyName} field in the ${customEvent.eventType} custom event is not a boolean value. Ensure it is set to true or false, and that it is not in quotes.`);
            }
            if (key.hidden && !(key.hiddenPolicyTagLocation || params.hiddenPolicyTagLocation)) {
                throw new Error(`hiddenPolicyTagLocation not set at either eventDataSource level or key level for the ${key.keyName} field in the ${customEvent.eventType} custom event, even though hidden is ${key.hidden}`);
            }
        })
    });
    return params;
}

function setDefaultSchemaParameters(params) {
    // Loop through dataSchema to set default values
    params.dataSchema.forEach(tableSchema => {
        // Set default value of materialisation to 'table' for all tables in dataSchema if not set explicitly
        if (!tableSchema.materialisation) {
            tableSchema.materialisation = 'table';
        }
        // Set default value of key level hiddenPolicyTagLocation to match event source level hiddenPolicyTagLocation if not set explicitly and the field is actually hidden
        tableSchema.keys.forEach(key => {
            if (key.hidden && !key.hiddenPolicyTagLocation) {
                key.hiddenPolicyTagLocation = params.hiddenPolicyTagLocation;
            }
            // If a key is the primary key and hidePrimaryKey is true, hide that key - otherwise just the copy of the primary key field that would be in the id field / entity_id field in various tables would be hidden
            if ((key.keyName == tableSchema.primaryKey) && tableSchema.hidePrimaryKey) {
                key.hidden = true;
                key.hiddenPolicyTagLocation = params.hiddenPolicyTagLocation;
            }
        });
    });
    params.customEventSchema.forEach(customEvent => {
        // Set default value of key level hiddenPolicyTagLocation to match event source level hiddenPolicyTagLocation if not set explicitly and the field is actually hidden
        customEvent.keys.forEach(key => {
            if (key.hidden && !key.hiddenPolicyTagLocation) {
                key.hiddenPolicyTagLocation = params.hiddenPolicyTagLocation;
            }
        });
    });
    return params;
}

function dateRangesToDisableAssertionsNow(ranges, currentDate) {
    let disableAssertionsNow = false;
    ranges.forEach(range => {
        if (range.fromDate instanceof Date && range.toDate instanceof Date) {
            // range is between two dates already so no further processing required
        } else if (Number.isInteger(range.fromMonth) && Number.isInteger(range.fromDay) && Number.isInteger(range.toMonth) && Number.isInteger(range.toDay)) {
            range.fromDate = new Date(currentDate.getFullYear(), range.fromMonth - 1, range.fromDay); //setMonth() takes January as month 0
            range.toDate = new Date(currentDate.getFullYear(), range.toMonth - 1, range.toDay); //setMonth() takes January as month 0
            if (range.toMonth < range.fromMonth || ((range.toMonth == range.fromMonth) && (range.toDay < range.fromDay))) {
                //If the days of the year are the wrong way round e.g. "From 10th Sept to 9th Sept" or "From 1st Aug to 1st May"
                if (range.fromDate >= currentDate) {
                    //If the from day of the year is in the future
                    range.fromDate.setFullYear(currentDate.getFullYear() - 1);
                } else if (range.fromDate < currentDate) {
                    //If the from day is in the past
                    range.toDate.setFullYear(currentDate.getFullYear() + 1);
                }

            }
        } else {
            throw new Error(`dateRangesToDisableAssertionsNow contains invalid range: ${JSON.stringify(range)}`);
        }
        if (range.toDate < range.fromDate) {
            throw new Error(`toDate is after fromDate in range: ${JSON.stringify(range)}. If you didn't specify these parameters then please make a bug report.`);
        } else if (range.fromDate <= currentDate && range.toDate >= currentDate) {
            disableAssertionsNow = true;
        }
    });

    return disableAssertionsNow;
}

module.exports = {
    validateParams,
    setDefaultSchemaParameters,
    dateRangesToDisableAssertionsNow
}
