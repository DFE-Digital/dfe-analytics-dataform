// TODO: import files from includes
// const file_one = require("./includes/file_one");
// const file_two = require("./includes/file_two");

const dataFunctions = require("./includes/data_functions");
const testTable1 = require ("./includes/test_table_counting_apply_events_today");
const testTable2 = require ("./includes/test_table_counting_apply_events_today2");

module.exports = (params) => {

    params = {
      // TODO: set default params

      ...params
    };

    // Publish and return datasets.
    let result = {
        testTable1: testTable1(params),
        testTable2: testTable2(params)
    // TODO: update files to call with params
    //   file_one: file_one(params),
    //   file_two: file_two(params)
    };

    return result;
}
