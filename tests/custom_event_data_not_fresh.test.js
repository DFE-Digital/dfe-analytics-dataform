const customEventDataNotFresh = require("../includes/custom_event_data_not_fresh");

describe("customEventDataNotFresh", () => {
  test("throws error for invalid dataFreshnessDays", () => {
    const params = {
      customEventSchema: [
        { eventType: "testEvent", dataFreshnessDays: -5 },
      ],
    };

    expect(() => customEventDataNotFresh(params)).toThrow(
      "dataFreshnessDays parameter for the testEvent eventType is not a positive integer."
    );
  });

  test("skips assertion when dataFreshnessDisableDuringRange is true and disableAssertionsNow is true", () => {
    const params = {
      customEventSchema: [
        {
          eventType: "testEvent",
          dataFreshnessDays: 5,
          dataFreshnessDisableDuringRange: true,
        },
      ],
      disableAssertionsNow: true,
    };

    const result = customEventDataNotFresh(params);

    expect(result).toBeUndefined(); // No assertion is generated
  });

  test("generates assertionfor valid custom event", () => {
    const params = {
      customEventSchema: [
        { eventType: "testEvent", dataFreshnessDays: 5 },
      ],
      eventSourceName: "testSource",
      defaultConfig: { someConfig: true },
    };

    let capturedQueryFn;

    const mockTags = jest.fn(() => ({
      query: fn => {
        capturedQueryFn = fn;
      }
    }));

    const mockAssert = jest.fn(() => ({
      tags: mockTags
    }));

    global.assert = mockAssert;

    customEventDataNotFresh(params);

    const mockCtx = {
      ref: tableName => `\`${tableName}\``
    };

    const sql = capturedQueryFn(mockCtx);

    expect(mockAssert).toHaveBeenCalledWith(
      "testEvent_custom_event_data_not_fresh_testSource",
      { someConfig: true }
    );

    expect(mockTags).toHaveBeenCalledWith(["testsource"]);

    expect(sql).toMatch(/SELECT MAX\(occurred_at\)/);
    expect(sql).toContain("FROM `testEvent_testSource`");
    expect(sql).toContain("INTERVAL 5 DAY");
  });
});