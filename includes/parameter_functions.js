function dateRangesToDisableAssertionsNow(ranges, currentDate) {
    let disableAssertionsNow = false;
    ranges.forEach(range => {
        if (range.fromDate instanceof Date && range.toDate instanceof Date) {
            // range is between two dates already so no further processing required
        }
        else if (Number.isInteger(range.fromMonth) && Number.isInteger(range.fromDay) && Number.isInteger(range.toMonth) && Number.isInteger(range.toDay)) {
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
        }
        else {
            throw new Error(`dateRangesToDisableAssertionsNow contains invalid range: ${JSON.stringify(range)}`);
        }
        if (range.toDate < range.fromDate) {
            throw new Error(`toDate is after fromDate in range: ${JSON.stringify(range)}. If you didn't specify these parameters then please make a bug report.`);
        }
        else if (range.fromDate <= currentDate && range.toDate >= currentDate) {
            disableAssertionsNow = true;
        }
    });

    return disableAssertionsNow;
}

function test(description, fn) {
    try {
        fn();
    } catch (error) {
        throw new Error(`❌ Test Failed: ${description}. Error was '${error}'`);
    }
}

function tests() {
    const ranges = [
        { fromMonth: 7, fromDay: 25, toMonth: 9, toDay: 1 },
        { fromMonth: 3, fromDay: 29, toMonth: 4, toDay: 14 },
        { fromMonth: 12, fromDay: 22, toMonth: 1, toDay: 7 }
    ];

    test(`When it is New Year's Day disableAssertionsNow should be true`, () => {
        const currentDate = new Date('2024-01-01');
        if (!dateRangesToDisableAssertionsNow(ranges, currentDate)) {
            throw new Error(`Expected date ${currentDate} be within the ranges ${JSON.stringify(ranges)} but it wasn't.`);
        }
    });

    test(`When it is 8th January disableAssertionsNow should be false`, () => {
        const currentDate = new Date('2024-01-08');
        if (dateRangesToDisableAssertionsNow(ranges, currentDate)) {
            throw new Error(`Expected date ${currentDate} be outside the ranges ${JSON.stringify(ranges)} but it wasn't.`);
        }
    });

    test(`When it is 15th August disableAssertionsNow should be true`, () => {
        const currentDate = new Date('2024-08-15');
        if (!dateRangesToDisableAssertionsNow(ranges, currentDate)) {
            throw new Error(`Expected date ${currentDate} be within the ranges ${JSON.stringify(ranges)} but it wasn't.`);
        }
    });

    test(`When it is 30th December disableAssertionsNow should be true`, () => {
        const currentDate = new Date('2024-12-30');
        if (!dateRangesToDisableAssertionsNow(ranges, currentDate)) {
            throw new Error(`Expected date ${currentDate} be within the ranges ${JSON.stringify(ranges)} but it wasn't.`);
        }
    });
    
    test(`When it is 29th February in a leap year disableAssertionsNow should be false`, () => {
        const currentDate = new Date('2024-02-29');
        if (dateRangesToDisableAssertionsNow(ranges, currentDate)) {
            throw new Error(`Expected date ${currentDate} be within the ranges ${JSON.stringify(ranges)} but it wasn't.`);
        }
    });

}
module.exports = {
    dateRangesToDisableAssertionsNow,
    tests
}