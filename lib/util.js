/*
Iterates over the fields listed in fieldNames in obj, and calls the iterator
for each field, passing it the field name and the original object.
Listed field names may end with a wildcard, for instance "foo.bar.*",
in which case the iterator would be called for each individual element of bar.
*/
var iterFields = function(fieldNames, obj, iterator) {
    arrayMap.call(fieldNames, function(fieldName) {
        if (fieldName.substr(-2) == '.*') {
            var fieldName = fieldName.substr(0, fieldName.length - 2);
                el = getAttr(obj, fieldName);
            for (var key in el) {
                iterator(fieldName + '.' + key, obj);
            }
        } else {
            iterator(fieldName, obj);
        }
    });
};

/*
Earlier MongoDB does not have Array.isArray, hence "polyfill" this function.
*/
var isArray = function (v) {
    if (Array.isArray) return Array.isArray(v);
    return v && typeof v === 'object' && typeof v.length === 'number' && !(v.propertyIsEnumerable('length'));
}

/*
Earlier MongoDB does not have Array.map, hence "polyfill" this function.
*/
var arrayMap = function(iterator) {
    if (Array.prototype.map) return Array.prototype.map.call(this, iterator);
    var arr = this,
        result = [];
    for (var i = 0; i < arr.length; i++) {
        result.push(iterator(arr[i]));
    }
    return result;
}

/*
Earlier MongoDB does not have Array.reduce, hence "polyfill" this function.
*/
var arrayReduce = function(iterator, initial) {
    if (Array.prototype.reduce) return Array.prototype.reduce.call(this, iterator, initial);
    var arr = this,
        current = initial;
    for (var i = 0; i < arr.length; i++) {
        current = iterator(current, arr[i], i);
    }
    return current;
}

var lpad = function(str, padString, length) {
    var s = new String(str);
    while (s.length < length) {
        s = padString + s;
    }
    return s;
};

// Returns the week number for this date.  dowOffset is the day of week the week.
// "starts" on for your locale - it can be from 0 to 6. If dowOffset is 1 (Monday),
// the week returned is the ISO 8601 week number.
// @param int dowOffset
// @return int
var getWeek = function(date, dowOffset) {
    dowOffset = typeof(dowOffset) == 'int' ? dowOffset : 0; //default dowOffset to zero
    var newYear = new Date(date.getFullYear(),0,1);
    var day = newYear.getDay() - dowOffset; //the day of week the year begins on
    day = (day >= 0 ? day : day + 7);
    var daynum = Math.floor((date.getTime() - newYear.getTime() -
        (date.getTimezoneOffset()-newYear.getTimezoneOffset()) * 60000) / 86400000) + 1;
    var weeknum;
    //if the year starts before the middle of a week
    if(day < 4) {
        weeknum = Math.floor((daynum+day-1)/7) + 1;
        if(weeknum > 52) {
            nYear = new Date(date.getFullYear() + 1,0,1);
            nday = nYear.getDay() - dowOffset;
            nday = nday >= 0 ? nday : nday + 7;
            /*if the next year starts before the middle of
              the week, it is week #1 of that year*/
            weeknum = nday < 4 ? 1 : 53;
        }
    }
    else {
        weeknum = Math.floor((daynum+day-1)/7);
    }
    return weeknum;
};

/**
 * Destructively finds the extremes in an array of values, and returns
 * an extremes object containing min, max, count, sum, diff (for numbers) or
 * min, max, count (for other types).
 *
 * If passed an extremes object as second parameter, the merged extremes will
 * be returned.
 *
 * You can pass the extremes object returned by this function to setStats(),
 * which will set average, variance and standard deviation.
 */
var findExtremes = function(value, previous) {
    var map = function(el) {
            if (el && typeof el == 'object' &&
                (el.min != undefined || el.max != undefined)) return el;
            if (typeof el != 'number') {
                return {
                    min: el,
                    max: el,
                    count: 1
                }
            } else {
                return {
                    min: el,
                    max: el,
                    count: 1,
                    sum: el,
                    diff: 0
                }
            }
        },
        arr = isArray(value) ? value : [value],
        reduce = function(a, b) {
            if (!a) return b;
            if (typeof b.sum != 'number') {
                delete a.sum;
                delete a.diff;
            } else {
                // inspired by https://gist.github.com/RedBeard0531/1886960:
                var delta = a.sum / a.count - b.sum / b.count; // a.mean - b.mean
                var weight = (a.count * b.count) / (a.count + b.count);
                a.diff += b.diff + delta * delta * weight;
                a.sum += b.sum;
            }
            a.min = a.min == undefined || b.min < a.min ? b.min : a.min;
            a.max = a.max == undefined || b.max > a.max ? b.max : a.max;
            a.count = isNaN(a.count) ? b.count : a.count + b.count;
            return a;
        };

    //return arr.map(map).reduce(reduce, previous || {});
    return arrayReduce.call(arrayMap.call(arr, map), reduce, previous || null);
};

/**
 * Destructively sets average, variance and standard deviation on an object containing
 * extremes as determined by findExtremes.
 */
var setStats = function(extremes) {
    if (typeof extremes.sum == 'number') {
        extremes.avg = extremes.sum / extremes.count;
        extremes.variance = extremes.diff / extremes.count;
        extremes.stddev = Math.sqrt(extremes.variance);
    } else {
        delete extremes.avg;
        delete extremes.variance;
        delete extremes.stddev;
    }
    return extremes;
}

/**
 * Returns a "good" number of bins for a range of values between min and max,
 * where the resulting number of bins is in the same order of magnitute, but
 * optimized for more readable fractions and less fragmentation where there
 * are no continous numbers.
 */
var getHistogramDimensions = (function(min, max, desiredBins, isInteger) {
    var delta = max - min,
        deltaOrderOfMagniture = Math.max(1, Math.round(Math.log10(delta))),
        desiredBinsOrderOfMagnitude = Math.max(1, Math.round(Math.log10(desiredBins))),
        numBins,
        binSize;

    if (isInteger && deltaOrderOfMagniture < desiredBinsOrderOfMagnitude) {
        // for integers, it doesn't make sense to blow of the number of
        // bins past the range of possible values
        numBins = delta + 1;
        binSize = 1;
    } else {
        numBins = Math.max(1, Math.round(
		    (delta) / Math.pow(10, deltaOrderOfMagniture)
            * desiredBins));
        binSize = (max - min) / numBins;
    }

    return {
        min: min,
        max: max,
        numBins: numBins,
        binSize: binSize
    };
});

module.exports = {
    iterFields: iterFields,
    isArray: isArray,
    arrayMap: arrayMap,
    arrayReduce: arrayReduce,
    lpad: lpad,
    getWeek: getWeek,
    findExtremes: findExtremes,
    setStats: setStats,
    getHistogramDimensions: getHistogramDimensions
};
