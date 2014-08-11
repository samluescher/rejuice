var	abstraction = require('../'),
	Mapper = abstraction.Mapper,
	findExtremes = abstraction.util.findExtremes,
	assert = require('assert');

describe('Histogram', function() {

	it('should create a histogram', function() {
		var histogram = new Mapper.Histogram(0, 100, 3),
			values = [0, 10, 30.329999, 33.34, 50, 75, 80, 85, 100],
			reduced;

		reduced = values.map(function(value) {
			var mapped = histogram.map(value);
			return {
				bin: mapped[0],
				count: 1,
				value: mapped[1]
			};
		}).reduce(function(previous, current) {
			if (!previous[current.bin]) {
				previous[current.bin] = {count: current.count};
			} else {
				previous[current.bin].count += current.count;
			}
			previous[current.bin].value = findExtremes(current.value, previous[current.bin].value);
			return previous;
		}, {});

		assert.equal(3, reduced[0].count);
		assert.equal(2, reduced[1].count);
		assert.equal(4, reduced[2].count);
	});

});


