var	abstraction = require('../'),
	Mapper = abstraction.Mapper,
	findExtremes = abstraction.util.findExtremes,
	assert = require('assert');

describe('Geometry', function() {

	// TODO: add more tests, but based on GeoJSON geometry only

	/*it('should return the correct emit key and bounding box given [x,y]', function() {
		assert.deepEqual(new Mapper.Tile.Rect(2.5, 2.5).map([1.0, 2.6]), ["2.5:0,1", [ [0,2.5],[2.5,5] ]]);
	});

	it('should return the correct emit key and bounding box given [x,y] and a non-square tile size', function() {
		assert.deepEqual(new Mapper.Tile.Rect(2.5, .5).map([1.0, 2.6]), ["2.5*0.5:0,5", [ [0,2.5],[2.5,3] ]]);
	});

	it('should return the correct emit key and bounding box given a bounding box [[w,n],[e,s]]', function() {
		assert.deepEqual(new Mapper.Tile.Rect(2.5, 2.5).map([ [7.5,2.7], [5.0,2.6] ]   ), ["2.5:2,1,3,1", [ [5,2.5],[10,5] ]]);
	});*/

	var rect = new Mapper.Tile.Rect(3,2);

	it('should return a Point for the center of the tile for original Point', function() {
		var emit = rect.map({
			type: 'Point',
			coordinates: [11,12]
		});
		assert.deepEqual(emit, ["3,6",{"type":"Point","coordinates":[10.5,13]}]);
	});

	it('should return a LineString from start to end for original LineString', function() {
		emit = rect.map({
			type: 'LineString',
			coordinates: [[15,16], [11,12]]
		});
		assert.deepEqual(emit, ["5,8,3,6",{"type":"LineString","coordinates":[[16.5,17],[10.5,13]]}]);
	});

	it('should return the reversed LineString original reversed LineString', function() {
		emit = rect.map({
			type: 'LineString',
			coordinates: [[11,12], [15,16]]
		});
		assert.deepEqual(emit, ["3,6,5,8",{"type":"LineString","coordinates":[[10.5,13],[16.5,17]]}]);
	});

	it('should return a Polygon for original closed LineString', function() {
		emit = rect.map({
			type: 'LineString',
			coordinates: [[11,12], [15,16], [11,12]]
		});
		assert.deepEqual(emit, ["3,6,5,8",{"type":"Polygon","coordinates":[[[9,12],[18,12],[18,18],[9,18],[9,12]]]}]);
	});

	it('should return a Polygon for original Polygon', function() {
		emit = rect.map({
			type: 'Polygon',
			coordinates: [[[11,12], [15,16]]]
		});
		assert.deepEqual(emit, ["3,6,5,8",{"type":"Polygon","coordinates":[[[9,12],[18,12],[18,18],[9,18],[9,12]]]}]);
	});

	it('should return the same Polygon for original reversed Polygon', function() {
		emit = rect.map({
			type: 'Polygon',
			coordinates: [[[15,16], [11,12]]]
		});
		assert.deepEqual(emit, ["3,6,5,8",{"type":"Polygon","coordinates":[[[9,12],[18,12],[18,18],[9,18],[9,12]]]}]);
	});

	it('should map coordinates to a grid cell for all 4 hemisphere cases', function() {
		var gw = 0.005149841307876887, ghw = gw / 2,
			tile = new Mapper.Tile.Rect(gw, gw),
			c,
			r;

		// test with arbitrary coordinates for all four hemisphere cases

		c = [-122.296831666667, 47.5767516666667];
		r = tile.map({
				type: 'Point',
				coordinates: c
			})[1].coordinates;
		assert(c[0] >= r[0] - ghw && c[0] <= r[0] + ghw, c[1] >= r[1] - ghw && c[1] <= r[1] + ghw);

		c = [114.18631, 22.592925];
		r = tile.map({
				type: 'Point',
				coordinates: c
			})[1].coordinates;
		assert(c[0] >= r[0] - ghw && c[0] <= r[0] + ghw, c[1] >= r[1] - ghw && c[1] <= r[1] + ghw);

		c = [122.296831666667, -47.5767516666667];
		r = tile.map({
				type: 'Point',
				coordinates: c
			})[1].coordinates;
		assert(c[0] >= r[0] - ghw && c[0] <= r[0] + ghw, c[1] >= r[1] - ghw && c[1] <= r[1] + ghw);

		c = [-114.18631, -22.592925];
		r = tile.map({
				type: 'Point',
				coordinates: c
			})[1].coordinates;
		assert(c[0] >= r[0] - ghw && c[0] <= r[0] + ghw, c[1] >= r[1] - ghw && c[1] <= r[1] + ghw);

	});

});
