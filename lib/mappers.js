// unpack scope functions to local scope, unfortunately with eval
var scopeFunctions = require('./scope_functions');
for (var k in scopeFunctions) {
	eval('var ' + k + ' = scopeFunctions[k]');
}

var Mapper =
{
	Copy: function()
	{
		this.map = function(value) {
			return value;
		};
		return this;
	},

	Time: {

		Daily: function(t)
		{
			this.map = function(t) {
				var t = new Date(t);
				return [
					t.getFullYear()+'-'+lpad(t.getMonth(), '0', 2)+'-'+lpad(t.getUTCDate(), '0', 2),
					new Date(t.getFullYear(), t.getMonth(), t.getUTCDate())
				];
			};
			this.name = 'daily';
			this.index = function(collection, field_name) {
				var index = {};
				index[field_name] = 1;
				collection.ensureIndex(index, function() {});
				return true;
			};
			return this;
		},

		Weekly: function(t)
		{
			this.map = function(t) {
				var t = new Date(t);
				var week = getWeek(t, 1);
				var day = t.getDay(),
			      diff = t.getDate() - day + (day == 0 ? -6 : 1);
				t.setDate(diff);
				return [
					t.getFullYear() + '-' + lpad(week, '0', 2),
					new Date(t.getFullYear(), t.getMonth(), t.getUTCDate())
				];
			};
			this.name = 'weekly';
			this.index = function(collection, field_name) {
				var index = {};
				index[field_name] = 1;
				collection.ensureIndex(index, function() {});
				return true;
			};
			return this;
		},

		Yearly: function(t)
		{
			this.map = function(t) {
				var t = new Date(t);
				return [
					t.getFullYear(),
					new Date(t.getFullYear(), 0, 1)
				];
			};
			this.name = 'yearly';
			this.index = function(collection, field_name) {
				var index = {};
				index[field_name] = 1;
				collection.ensureIndex(index, function() {});
				return true;
			};
			return this;
		}

	},

	Tile: {

		Rect: function(gridW, gridH, options)
		{
			var opts = options || {index: true};
			this.gridW = gridW;
			this.gridH = gridH;
			this.gridHW = gridW ? gridW / 2 : 0;
			this.gridHH = gridH ? gridH / 2 : 0;
			var name = gridW != gridH ?
				(gridW != undefined ? gridW : 'x') + '*' + (gridH != undefined ? gridH : 'y')
				: (gridW != undefined ? gridW + '' : '')
			this.prefix = /*name + ':'*/'';

			this.map = function(geometry) {
				if (!geometry) return;

				/* returns
					{
						[gridWest, gridSouth],
						[[realWest, realSouth], [realEast, realNorth]]
					}
				*/
				var _getTileCoords = function(coordinates)
				{
					var gW = this.gridW,
						gH = this.gridH,
						w = coordinates[0], e = w,
						s = coordinates[1], n = s,
						gX = w, gY = s;

					if (gW != undefined) {
						// west of tile
						gX = Math.floor(w / gW);
						w = gX * gW;
						// east of tile
						e = w + gW;
					}
					if (gH != undefined) {
						// south of tile
						gY = Math.floor(s / gH);
						s = gY * gH;
						// north of tile
						n = s + gH;
					}

					return [
						[gX, gY],
						[[w, s], [e, n]],
					];
				};

				if (geometry.coordinates) {
					// geometry is GeoJSON

					var returnType, c;
					switch (geometry.type) {
						case 'LineString':
							returnType = geometry.coordinates.length == 2 ?
								'LineString': 'Polygon';
							break;
						case 'Point':
							returnType = 'Point';
							break;
						default:
							returnType = 'Polygon';
					}


					switch (returnType) {

						case 'LineString':
							var isLine = geometry.coordinates.length;
							var p0 = geometry.coordinates[0],
								p1 = geometry.coordinates[geometry.coordinates.length - 1];
								c0 = _getTileCoords.call(this, p0),
								c1 = _getTileCoords.call(this, p1),
								start = [c0[1][0][0] + this.gridHW, c0[1][0][1] + this.gridHH],
								end = [c1[1][1][0] - this.gridHW, c1[1][1][1] - this.gridHH];
							if (Math.abs(start[0] - end[0]) >= this.gridW || Math.abs(start[1] - end[1]) > this.gridH) {
								// if start in same tile as end, return LineString from center of start rect
								// to center of end rect
								return [this.prefix + c0[0] + ',' + c1[0], {
									type: 'LineString',
									coordinates: [coordinates2d(start), coordinates2d(end)]
								}];
							}
							// if start == end, treat as Point since MongoDB would throw an error
							// for LineString with two identical coordinate pairs
							c = _getTileCoords.call(this, geometry.coordinates[0]);

						case 'Point':
							// for points, return Point at center of rect
							if (!c) {
								c = _getTileCoords.call(this, geometry.coordinates);
							}
							return [this.prefix + c[0], {
								type: 'Point',
								coordinates:
									coordinates2d([c[1][0][0] + this.gridHW, c[1][0][1] + this.gridHH])
							}];

						case 'Polygon':
							var bounds = getBounds(geometry.coordinates);
								csw = _getTileCoords.call(this, bounds[0]),
								cne = _getTileCoords.call(this, bounds[1]);

							return [this.prefix + csw[0] + ',' + cne[0], {
								type: 'Polygon',
								coordinates: [[
									coordinates2d(csw[1][0]),
									coordinates2d([cne[1][1][0], csw[1][0][1]]),
									coordinates2d(cne[1][1]),
									coordinates2d([csw[1][0][0], cne[1][1][1]]),
									coordinates2d(csw[1][0])
								]]
							}];
					} // switch
				}
			};

			this.name = 'tile_rect_' + name;

			if (opts.index) {
				this.index = function(collection, field_name) {
					var index = {};
					index[field_name] = '2d';
					collection.ensureIndex(index, function() {});
					return true;
				};
			}

			return this;
		}
	},

	Histogram: function(min, max, numBins, binSize)
	{
		this.numBins = numBins;
		this.maxBin = numBins - 1;
		this.pad = (this.maxBin + '').length;
		this.binSize = binSize !== undefined ? binSize : (max - min) / numBins;
		this.max = max;
		this.min = min;
		this.map = function(val) {
			if (typeof val != 'number') return;
			var normVal = (val - this.min) / (this.max - this.min),
				// due to the way Javascript handles floats, floatBin may be
				// a number like 2.9999999999999987:
				floatBin = normVal * this.numBins,
				// converting it to fixed point will result in 3.0000...,
				// the value we would have expected:
				fixedBin = Number(floatBin).toFixed(10),
				bin = Math.max(0, Math.min(this.maxBin, Math.floor(fixedBin)));
			return [
				// padding the bin number will make it sortable if stored as
				// string, for example if used Mongoose _id:
				lpad(bin, '0', this.pad),
				this.min + bin * this.binSize
			];
		};
		this.name = 'histogram_' + numBins;
	}

};

module.exports = {
	Mapper: Mapper,
	// the functions that need to be in the scope of the Mapper get() methods,
	// apart from the Mapper object itself, this.
	KEY_SEPARATOR: '|'
};
