var coordinates = require('../../../geogoose/').coordinates,
	getBounds = coordinates.getBounds,
	utils = require('../../../utils'),
	findExtremes = utils.findExtremes,
	setStats = utils.setStats,
	util = require('../../import/data_transform/util'),
	getAttr = util.getAttr, setAttr = util.setAttr;

/*
The following code can both be executed by Node and MongoDB during the MapReduce
operation. The latter needs to be passed a scope object containing all the 
utility functions that are used by the code below as MongoDB Code objects. 
The module exports an object called scopeFunctions for this purpose. 
*/

var dump = function(obj) {
	var _dump = function(obj, indent) {
		var str = '';
		for (var k in obj) {
			for (var i = 0; i < indent * 2; i++) {
				str += ' ';
			}
			var d = '';
			if (obj[k] == null) {
				d = k + ': null\n';
			} else if (typeof obj[k] == 'function') {
				d = k + ': [Function]\n';
			} else if (typeof obj[k] == 'object') {
				if (obj[k].isObjectId) {
					d = k + ': ' + obj[k].str + '\n';
				} else {
					d = k + ':\n' + _dump(obj[k], indent + 1);
				}
			} else {
				d = k + ': ' + obj[k] + '\n';
			}
			str += d;
		}
		return str;
	};
	return _dump(obj, 0);
};


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
MongoDB does not have Array.isArray, hence this function.
*/
isArray = function (v) {
  return v && typeof v === 'object' && typeof v.length === 'number' && !(v.propertyIsEnumerable('length'));
}

/*
MongoDB does not have Array.map, hence this function.
*/
var arrayMap = function(iterator) {
	var arr = this,
		result = [];
	for (var i = 0; i < arr.length; i++) {
		result.push(iterator(arr[i]));
	}
	return result;
}

/*
MongoDB does not have Array.reduce, hence this function.
*/
var arrayReduce = function(iterator, initial) {
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

var EmitKey = 
{
	Copy: function() 
	{
		this.get = function(value) {
			return value;
		};
		return this;
	},

	Time: {

		Daily: function(t) 
		{
			this.get = function(t) {
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
				//return (utils.collectionHasIndex(collection, index));
				return true;
			};
			return this;
		},

		Weekly: function(t) 
		{
			this.get = function(t) {
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
				//return (utils.collectionHasIndex(collection, index));
				return true;
			};
			return this;
		},

		Yearly: function(t) 
		{
			this.get = function(t) {
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
				//return (utils.collectionHasIndex(collection, index));
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

			this.get = function(geometry) {
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
						gX = Math.round((w - w % gW) / gW); 
						w = gX * gW; 
						// east of tile
						e = w + gW;
					}
					if (gH != undefined) {
						// south of tile
						gY = Math.round((s - s % gH) / gH); 
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
									coordinates: [start, end]
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
								coordinates: [
									c[1][0][0] + this.gridHW, c[1][0][1] + this.gridHH
								]
							}];

						case 'Polygon':
							var bounds = getBounds(geometry.coordinates);
								csw = _getTileCoords.call(this, bounds[0]),
								cne = _getTileCoords.call(this, bounds[1]);

							return [this.prefix + csw[0] + ',' + cne[0], { 
								type: 'Polygon', 
								coordinates: [[
									csw[1][0], 
									[cne[1][1][0], csw[1][0][1]],
									cne[1][1],
									[csw[1][0][0], cne[1][1][1]],
									csw[1][0]
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
					//return (utils.collectionHasIndex(collection, index));
					return true;
				};
			}

			return this;
		}
	},

	Histogram: function(min, max, steps) 
	{
		this.step = (max - min) / steps;
		this.steps = steps;
		this.min = min - min % this.step;
		this.max = max - max % this.step;
		this.get = function(val) {
			var stepVal = val - val % this.step;
			return [
				stepVal, 
				{x: Math.round((stepVal - this.min) / this.step)}
			];
		};
		this.name = 'histogram_'+steps;
	}

};

module.exports = {
	EmitKey: EmitKey,
	// the functions that need to be in the scope of the EmitKey get() methods,
	// apart from the EmitKey object itself, this.
	scopeFunctions: {
		lpad: lpad,
		getBounds: coordinates.getBounds,
		bboxFromBounds: coordinates.bboxFromBounds,
		getWeek: getWeek,
		getAttr: getAttr,
		setAttr: setAttr,
		isArray: isArray,
		findExtremes: findExtremes,
		setStats: setStats,
		arrayMap: arrayMap,
		arrayReduce: arrayReduce,
		iterFields: iterFields,
		dump: dump
	},
	KEY_SEPARATOR: '|'
};
