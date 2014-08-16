var coordinates = require('geogoose').coordinates,
	transmeta = require('transmeta'),
	getBounds = coordinates.getBounds,
	util = require('./util');

/*
The following code can both be executed by Node and MongoDB during the MapReduce
operation. The latter needs to be passed a scope object containing all the 
utility functions that are used by the code below as MongoDB Code objects. 
This module bundles the relevant functions for this purpose. 
*/

module.exports = {
	lpad: util.lpad,
	getBounds: coordinates.getBounds,
	bboxFromBounds: coordinates.bboxFromBounds,
	coordinates2d: coordinates.coordinates2d,
	overflow: coordinates.overflow,
	getWeek: util.getWeek,
	getAttr: transmeta.util.getAttr,
	setAttr: transmeta.util.setAttr,
	isArray: util.isArray,
	findExtremes: util.findExtremes,
	setStats: util.setStats,
	arrayMap: util.arrayMap,
	arrayReduce: util.arrayReduce,
	iterFields: util.iterFields,
};

