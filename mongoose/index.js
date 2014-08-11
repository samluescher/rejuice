var	Code = require('mongodb').Code,
	mongoose = require('mongoose'),
	mappers = require('../mappers'),
	Mapper = mappers.Mapper,
	util = require('../util'),
	scopeFunctions = require('../scope_functions'),
	phases = require('../phases');

var runMapReduce = function(collectionName, outputCollectionName, mappers, options, callback) 
{
	var options = options || {};

	var scope = {
		aggregates: !options.aggregates ? []
			: (Array.isArray(options.aggregates) ? options.aggregates : [options.aggregates]),
		mappers: mappers,
		mappers_code: {},
		events: {},
		KEY_SEPARATOR: mappers.KEY_SEPARATOR,
		DEBUG: false
	};

	// Pass all utility functions with the scope that is available to MongoDB
	// during the MapReduce operation.
	for (var scopeName in scopeFunctions) {
		scope[scopeName] = scopeFunctions[scopeName];
	}

	// Pass all utility functions with the scope that is available to MongoDB
	// during the MapReduce operation.
	// Since we can't pass functions to MongoDB directly, we need to convert
	// them to a Code object first.	
	for (var eventName in options.events || {}) {
		scope.events[eventName] = new Code(options.events[eventName]);
	}

	// Same goes for all Mapper get methods. 
	// Note that when these functions are called from the map() function, 
	// they are passed the Mapper object.
	for (var keyName in mappers) {
		scope.mappers_code[keyName] = new Code(mappers[keyName].map);
	}

	if (options.scope) {
		for (var k in options.scope) {
			scope[k] = options.scope[k];
		}
	}

	// Since we can't pass functions to MongoDB directly, we need to convert
	// them to a Code object first.	
	for (var scopeName in scope) {
		if (typeof scope[scopeName] == 'function') {
			scope[scopeName] = new Code(scope[scopeName]);
		}
	}

	var params = {
		mapreduce: collectionName
		,map: phases.map.toString()
		,reduce: phases.reduce.toString()
		,finalize: phases.finalize.toString()
		,out: {reduce: outputCollectionName}
		,scope: scope
		,verbose: true
		,keeptemp: true
	};

	var logOptions = [];
	for (k in options) {
		if (['query', 'sort', 'limit', 'jsMode'].indexOf(k) != -1) {
			params[k] = options[k];
			logOptions.push(k + ': ' + JSON.stringify(options[k]));
		} 
	}

	var info = [];
	for (var k in mappers) {
		info.push(mappers[k].name || k);
	}

	var db = mongoose.connection;

	/*db.collection(collectionName).count(params.query, function(err, totalCount) {
		console.log('  * running MapReduce for '+totalCount+' '+collectionName+' to '+outputCollectionName+' with key: '+info.join(' | '));*/
		console.info('  * running MapReduce for collection ' + collectionName + ' ==> ' + outputCollectionName);
		console.info('  * emit key: '+info.join(mappers.KEY_SEPARATOR));
		console.info('  * aggregate values: '+scope.aggregates.join(', '));
		console.info('  * options: '+logOptions.join(', '));
		console.info('  * executing MapReduce...');

		// params.scope contains hashes such as {'mappers': {'properties.value': 'foo'}} and Mongoose > 3.8.8
		// seems to check BSON keys in scope and throw an error if they contain periods. Rolling back to 3.8.3 fixes it.
		mongoose.connection.db.executeDbCommand(params, function(err, op) {
			if (err || (op.documents.length && op.documents[0].errmsg)) {
				if (!err) {
					err = new Error('MongoDB error: ' + op.documents[0].errmsg);
				}
				console.error('   * error during MapReduce', err)
			} else {

				console[console.success ? 'success' : 'info']('  * done. ensuring required indexes ...');
				for (var k in mappers) {
					if (mappers[k].index) {
						var fieldName = 'value.' + k;
						console.info('  * building key index for '+fieldName+' ...');
						if (!mappers[k].index.call(mappers[k], 
								db.collection(outputCollectionName), fieldName)) {
							console.error('ERROR: could not build index');
							//return false;
						}
					}
				}
				if (options.indexes) {
					for (var k in options.indexes) {
						var fieldName = 'value.' + k;
						console.info('  * building index for '+fieldName+' ...');
						var index = {};
						index[fieldName] = options.indexes[k];
						db.collection(outputCollectionName).ensureIndex(index, function(err) {
							if (err) {
								console.error('ERROR: could not build index', err);
								//return false;
							}
						});
					}
				}

				console[console.success ? 'success' : 'info']('  * MapReduce successful:', op.documents[0].counts);
				if (op.documents[0].timing) {
					delete op.documents[0].timing.mode;
					console.info('  * Timing:', op.documents[0].timing);
				} 
			}

			callback(err);
		});
	/*});*/
};

module.exports = {
	runMapReduce: runMapReduce,
	Mapper: Mapper,
	util: util,
	scopeFunctions: scopeFunctions
};