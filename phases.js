// unpack scope functions to local scope, unfortunately with eval 
var scopeFunctions = require('./scope_functions');
for (var k in scopeFunctions) {
	eval('var ' + k + ' = scopeFunctions[k]');
}

var phases = 
{
	map: function() 
	{
		if (DEBUG) {
			print('map:_id = '+this._id);
		}
		var emitted = {count: 1};

		// get the combined emit key 
		var keySegments = [];
		for (var k in mappers) {
			// get the emit key from the Mapper's get() method
			// var f = mappers[k].map;
			var f = mappers_code[k]; 
				mapped = f.call(mappers[k], getAttr(this, k), this);
			
			// if no key is returned, do not emit.
			if (!mapped) return;

			if (isArray(mapped)) {
				// if the return value is an Array, the first element is the emit key content
				// and the second is an arbitrary value to be inserted into the data
				keySegments.push(mapped[0]);
				setAttr(emitted, k, mapped[1]);
			} else {
				// otherwise the the emit key content is inserted into the data
				keySegments.push(mapped);
				setAttr(emitted, k, mapped);
			}
		}
		var fullKey = keySegments.join(KEY_SEPARATOR);


		// call findExtremes to initialize each aggregate field 
		iterFields(aggregates, this, function(fieldName, doc) {
			// if field is already in emitted values (see above), use that value for extremes
			var value = getAttr(emitted, fieldName);
			if (value == undefined) {
				// otherwise use original value for extremes
				value = getAttr(doc, fieldName)
			}
			setAttr(emitted, fieldName, findExtremes(value));
		});

		if (events.emit) {
			events.emit(fullKey, emitted);
		}

		if (DEBUG) {
			print('emit:key = ' + fullKey);
		}
		emit(fullKey, emitted);
	},

	reduce: function(key, docs) 
	{
		// initialize return object
		var reduced = {count: 0};
		// copy all mapper values from first element
		for (var k in mappers) {
			setAttr(reduced, k, getAttr(docs[0], k));
		}

		docs.forEach(function(doc) {
			// add count from reduced document to total count
			reduced.count += doc.count;
			// call findExtremes over all aggregate values
			iterFields(aggregates, doc, function(fieldName, doc) {
				setAttr(reduced, fieldName, findExtremes(getAttr(doc, fieldName), getAttr(reduced, fieldName)));
			});
		});

		if (events.reduce) {
			events.reduce(key, reduced);
		}

		return reduced;
	},

	finalize: function(key, doc) 
	{
		// determine average
		iterFields(aggregates, doc, function(fieldName, doc) {
			var extremes = getAttr(doc, fieldName);
			// doc.extremes is modified by reference
			setStats(extremes);
		});

		if (events.finalize) {
			events.finalize(key, doc);
		}

		return doc;
	}
};

module.exports = phases;