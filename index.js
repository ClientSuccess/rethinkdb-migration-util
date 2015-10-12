var r = require('rethinkdb');
var R = require('ramda');
var util = require('util');

var Promise = require('bluebird');
var cert = require('fs').readFileSync('./certFile');

var local = R.always({
	host: 'localhost',
	port: 28015
});

var remote = R.always({
	host: '',
	authKey: '',
	port: 1234
});

var rConn = R.always(r.connect(R.assoc('ssl', {ca: cert}, remote())));

var count = 1;

function _getUndefinedFields(document) {
	return R.filter(fieldName => R.isNil(document[fieldName]), R.keys(document));
}

function _removeUndefinedFields(document) {

	return R.isArrayLike(document)
		? R.map(document => R.omit(_getUndefinedFields(document), document), document)
		: R.omit(_getUndefinedFields(document), document);
}

const db = 'salesforce';
const table = 'accounts';

var _replace = document => rConn()
		.then(conn => r.db(db).table(table).insert(_removeUndefinedFields(document), {conflict: 'replace', returnChanges: true}).run(conn)
			.tap(() => console.log('inserted ' + count++)));

var _run = () => r.connect(local())
	.then(conn => r.db(db).table(table).run(conn)
		.then(cursor => cursor.each(function(err, row) {
			if (err) console.error('ERROR');
			_replace(row);
		}, function () {
			cursor.close();
			conn.close();
		})));

module.exports = {
	run: _run
}