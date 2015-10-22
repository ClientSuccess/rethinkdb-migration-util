var R = require('ramda');
var util = require('util');

var Promise = require('bluebird');
var cert = require('fs').readFileSync('./compose');

var rLocal = require('rethinkdbdash')({
    max: 100,
    port: 28015,
    host: 'rethinkdb',
    cursor: true
});

var rRemote = require('rethinkdbdash')({
    max: 100,
    port: 1,
    host: '',
    authKey: '',
    cursor: true,
    ssl: {ca: cert}
});

var db = 'salesforce';
var table = 'accounts';
var insertedCount = 1;

function _getUndefinedFields(document) {
    return R.filter(fieldName => R.isNil(document[fieldName]), R.keys(document));
}

function _removeUndefinedFields(document) {
    return R.isArrayLike(document)
        ? R.map(document => R.omit(_getUndefinedFields(document), document), document)
        : R.omit(_getUndefinedFields(document), document);
}

var _replace = document => rConn()
    .then(conn => r.db(db).table(table).insert(_removeUndefinedFields(document), {
        conflict: 'replace',
        returnChanges: true
    }).run(conn)
        .tap(() => console.log('inserted ' + insertedCount++)));

var _run = () => rRemote.db(db).table(table).run()
    .then(cursor => cursor.each(function (err, row) {
        if (err) console.error('ERROR');
        console.log(row);
        //_replace(row);

    }, function () {
        cursor.close();
    }));

module.exports = {
    run: _run
};