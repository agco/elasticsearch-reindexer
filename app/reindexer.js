'use strict';

var _ = require('lodash');
var ElasticHarvester = require('elastic-harvesterjs');
var Promise = require('bluebird');
var $http = require('http-as-promised');
//noinspection CodeAssistanceForRequiredModule
var url = require('url');
var mongojs = require('mongojs');
var ElasticScroll = require('elasticscroll');

var STATUS_READY = 'ready';
var STATUS_ERROR = 'error';
var STATUS_IN_PROGRESS = 'in-progress';

var PHASE_INITIAL_INDEXING = 'initial-indexing';
var PHASE_INDEXING_NEW_ARRIVALS = 'index-new-arrivals';

var status = STATUS_READY;
var phase;
var progress;
var error;

function doPutMapping(options) {
    const newIndexUrl = url.resolve(options.elasticsearchUrl, options.newIndex);
    return $http.get(newIndexUrl, {error: false}).spread(function (result) {
        if (result.statusCode === 200) {
            throw new Error('New index ' + options.newIndex + ' already exists! How about: curl -XDELETE ' + newIndexUrl);
        } else if (result.statusCode !== 404) {
            throw new Error('Cannot check if index exists:' + options.newIndex);
        }
    }).then(function () {
        return $http.put(newIndexUrl, {json: true, body: options.mapping});
    });
}

function doSwitchAlias(options) {
    var body = {
        actions: [
            {remove: {index: options.oldIndex, alias: options.alias}},
            {add: {index: options.newIndex, alias: options.alias}}
        ]
    };
    return $http.post(url.resolve(options.elasticsearchUrl, '_aliases'), {json: true, body: body});
}

function indexBulk(result, newIndex, type, elasticsearchUrl) {
    var bulk = [];
    var spec = {
        index: {
            _index: newIndex,
            _type: type
        }

    };
    _.forEach(result, function (item) {
        var specInstance = _.cloneDeep(spec);
        specInstance.index._id = item.id;
        bulk.push(JSON.stringify(specInstance));
        bulk.push(JSON.stringify(item));
    });
    var body = bulk.join('\n') + '\n';
    return $http.post(url.resolve(elasticsearchUrl, newIndex + '/' + type + '/_bulk'), {body: body});
}

function fullReindex(options) {
    var oldIndex = options.oldIndex;
    var newIndex = options.newIndex;
    var type = options.type;
    var mongoType = type.toLowerCase();
    var app = {
        _schema: []
    };
    var trackingDataSearch = new ElasticHarvester(app, options.elasticsearchUrl, null, type, {asyncInMemory: true});
    var lastUpdated;
    var limit = 100; //this is batch size, you can increase it if you like
    var db = mongojs(options.mongodbUrl, [mongoType]);

    function fetchFromMongoAndIndex(q, comment) {
        var skip = 0;

        function loop() {
            if (comment) {
                console.info(comment, '-', 'Limit:', limit, 'Skip:' + skip);
            }
            return new Promise(function (resolve, reject) {
                q().limit(limit).skip(skip).toArray(function (err, result) {
                    if (err) {
                        if ('cursor is exhausted' === err.message) {
                            resolve();
                        } else {
                            reject(err);
                        }
                        return;
                    }
                    if (!result.length) {
                        resolve(result.length);
                        return;
                    }
                    progress = skip;
                    result = result.map(function (item) {
                        item.id = item._id;
                        delete item._id;
                        return trackingDataSearch.expandEntity(item).then(function (item) {
                            item._lastUpdated = new Date().getTime();
                            return item;
                        });
                    });
                    Promise.all(result)
                        .then(function (result) {
                            return indexBulk(result, newIndex, type, options.elasticsearchUrl);
                        })
                        .then(function () {
                            resolve(result.length);
                        }, reject);
                });
            }).then(function (result) {
                if (result >= limit) {
                    skip += limit;
                    return loop();
                } else {
                    return null;
                }
            });
        }

        return loop();
    }

    function initialIndexing() {
        phase = PHASE_INITIAL_INDEXING;
        progress = 0;
        function queryFactory() {
            return db[mongoType].find().sort({_id: 1});
        }

        return fetchFromMongoAndIndex(queryFactory, 'initialIndexing');
    }

    function indexNewArrivals() {
        phase = PHASE_INDEXING_NEW_ARRIVALS;
        progress = 0;
        var body = {
            fields: ['_id'],
            sort: ['_lastUpdated'],
            query: {
                filtered: {
                    filter: {
                        range: {
                            '_lastUpdated': {
                                gt: lastUpdated
                            }
                        }
                    }
                }
            }
        };

        var ids = [];

        function onScroll(item) {
            ids.push(item._id);
            if (ids.length >= limit) {
                progress += ids.length;
                var idsToSend = ids.slice();
                ids = [];
                return fetchFromMongoAndIndex(function queryFactory() {
                    return db[mongoType].find({_id: {$in: idsToSend}}).sort({_id: 1});
                });
            }
        }

        return new ElasticScroll(url.resolve(options.elasticsearchUrl, oldIndex + '/' + type), body, onScroll).scroll();
    }

    function putMapping() {
        return doPutMapping(options);
    }

    function switchAlias() {
        return doSwitchAlias(options);
    }

    function saveLastUpdated() {
        var body = {
            size: 0,
            query: {'match_all': {}},
            aggs: {
                maxLastUpdated: {
                    max: {
                        field: '_lastUpdated'
                    }
                }
            }
        };
        return $http.post(url.resolve(options.elasticsearchUrl, oldIndex + '/' + type + '/_search'), {json: true, body: body}).spread(function (res, body) {
            lastUpdated = body.aggregations.maxLastUpdated.value;
        });
    }

    return putMapping().then(saveLastUpdated).then(initialIndexing).then(indexNewArrivals).then(switchAlias);
}

function simpleReindex(options) {
    var oldIndex = options.oldIndex;
    var newIndex = options.newIndex;
    var type = options.type;

    function reindex() {
        phase = PHASE_INITIAL_INDEXING;
        progress = 0;
        var body = {
            sort: ['_lastUpdated'],
            query: {
                'match_all': {}
            }
        };

        var items = [];

        function onScroll(item) {
            items.push(item._source);
            progress++;
            if (items.length >= 100) {
                var itemsToIndex = items.slice();
                items.length = 0;
                return indexBulk(itemsToIndex, newIndex, type, options.elasticsearchUrl);
            }
        }

        return new ElasticScroll(url.resolve(options.elasticsearchUrl, oldIndex + '/' + type), body, onScroll).scroll().then(function () {
            if (items.length) {
                return indexBulk(items, newIndex, type, options.elasticsearchUrl)
            }
        });
    }

    function putMapping() {
        return doPutMapping(options);
    }

    function switchAlias() {
        return doSwitchAlias(options);
    }

    return putMapping().then(reindex).then(switchAlias);
}


module.exports = {
    clearError: function () {
        if (STATUS_ERROR !== status) {
            return Promise.reject({status: 412, message: 'Indexer is not in error state'});
        }
        status = STATUS_READY;
        error = null;
        return Promise.resolve();
    },
    getStatus: function () {
        return Promise.resolve({status: status, error: error, phase: phase, progress: progress});
    },
    startReindexing: function (options) {
        if (STATUS_READY !== status && STATUS_ERROR !== status) {
            return Promise.reject({status: 412, message: 'Indexer is not ready'});
        }
        error = null;
        status = STATUS_IN_PROGRESS;
        var reindexMethod = 'full' === options.mode ? fullReindex : simpleReindex;
        return Promise.resolve().then(function () {
            return reindexMethod(options);
        }).then(function () {
            console.info('Indexing completed successfully');
            status = STATUS_READY;
        }).catch(function (_error) {
            error = _error && _error.message || _error;
            console.error(_error && _error.stack || _error);
            status = STATUS_ERROR;
        });
    }
};
