'use strict';

var _ = require('lodash');
var ElasticHarvester = require('elastic-harvesterjs');
var Promise = require('bluebird');
var $http = require('http-as-promised');
var url = require('url');
var mongojs = require('mongojs');

var STATUS_READY = 'ready';
var STATUS_ERROR = 'error';
var STATUS_IN_PROGRESS = 'in-progress';

var status = STATUS_READY;
var error;


function fullReindex(options) {
    var oldIndex = options.oldIndex;
    var newIndex = options.newIndex;
    var alias = options.alias;
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
                q.skip(skip).toArray(function (err, result) {
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
                    result = result.map(function (item) {
                        item.id = item._id;
                        delete item._id;
                        return trackingDataSearch.expandEntity(item);
                    });
                    Promise.all(result)
                        .then(function (result) {
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
                            return $http.post(url.resolve(options.elasticsearchUrl, newIndex + '/' + type + '/_bulk'), {body: body});
                        })
                        .then(function () {
                            resolve(result.length);
                        }, reject);
                });
            }).then(function (result) {
                if (result >= limit) {
                    skip += limit;
                    return loop(q);
                } else {
                    return null;
                }
            });
        }

        q.limit(limit);
        return loop();
    }

    function initialIndexing() {
        var q = db[mongoType].find().sort({_id: 1});
        return fetchFromMongoAndIndex(q, 'initialIndexing');
    }

    function indexNewArrivals() {
        var skip = 0;
        //TODO use scroll search from ES http://www.maori.geek.nz/scroll_elasticsearch_using_promises_and_node_js/
        var body = {
            size: limit,
            from: skip,
            fields: ['_id'],
            query: {
                //TODO add sort here
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

        function loop() {
            console.info('indexNewArrivals', '-', 'Limit:', limit, 'Skip:', skip);
            body.from = skip;
            skip += limit;
            return $http.post(url.resolve(options.elasticsearchUrl, oldIndex + '/' + type + '/_search'), {json: true, body: body}).spread(function (res, body) {
                var ids = _.pluck(body.hits.hits, '_id');
                var q = db[mongoType].find({_id: {$in: ids}}).sort({_id: 1});
                return fetchFromMongoAndIndex(q).then(function () {
                    if (body.hits.hits.length >= limit) {
                        return loop();
                    }
                });

            });
        }

        return loop();
    }

    function putMapping() {
        const newIndexUrl = url.resolve(options.elasticsearchUrl, newIndex);
        return $http.get(newIndexUrl, {error: false}).spread(function (result) {
            if (result.statusCode === 200) {
                throw new Error('New index ' + newIndex + ' already exists! How about: curl -XDELETE ' + newIndexUrl);
            } else if (result.statusCode !== 404) {
                throw new Error('Cannot check if index exists:' + newIndex);
            }
        }).then(function () {
            return $http.put(newIndexUrl, {json: true, body: options.mapping});
        });
    }

    function switchAlias() {
        var body = {
            actions: [
                {remove: {index: oldIndex, alias: alias}},
                {add: {index: newIndex, alias: alias}}
            ]
        };
        return $http.post(url.resolve(options.elasticsearchUrl, '_aliases'), {json: true, body: body});
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

function simpleReindex() {
    return Promise.reject('Simple reindex not implemented yet');
}


module.exports = {
    clearError: function () {
        if (STATUS_ERROR !== status) {
            return Promise.reject({status: 412, message: 'Indexer is not ready'});
        }
        status = STATUS_READY;
        error = null;
        return Promise.resolve();
    },
    getStatus: function () {
        return Promise.resolve({status: status, error: error});
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
