'use strict';

var Promise = require('bluebird');
var STATUS_READY = 'ready';
var STATUS_IN_PROGRESS = 'in-progress';

var status = STATUS_READY;

module.exports = {
    getStatus: function () {
        return Promise.resolve(status);
    },
    startReindexing: function (/*options*/) {
        if (STATUS_READY !== status) {
            return Promise.reject({status: 412, message: 'Indexer is not ready'});
        }
        status = STATUS_IN_PROGRESS;
        Promise.delay(10000).then(function () {
            status = STATUS_READY;
        });
        return Promise.resolve();

    }
};
