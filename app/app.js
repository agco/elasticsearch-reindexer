'use strict';

var express = require('express');
var bodyParser = require('body-parser');

var reindexer = require('./reindexer');

module.exports = function () {
    var app = express();

    app.use(express.static(__dirname + '/views'));
    app.use(bodyParser.urlencoded({extended: false}));
    app.use(bodyParser.json());

    app.route('/api/reindex/status').get(function (req, res) {
        reindexer.getStatus().then(function (result) {
            res.send(result);
        }).catch(function (error) {
            console.error(error && error.stack || error);
            res.sendStatus(500);
        });
    });

    app.route('/api/reindex/error').delete(function (req, res) {
        reindexer.clearError().then(function () {
            res.sendStatus(200);
        }).catch(function (error) {
            console.error(error && error.stack || error);
            res.sendStatus(500);
        });
    });
    app.route('/api/reindex/start').post(function (req, res) {
        reindexer.startReindexing(req.body).then(function () {
            res.sendStatus(200);
        }).catch(function (error) {
            console.error(error && error.stack || error);
            if (error && error.status) {
                res.status(error.status).send(error.message);
            } else {
                res.sendStatus(500);
            }
        });
    });
    return app;
};
