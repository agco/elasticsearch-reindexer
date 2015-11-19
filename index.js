'use strict';

var app = require('./app/app');

var config = {
    port: process.env.PORT || 9000
};


app(config).listen(config.port, function () {
    console.info('Listening on port', config.port);
});
