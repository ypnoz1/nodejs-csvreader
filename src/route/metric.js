const express = require('express');
const metricRoute = express.Router();
const {
    getFileByMonth,
    getMainFile,
} = require('../controller/metricController.js');

metricRoute.get('/main', getMainFile);
metricRoute.get('/monthly', getFileByMonth);


module.exports = metricRoute; 