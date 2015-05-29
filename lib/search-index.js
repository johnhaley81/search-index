'use strict';
/*jslint indent: 2 */
var async = require('async');
var levelMultiply = require('level-multiply');
var bunyan = require('bunyan');
var level = require('levelup');
var levelMultiply = require('level-multiply');
var _ = require('lodash');

module.exports = function(options) {
  var SearchIndex = {};
  var defaults = {
    indexPath: 'si',
    logLevel: 'warn'
  };
  //initialize defaults options
  var options = _.defaults(options || {}, defaults);
  var log = bunyan.createLogger({name: "norch", level: options.logLevel});
  var calibrater = require('./indexing/calibrater')({
    log: log.child({component: 'calibrater', level: options.logLevel})
  });
  var deleter = require('./indexing/deleter.js');
  var docGetter = require('./search/docGetter.js');
  var indexPeek = require('./indexing/indexPeek.js');
  var indexer = require('./indexing/indexer.js');
  var matcher = require('./matchers/matcher.js');
  var replicator = require('./indexing/replicator.js');
  var searcher = require('./search/searcher.js');

  var indexes = (options && options.db)
    ? level(options.indexPath, {valueEncoding: 'json', db: options.db})
    : level(options.indexPath, {valueEncoding: 'json'});
  var indexesMultiply = levelMultiply(indexes);

  deleter.setLogLevel(options.logLevel);
  searcher.setLogLevel(options.logLevel);
  indexPeek.setLogLevel(options.logLevel);
  matcher.setLogLevel(options.logLevel);
  replicator.setLogLevel(options.logLevel);


  //is there a better way of doing this?
  calibrater.getTotalDocs(indexes, function(err, totalDocs) {
    searcher.setTotalDocs(totalDocs);
  });


  SearchIndex.getOptions = function() {
    return options;
  }


  SearchIndex.add = function(options, batch, callback) {
    var filters = options.filters || {};
    indexer.addDocToIndex(indexes, batch, options.batchName, filters, callback);
  };


  SearchIndex.del = function(docID, callback) {
    deleter.deleteBatch([docID], indexes, callback);
  };

  SearchIndex.deleteBatch = function(batch, callback) {
    deleter.deleteBatch(batch, indexes, callback);
  };

  SearchIndex.empty = function(callback) {
    var deleteOps = [];
    indexes = indexes;
    indexes.createKeyStream({'gte':'0', 'lte':'~~~~~~~~~~~~~~~~~~~'})
      .on('data', function (data) {
        deleteOps.push({'type': 'del', 'key': data});
      })
      .on('error', function (err) {
        log.error(err, ' failed to empty index')
      })
      .on('end', function () {
        indexes.batch(deleteOps, callback);
      })
  };

  SearchIndex.get = function(docID, callback) {
    docGetter.getDoc(indexes, docID, callback)
  };

  SearchIndex.match = function(beginsWith, callback) {
    matcher.matcher(indexes, beginsWith, callback);
  };

  //create a pipey and non-pipey version here. (non-pipey is for bundling with browserify)
  SearchIndex.replicate = function(readStream, callback) {
    replicator.replicateFromSnapShotStream(readStream, indexes, callback);
  };

  SearchIndex.replicateBatch = function(serializedDB, callback) {
    replicator.replicateFromSnapShotBatch(serializedDB, indexes, callback);
  };


  SearchIndex.search = function(q, callback) {
    searcher.search(indexes, q, function(results){
      //TODO: make error throwing real
      callback(false, results);
    });
  };

  SearchIndex.snapShotBatch = function(callback) {
    replicator.createSnapShotBatch(indexes, callback);
  };

  SearchIndex.snapShot = function(callback) {
    replicator.createSnapShot(indexes, callback);
  };

  SearchIndex.tellMeAboutMySearchIndex = function(callback) {
    calibrater.getTotalDocs(indexes, function(err, totalDocs) {
      var metadata = {};
      metadata['totalDocs'] = totalDocs;
      callback(metadata);
    });
  };


  //utility methods for testing and devlopment
  //******************************************
  SearchIndex.indexRange = function(options, callback) {
    indexPeek.indexRange(options.start, options.stop, indexes, callback);
  };

  SearchIndex.indexValue = function(options, callback) {
    indexPeek.indexValue(options.key, indexes, callback);
  };

  //do a full recalibration of the index
  SearchIndex.calibrate = function(callback) {
    calibrater.calibrate(indexes, callback);
  };

  return SearchIndex;
};
