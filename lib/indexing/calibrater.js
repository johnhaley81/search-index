var _ = require('lodash');
var async = require('async');

module.exports = function (options) {
  var log = options.log || require('bunyan').createLogger({name: "norch"});
  var calibrater = {};

  calibrater.getTotalDocs = function (indexes, callback) {
    indexes.get('search-index.totalDocs', function (err, value) {
      if (err) {
        log.info('Index is empty or not calibrated');
        callback(null, 0);
      }
      else {
        log.info(value + ' documents searchable');
        callback(null, value);
      }
    });
  };

  calibrater.incrementallyCalibrate = function (indexes, tf, callback) {
    log.info('calibrating...');
    async.parallel({
      countDocuments: function (callback) {
        countDocuments(indexes, function (err, msg) {
          callback(null);
        });
      },
      reducedTF: function (callback) {
        var keys = _.keys(tf['reducedTF']);
        async.map(keys, function (key, callback) {
          var rTF = tf['reducedTF'][key];
          indexes.get(key, function (err, value) {
            if (!err) rTF = rTF.concat(value);
            rTF = rTF.sort();
            callback(null, {key: key, rTF: rTF});
          });
        }, function (err, result) {
          var batch = _.reduce(result, function (batch, item) {
            return batch.put(item.key, item.rTF);
          }, indexes.batch())
          batch.write(function () { callback(null); });
        });
      },
      reducedTFSortOnTF: function (callback) {
        var keys = _.keys(tf['reducedTFSortOnTF']);
        async.map(keys, function (key, callback) {
          var rTF = tf['reducedTFSortOnTF'][key];
          indexes.get(key, function (err, value) {
            if (!err) rTF = rTF.concat(value);
            rTF = _.sortBy(rTF, function (a) { return a[0]; });
            callback(null, {key: key, rTF: rTF});
          });
        }, function (err, result) {
          var batch = _.reduce(result, function (batch, item) {
            return batch.put(item.key, item.rTF);
          }, indexes.batch());

          batch.write(function () { callback(null); });
        });
      }
    },
    function (err, result) {
      if (err) {
        log.error(err.toString());
        return callback(err);
      }
      callback(null);
    });
  }
  return calibrater;
};

var countDocuments = function (indexes, callback) {
  var tally = 0;
  indexes.createReadStream({
    start: 'DOCUMENT~',
    end: 'DOCUMENT~~'
  })
  .on('data', function (data) {
    tally++;
  })
  .on('end', function () {
    indexes.put('search-index.totalDocs', tally, function () {
      callback(null, 'calibrated ' + tally + ' docs');
    });
  });
};
