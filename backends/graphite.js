/*
 * Flush stats to graphite (http://graphite.wikidot.com/).
 *
 * To enable this backend, include 'graphite' in the backends
 * configuration array:
 *
 *   backends: ['graphite']
 *
 * This backend supports the following config options:
 *
 *   graphiteHost: Hostname of graphite server.
 *   graphitePort: Port to contact graphite server at.
 */

var net = require('net'),
   util = require('util');

var debug;
var flushInterval;
var graphiteHost;
var graphitePort;
var graphiteRoot;

var graphiteStats = {};

var post_stats = function graphite_post_stats(statString) {
  var last_flush = graphiteStats.last_flush || 0;
  var last_exception = graphiteStats.last_exception || 0;
  if (graphiteHost) {
    try {
      var graphite = net.createConnection(graphitePort, graphiteHost);
      graphite.addListener('error', function(connectionException){
        if (debug) {
          util.log(connectionException);
        }
      });
      graphite.on('connect', function() {
        var ts = Math.round(new Date().getTime() / 1000);
        statString += graphiteRoot + '.statsd.graphiteStats.last_exception ' + last_exception + ' ' + ts + "\n";
        statString += graphiteRoot + '.statsd.graphiteStats.last_flush ' + last_flush + ' ' + ts + "\n";
        this.write(statString);
        this.end();
        graphiteStats.last_flush = Math.round(new Date().getTime() / 1000);
      });
    } catch(e){
      if (debug) {
        util.log(e);
      }
      graphiteStats.last_exception = Math.round(new Date().getTime() / 1000);
    }
  }
}

var flush_stats = function graphite_flush(ts, metrics) {
  var starttime = Date.now();
  var statString = '';
  var numStats = 0;
  var key;
  var timer_data_key;
  var counters = metrics.counters;
  var gauges = metrics.gauges;
  var timers = metrics.timers;
  var sets = metrics.sets;
  var counter_rates = metrics.counter_rates;
  var timer_data = metrics.timer_data;
  var statsd_metrics = metrics.statsd_metrics;

  for (key in counters) {
    statString += graphiteRoot + '.'        + key + ' ' + counter_rates[key] + ' ' + ts + "\n";
    statString += graphiteRoot + '_counts.' + key + ' ' + counters[key]      + ' ' + ts + "\n";

    numStats += 1;
  }

  for (key in timer_data) {
    if (Object.keys(timer_data).length > 0) {
      for (timer_data_key in timer_data[key]) {
         statString += graphiteRoot + '.timers.' + key + '.' + timer_data_key + ' ' + timer_data[key][timer_data_key] + ' ' + ts + "\n";
      }

      numStats += 1;
    }
  }

  for (key in gauges) {
    statString += graphiteRoot + '.gauges.' + key + ' ' + gauges[key] + ' ' + ts + "\n";

    numStats += 1;
  }

  for (key in sets) {
    statString += graphiteRoot + '.sets.' + key + '.count ' + sets[key].values().length + ' ' + ts + "\n";

    numStats += 1;
  }

  for (key in statsd_metrics) {
    statString += graphiteRoot + '.statsd.' + key + ' ' + statsd_metrics[key] + ' ' + ts + "\n";
  }

  statString += graphiteRoot + '.numStats ' + numStats + ' ' + ts + "\n";
  statString += graphiteRoot + '.statsd.graphiteStats.calculationtime ' + (Date.now() - starttime) + ' ' + ts + "\n";
  post_stats(statString);
};

var backend_status = function graphite_status(writeCb) {
  for (stat in graphiteStats) {
    writeCb(null, 'graphite', stat, graphiteStats[stat]);
  }
};

exports.init = function graphite_init(startup_time, config, events) {
  debug = config.debug;
  graphiteHost = config.graphiteHost;
  graphitePort = config.graphitePort;
  graphiteRoot = (config.graphiteRoot || 'stats').replace(/^\.|\.$/g, '');

  graphiteStats.last_flush = startup_time;
  graphiteStats.last_exception = startup_time;

  flushInterval = config.flushInterval;

  events.on('flush', flush_stats);
  events.on('status', backend_status);

  return true;
};
