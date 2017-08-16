const config = require('./conf/Config');
const BackbeatConsumer = require('./lib/BackbeatConsumer');
var AWS = require('aws-sdk'),
    fs = require('fs');
var sox = require('sox');

const bucket = require('./bucket.json');

var s_buck = bucket.src;
var d_buck = bucket.destiny;
var f_name = "issasong.mp3";

var options = {
  input: f_name,
  output: 'something.mp4',
  preset: 'Normal',
  rotate: 1
}

function processKafkaEntry(kafkaEntry, done) {
    process.stdout.write(`${kafkaEntry.value}\n`);
    var s3 = new AWS.S3();
    var params = {Bucket: d_buck, Key: f_name};
    var file = require('fs').createWriteStream('./temp/save.mp3');
    test();
    return done();
}

function test()
{
  var hbjs = require('handbrake-js');
  hbjs.spawn({ input: 'issashow.avi', output: 'something.m4v' })
    .on('error', function(err){
      // invalid user input, no video found etc
    })
    .on('progress', function(progress){
      console.log(
        'Percent complete: %s, ETA: %s',
        progress.percentComplete,
        progress.eta
      );
    });
}



const consumer = new BackbeatConsumer({
    zookeeper: config.zookeeper,
    topic: 'producer-test-topic',
    groupId: config.extensions.replication.queueProcessor.groupId,
    concurrency: 1, // Process one entry at a time.
    queueProcessor: processKafkaEntry,
});

consumer.on('error', err => process.stdout.write(`${err}`));
consumer.subscribe();
process.stdout.write(`hello\n`);
