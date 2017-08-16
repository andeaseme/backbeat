const config = require('./conf/Config');
const BackbeatProducer = require('./lib/BackbeatProducer');
var AWS = require('aws-sdk'),
    fs = require('fs');

const bucket = require('./bucket.json');

if (process.argv.length != 3) {
  console.log("Usage: " + __filename + " [filepath]\n");
  process.exit(-1);
}

var buck = bucket.src;
var f_name = process.argv[2];

var fileStream = fs.createReadStream(f_name);
fileStream.on('error', function (err) {
  if (err) { throw err; }
});

fileStream.on('open', function () {
  var s3 = new AWS.S3();
  s3.putObject({
    Bucket: buck,
    Key: f_name,
    Body: fileStream
  }, function (err) {
    if (err) { throw err; }
    process.stdout.write('SEND COMPLETED.\n');
    return process.exit();
  });
});

const producer = new BackbeatProducer({
    zookeeper: config.zookeeper,
    topic: 'producer-test-topic',
});

producer.on('ready', () =>
    producer.send([{ key: 'foo', message: f_name }], err => {
        if (err) {
            return process.stdout.write(`${err}`);
        }
        // process.stdout.write('SEND COMPLETED.\n');
        // return process.exit();
    }));

producer.on('error', err => {
    process.stdout.write(`${err}`);
    return process.exit();
});
