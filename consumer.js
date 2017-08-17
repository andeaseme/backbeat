const config = require('./conf/Config');
const BackbeatConsumer = require('./lib/BackbeatConsumer');
const AWS = require('aws-sdk');
const events = require('events');
let fs = require('fs');

const bucket = require('./bucket.json');
var src_bucket = bucket.src;
var dst_bucket = bucket.destiny;

function processKafkaEntry(kafkaEntry, done) {
	let s3 = new AWS.S3();
	var f_name = kafkaEntry.value.toString();
	process.stdout.write('GETTING: ' + f_name + '\n');
	let params = {Bucket: src_bucket, Key: f_name};
	let file = fs.createWriteStream('./temp/' + f_name);

	s3.getObject(params).createReadStream().pipe(file);

	file.on('close', function () {
		var hbjs = require('handbrake-js');

		process.stdout.write('FILE RECIEVED\n');
		var trans_name = f_name.substring(0, f_name.lastIndexOf('.')) + '.m4v';
		process.stdout.write('TRANSCODING: ' + trans_name + '\n');
		hbjs.spawn({ input: './temp/' + f_name, output: trans_name })
		  .on('error', function(err){
		    // invalid user input, no video found etc
		  })
		  .on('progress', function(progress){
		    console.log(
		      'Percent complete: %s, ETA: %s',
		      progress.percentComplete,
		      progress.eta
		    );
		  })
		  .on('end', function() {
			  let read_file = fs.createReadStream(trans_name);
			  s3.putObject({
				  Bucket: dst_bucket,
				  Key: trans_name,
				  Body: read_file
			  }, function (err) {
				  if (err) {
					  throw err;
					  console.log('error putting object\n');
				  }
				  process.stdout.write('FILE UPLOADED\n');
			  });
		  });
	});
	return done();
}

let consumer = new BackbeatConsumer({
	zookeeper: config.zookeeper,
	topic: 'producer-test-topic',
	groupId: config.extensions.replication.queueProcessor.groupId,
	concurrency: 1, // Process one entry at a time.
	queueProcessor: processKafkaEntry,
});

consumer.on('error', err => process.stdout.write(`${err}`));
consumer.subscribe();
process.stdout.write(`CONSUMER STARTED.\n`);
