const config = require('./conf/Config');
const BackbeatConsumer = require('./lib/BackbeatConsumer');
const AWS = require('aws-sdk');
const events = require('events');
let fs = require('fs');

const bucket = require('./bucket.json');

function processKafkaEntry(kafkaEntry, done) {
	process.stdout.write(`${kafkaEntry.value}\n`);
	let s3 = new AWS.S3();
	// f_name = kafkaEntry.value;
	let params = {Bucket: '42senko', Key: 'issasong.wav'};
	let file = fs.createWriteStream('./save.wav');
	s3.getObject(params).createReadStream().pipe(file);
	file.on('close', function () {
		let read_file = fs.createReadStream('./save.wav');
		s3.putObject({
			Bucket: '42mabucket',
			Key: 'save.wav',
			Body: read_file
		}, function (err) {
			if (err) {
				throw err;
				console.log('error putting object\n');
			}
			process.stdout.write('uploaded\n');
		});
	});
	// test();
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
process.stdout.write(`hello\n`);
