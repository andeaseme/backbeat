const config = require('./conf/Config');
const BackbeatConsumer = require('./lib/BackbeatConsumer');
const AWS = require('aws-sdk');
const EventEmitter = require('events');
let fs = require('fs');

const bucket = require('./bucket.json');
const EE = new EventEmitter();
let src_bucket = bucket.src;
let dst_bucket = bucket.destiny;
let output_list = bucket.output_types;

function deleteTemps(f_name) {
	fs.unlink("./temp/" + f_name, function (err) {
		if (err) {
			throw err;
			console.log('Failure to delete ' + f_name);
			return ;
		}
	});
	console.log(f_name + ' deleted\n');
}

function transcode_upload(f_name, format_ext) {
	let hbjs = require('handbrake-js');

	let trans_name = f_name.substring(0, f_name.lastIndexOf('.')) + '.' + format_ext;
	process.stdout.write('TRANSCODING: ' + trans_name + '\n');
	hbjs.spawn({ input: './temp/' + f_name, output: './temp/' + trans_name })
		.on('error', function(err){
			// invalid user input, no video found etc
		})
	.on('progress', function(progress){
		console.log(
				'%s Percent complete: %s, ETA: %s',
				format_ext,
				progress.percentComplete,
				progress.eta
				);
	})
	.on('end', function() {
		let s3 = new AWS.S3();
		let read_file = fs.createReadStream('./temp/' + trans_name);
		s3.putObject({
			Bucket: dst_bucket,
			Key: trans_name,
			Body: read_file
		}, function (err) {
			if (err) {
				throw err;
				console.log('error putting object\n');
			}
			process.stdout.write(format_ext + ' FILE UPLOADED\n');
			deleteTemps(trans_name);
		});
	});
}

function processKafkaEntry(kafkaEntry, done) {
	let f_name = kafkaEntry.value.toString();
	process.stdout.write('GETTING: ' + f_name + '\n');
	let params = {Bucket: src_bucket, Key: f_name};
	f_name = f_name.substring(6);
	let file = fs.createWriteStream('./temp/' + f_name);
	let s3 = new AWS.S3();

	s3.getObject(params).createReadStream().pipe(file);
	file.on('close', function () {
		process.stdout.write('FILE RECIEVED\n');
		for (let x in output_list) {
			transcode_upload(f_name, output_list[x]);
		}
		process.stdout.write('PROCESS DONE.\n');
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
