const config = require('./conf/Config');
const BackbeatConsumer = require('./lib/BackbeatConsumer');
var AWS = require('aws-sdk'),
	fs = require('fs');
//var sox = require('sox');

const bucket = require('./bucket.json');
const send = async (rs, bucket, filename) => {
	let s3 = new AWS.S3();
	rs.on('open', () => {
		console.log('OPEN');
	});
	rs.on('end', () => {
		console.log('END');
	});
	rs.on('close', () => {
		console.log('CLOSE');
	});

	const response = await s3.upload({
		Bucket: bucket,
		Key: filename,
		Body: rs
	}).promise();
	console.log('response');
	console.log(response);
}


let s_buck = bucket.src;
let d_buck = bucket.destiny;
let f_name = "issasong.mp3";

let options = {
	input: f_name,
	output: 'something.mp4',
	preset: 'Normal',
	rotate: 1
}

function processKafkaEntry(kafkaEntry, done) {
	process.stdout.write(`${kafkaEntry.value}\n`);
	let s3 = new AWS.S3();
	// f_name = kafkaEntry.value;
	let params = {Bucket: '42senko', Key: 'issasong.wav'};
	let file = require('fs').createWriteStream('./save.wav');
	let read_file = fs.createReadStream('./save.wav');
	s3.getObject(params).createReadStream().pipe(file);
	send(read_file, '42mabucket', 'save.wav').catch(err =>{
		console.log(err);
	});
	// test();
	return done();
}

/*function test()
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
}*/

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
