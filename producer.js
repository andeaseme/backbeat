const config = require('./conf/Config');
const BackbeatProducer = require('./lib/BackbeatProducer');
const uuidv4 = require('uuid/v4');
var AWS = require('aws-sdk'),
	fs = require('fs');

const bucket = require('./bucket.json');
var src_bucket = bucket.src;
var f_name = process.argv[2];

var params1 = {
 Bucket: bucket.src,
 Prefix : "raw/",
 MaxKeys: 2
};

function check()
{
	let s3 = new AWS.S3();
	s3.listObjects(params1, function(err, data) {
		if (err) console.log(err, err.stack); // an error occurred
		// else     console.log(data);
		if (data.Contents[1])
		{
			console.log('FOUND');

var raw_name = data.Contents[1].Key;
let ready_name = 'ready/' + data.Contents[1].Key.substring(4);
 let params = {
  Bucket: bucket.src,
  CopySource: bucket.src + '/' + data.Contents[1].Key,
  Key: ready_name
 };
 console.log("WILL BE DELETING OBJECT: " + raw_name);           // successful response

 console.log('PARAMS:' + params.CopySource);
 s3.copyObject(params, (err, data) => {
   if (err) console.log(err, err.stack); // an error occurred
   else     {
			 console.log("DELETING OBJECT: " + raw_name);           // successful response

			 let params = {
	  Bucket: bucket.src ,
	  Key: raw_name
	 };
	 s3.deleteObject(params, function(err, data) {
	   if (err) console.log(err, err.stack); // an error occurred
	   else
		 {
			 send(ready_name, 0);
			 send("Done",0);
		 }


	 });
 }
 });
		}
		else {
			 send("Done",0);
		}
	});
}

if (process.argv.length == 2) {
		setInterval(check, 1000);
}
else if (process.argv.length == 3)
{
	upload();
}

function upload()
{
	var fileStream = fs.createReadStream(f_name);

	fileStream.on('error', function (err) {
		if (err) { throw err; }
	});

	fileStream.on('open', function () {
		f_name = f_name.substring(0, f_name.lastIndexOf('.')) + '-' +
				 uuidv4() + f_name.substring(f_name.lastIndexOf('.'),
						 f_name.length);
		var s3 = new AWS.S3();
		s3.putObject({
			Bucket: src_bucket,
			Key: 'ready/' + f_name,
			Body: fileStream
		}, function (err) {
			if (err) { throw err; }
			process.stdout.write('UPLOAD COMPLETED.\n');
			send('ready/' + f_name, 1);
		});
	});
};

function send(name, cb)
{
	console.log('SENDING: ' + name);
	const producer = new BackbeatProducer({
		zookeeper: config.zookeeper,
		topic: 'producer-test-topic',
	});

	producer.on('ready', () =>
			producer.send([{ key: 'foo', message: name }], err => {
				if (err) {
					return process.stdout.write(`${err}`);
				}
				process.stdout.write('SEND COMPLETED.\n');
				if (cb == 1)
					process.exit(0);
			}));

	producer.on('error', err => {
		process.stdout.write(`${err}`);
	});
};
