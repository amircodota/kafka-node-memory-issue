const kafka = require('node-rdkafka');
const async = require('async');

const consumer = new kafka.KafkaConsumer({
        'group.id': 'mem-test-group17',
        'message.max.bytes': 5000000,
        'fetch.message.max.bytes': 50000000,
        'receive.message.max.bytes': 5100000,
        'queued.min.messages': 1,
        'metadata.broker.list': 'localhost:9092',
        'statistics.interval.ms': 1000,
    }, {
        'auto.offset.reset': 'earliest',
    });

consumer.connect();

let count = 0;
const queue = async.queue((message, done) => {
    queueSize--;
    count++;
    if (count % 10 === 0) {
        console.log(`${new Date()} processed ${count} messages`);
    }
    setTimeout(done, 1000);
}, 10);

//let consumed = 0;
const readNext = (err, messages) => {
    if (err) console.error(err);
    //if (messages) console.log('Received:', messages.length, 'messages');
    if (!paused) {
        //consumed += 100;
        //console.log('Consumed:', consumed);

        setImmediate(() => consumer.consume(100, readNext));
    }
};

let paused = false;
queue.saturated = () => {
    if (!paused) {
        console.log('pausing....');
        //consumer.pause();
        paused = true;
    }
};

queue.unsaturated = () => {
    if (paused) {
        console.log('resuming');
        //consumer.resume();
        paused = false;
        readNext();
    }
};

queue.drain = () => console.log('All messages processed');
queue.buffer = 5;

const PRINT_STATS = false;

if (PRINT_STATS) {
    consumer.on('event.stats', function (stats) {
        console.log('STATISTICS:', JSON.stringify(JSON.parse(stats.message), null, 2));
    });
}

let queueSize = 0;
consumer.on('ready', () => {
    consumer.subscribe(['mem-issue-test2']);

    readNext();
}).on('data', message => {
    queue.push(message);
    queueSize++;

    if (queueSize % 100 === 0) {
        console.log('queueSize:', queueSize);
    }
}).on('event.error', err => console.error('Error:', err)
).on('event.log', message => console.log('Message:', message));


console.log(`Starting consumer. This process is pid ${process.pid}, librdkakfa version: ${kafka.librdkafkaVersion}`);