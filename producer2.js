const kafka = require('kafka-node');
const async = require('async');

const client = new kafka.KafkaClient();
const producer = new kafka.HighLevelProducer(client);

let queue = async.queue((message, done) => {
    producer.send([{ topic: 'mem-issue-test', messages: [message], attributes: 1}], err => {
       if (err) console.error(err);
       done();
    });
}, 100);

queue.buffer = 50;

let paused = false;

queue.saturated = () => paused = true;
queue.unsaturated = () => paused = false;

queue.drain = () => console.log('Everything published!');

const PAYLOAD = '1234567890'.repeat(100000);

let count = 0;
let interval = setInterval(() => {
  if (!paused) {
      queue.push(PAYLOAD);
      count++;

      if (count % 1000 === 0) {
        console.log(`${new Date()} Published ${count}`);
      }

      if (count === 10000) {
          clearInterval(interval);
      }
  }
}, 1);
