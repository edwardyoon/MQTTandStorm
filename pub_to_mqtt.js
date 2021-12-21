const mqtt = require('mqtt');
const config = require('./config.json');

const client = mqtt.connect(config.options);

client.on("connect", () => {	
  console.log("connected: "+ client.connected);
  client.publish('test', "hello from mqtt");

  client.end();
});

