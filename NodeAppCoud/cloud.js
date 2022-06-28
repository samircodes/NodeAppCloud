
import { BlobServiceClient } from '@azure/storage-blob';
import openpgp from 'openpgp';
import fs, { ReadStream } from 'fs';
import * as readline from 'node:readline';
import { Stream } from 'stream';
import { chunk } from 'chunk';
import { Kafka } from 'kafkajs';
import { Transform } from 'node:stream';
import ip from 'ip';
// import { disconnect } from 'process';

(async() => {

// const host = process.env.'pkc-epwny.eastus.azure.confluent.cloud'|| ip.address()
const kafka = new Kafka({
   
    ssl: true,
    brokers: ["pkc-epwny.eastus.azure.confluent.cloud:9092"],
    clientId: 'my-app',
    sasl: {
        mechanism: 'plain', // scram-sha-256 or scram-sha-512
        username: "WWURDQN22AXUSOJU",
        password: "2ZZxhaZY57/VEZSl6YT7RaAt3uj7DbVR990g0a+pwPeSYNfVobimj5cENfzKrlgx"
      },
      connectionTimeout: 3000

  })

  const producer = kafka.producer();
  await producer.connect();
  console.log("Producer connected");
 const sam = "samirrocks";
  producer.send({
    topic: 'poems',
    messages: [
      { value: 'sam',
       partition:  0,
           },
    ],
  })



})();