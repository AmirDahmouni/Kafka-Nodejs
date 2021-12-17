import Kafka from 'node-rdkafka';
import eventType from '../eventType.js';

const stream = Kafka.Producer.createWriteStream({
  'metadata.broker.list': 'localhost:9092'
}, {}, {
  topic: 'big_data'
});

stream.on('error', (err) => {
  console.error('Error in our kafka stream');
  console.error(err);
});

function queueRandomMessage() {
  const category = getRandomAnimal();
  const noise = getRandomNoise(category);
  const event = { category, noise };
  const success = stream.write(eventType.toBuffer(event));     
  if (success) {
    console.log(`message queued (${JSON.stringify(event)})`);
  } else {
    console.log('Too many messages in the queue already..');
  }
}

function getRandomAnimal() {
  const categories = ['CAT', 'DOG',"MONKEY","LIONESS","SHEEP"];
  return categories[Math.floor(Math.random() * categories.length)];
}

function getRandomNoise(animal) {
  switch(animal)
  {
    case "CAT":{
      const noises = ['meow', 'miaaaw'];
      return noises[Math.floor(Math.random() * noises.length)];
    }
    case 'DOG':{
      const noises = ['wooooof',"habhab","hobhob"];
      return noises[Math.floor(Math.random() * noises.length)];
    }
    case 'MONKEY':{
      const noises = ['hoho', 'oooa',"houha"];
      return noises[Math.floor(Math.random() * noises.length)];
    }
    case 'LIONESS':{
      const noises = ['greeeeeee', 'ghaaaaaoui'];
      return noises[Math.floor(Math.random() * noises.length)];
    }
    case "SHEEP": return "baaaaabaaaaa"
    default :return "silence.."
  } 
}

setInterval(() => {
  queueRandomMessage();
}, 2000);
