import { Kafka } from 'kafkajs'
const BROKER_1 = process.env.BROKER_1 || 'localhost:9092'
const BROKER_2 = process.env.BROKER_2 || 'localhost:9092'
const BROKER_3 = process.env.BROKER_3 || 'localhost:9092'
const TOKEN = process.env.STRAPI_TOKEN || ''
const STRAPI_URL = process.env.STRAPI_URL || 'http://localhost:8080'
const TOPIC = process.env.TOPIC || 'product'
const BEGINNING = process.env.BEGINNING == 'true' || 'false'
const ERROR_TOPIC = process.env.ERROR_TOPIC || 'errors'

const log = (...str) => console.log(`${new Date().toUTCString()}: `, ...str)

const kafka = new Kafka({
  clientId: 'product-consumer',
  brokers: [BROKER_1, BROKER_2, BROKER_3],
})

const consumer = kafka.consumer({ groupId: 'product-creator' })
const producer = kafka.producer()

const consume = async () => {
  await Promise.all([consumer.connect(), producer.connect()])
  await consumer.subscribe({ topic: TOPIC, fromBeginning: BEGINNING })

  await consumer.run({
    eachMessage: async ({ message }) => {
      try {
        const strProduct = message.value.toString()
        const product = JSON.parse(strProduct)
        log('creating', strProduct)
        log(product.name, await createProduct(product))
        log('created', strProduct)
      } catch (error) {
        if (ERROR_TOPIC)
          producer.send({
            topic: ERROR_TOPIC,
            messages: [{ value: { error, message } }],
          })
      }
    },
  })
}

const createProduct = async (product) => {
  const res = await fetch(STRAPI_URL + '/api/products', {
    method: 'POST',
    body: JSON.stringify({
      data: product,
    }),
    headers: {
      Authorization: `Bearer ${TOKEN}`,
      'content-type': 'application/json',
    },
  })
  if (res.status === 200) {
    const response = await res.json()
    return response
  }
  return 'error'
}

await consume()
