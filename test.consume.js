import { Kafka } from 'kafkajs'
import { faker } from '@faker-js/faker'
import fs from 'fs'

const BROKER_1 = process.env.BROKER_1 || 'localhost:9092'
const BROKER_2 = process.env.BROKER_2 || 'localhost:9092'
const BROKER_3 = process.env.BROKER_3 || 'localhost:9092'

const kafka = new Kafka({
  clientId: 'product-consumer',
  brokers: [BROKER_1, BROKER_2, BROKER_3],
})

const producer = kafka.producer()

const genProduct = () => ({
  name: faker.commerce.productName(),
  description: faker.commerce.productDescription(),
  stock_available: faker.number.int(40),
  barcode: faker.commerce.isbn({ variant: 10, separator: '' }),
  status: ['safe', 'danger'][faker.number.int(1)],
})

const produce = (amount = 1) => new Array(amount).fill(0).map(genProduct)

const produceProduct = async (products) => {
  await producer.connect()
  await producer.send({
    topic: 'product',
    messages: products.map((product) => ({
      value: JSON.stringify(product),
    })),
  })
  console.log(products.map((p) => p.name).join('\n'))
  await producer.disconnect()
}

await produceProduct(produce(10))
//
//
// fs.writeFileSync(
//   '../product-producer/products.json',
//   [
//     'name,description,stock_available,barcode,status',
//     ...produce(200).map((r) =>
//       [r.name, r.description, r.stock_available, r.barcode, r.status].join(',')
//     ),
//   ].join('\n')
// )
