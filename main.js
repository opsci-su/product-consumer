import { Kafka } from 'kafkajs'
const BROKER_1 = process.env.BROKER_1 || 'localhost:9092'
const BROKER_2 = process.env.BROKER_2 || 'localhost:9092'
const BROKER_3 = process.env.BROKER_3 || 'localhost:9092'
const STRAPI_URL = process.env.STRAPI_URL || 'http://localhost:8080'
const TOKEN = process.env.STRAPI_TOKEN || ''
const TOPIC = process.env.TOPIC || 'product'
const ERROR_TOPIC = process.env.ERROR_TOPIC || 'errors'
const MAX_RETRY = 5 // Maximum number of retries for processing a message

const log = (...str) => console.log(`${new Date().toUTCString()}: `, ...str)

log(BROKER_1, BROKER_2, BROKER_3)
const kafka = new Kafka({
  clientId: 'product-consumer',
  brokers: [BROKER_1, BROKER_2, BROKER_3],
})

const consumer = kafka.consumer({
  groupId: 'product-creator',
  autoOffsetReset: 'earliest',
  autoCommit: false,
  enableAutoOffsetReset: false, // Disable automatic offset reset
  groupOffsetTrackingTimeout: -1, // Enable key-based offset tracking
  assignPartitions: true, // Use assigned consumer
})

const producer = kafka.producer({
  acks: 'all', // Wait for acknowledgment from all replicas
  enableIdempotence: true, // Enable idempotent producer
  autoOffsetReset: 'earliest',
})

const consume = async () => {
  try {
    log('Try to connect consumer and producer ...')
    await Promise.all([consumer.connect(), producer.connect()])
    log('consumer and producer connnected !')
    await consumer.subscribe({ topic: TOPIC })

    await consumer.run({
      eachBatch: async ({
        batch,
        resolveOffset,
        heartbeat,
        commitOffsetsIfNecessary,
        uncommittedOffsets,
        isRunning,
      }) => {
        let partition = batch.partition

        await Promise.all(
          batch.messages.map(async (message) => {
            let retries = 0
            let processed = false
            const strProduct = message.value.toString()

            while (!processed && retries < MAX_RETRY) {
              try {
                const product = JSON.parse(strProduct)
                log('creating', strProduct)
                const result = await createProduct(product)

                if (result === 'error') {
                  retries++
                  if (retries === MAX_RETRY) {
                    await handleOffsetOutOfRange(
                      resolveOffset,
                      strProduct,
                      partition
                    )
                    processed = true
                  }
                } else {
                  log('created', strProduct)
                  await resolveOffset(message.offset)
                  processed = true
                }
              } catch (error) {
                log('Error processing message:', error.message)
                if (ERROR_TOPIC) {
                  await sendErrorMessage(error, strProduct)
                }
                break
              }
            }

            if (!processed && retries === MAX_RETRY) {
              log(
                `Message processing failed after ${MAX_RETRY} retries, sending to ERROR_TOPIC`
              )
              await handleOffsetOutOfRange(resolveOffset, strProduct, partition)
            }
          })
        )

        if (isRunning() && uncommittedOffsets.size > 0) {
          await commitOffsetsIfNecessary(uncommittedOffsets)
        }

        await heartbeat()
      },
    })
  } catch (error) {
    log('Error in consume function:', error.message)
    // Handle the error as needed
  }
}

const handleOffsetOutOfRange = async (resolveOffset, message, partition) => {
  log(`Handling Offset Out of Range for message: ${JSON.stringify(message)}`)
  try {
    // Mark the message as consumed
    await resolveOffset(message.offset)
    // Send the message to the error topic
    const errorMessage = {
      error: 'Exceeded max retries',
      message: message,
    }
    await producer.send({
      topic: ERROR_TOPIC,
      messages: [{ value: JSON.stringify(errorMessage) }],
    })
    // Move to the next message
    await consumer.seek({
      topic: TOPIC,
      partition: partition,
      offset: message.offset + 1,
    })
  } catch (error) {
    log('Error handling offset out of range:', error.message)
  }
}

const sendErrorMessage = async (error, message) => {
  log(`Sending error message to ${ERROR_TOPIC}:`, error.message)
  try {
    const errorMessage = {
      error: error.message,
      message: message,
    }
    await producer.send({
      topic: ERROR_TOPIC,
      messages: [{ value: JSON.stringify(errorMessage) }],
    })
  } catch (error) {
    log('Error sending error message:', error.message)
  }
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
}

await consume()
