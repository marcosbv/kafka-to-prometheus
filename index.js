/* Kafka libraries */
const { Kafka, CompressionTypes, CompressionCodecs, logLevel } = require('kafkajs')
const LZ4 = require('kafkajs-lz4')

CompressionCodecs[CompressionTypes.LZ4] = new LZ4().codec


const http = require('http')
const url = require('url')
const client = require('prom-client')
const register = new client.Registry()

async function startPrometheusServer() {
    // Add a default label which is added to all metrics
    register.setDefaultLabels({
      app: 'ibmcloud-paas-metrics'
    })
    
    // Enable the collection of default metrics
    // client.collectDefaultMetrics({ register })
    
    // Define the HTTP server
    const server = http.createServer(async (req, res) => {
      // Retrieve route from request object
      const route = url.parse(req.url).pathname
    
      if (route === '/metrics') {
        // Return all metrics the Prometheus exposition format
        res.setHeader('Content-Type', register.contentType)
        res.end(await register.metrics())
      }
    })

    server.listen(9000)
}


async function consumeKafka() {

   const metricsRegistryMap = new Map()

   const kafka = new Kafka({
       clientId: 'ibmcloud-paas-metrics',
       brokers: process.env.KAFKA_BROKERS.split(','),
       ssl: {
           rejectUnauthorized: false
       },
       sasl: {
           mechanism: 'plain',
           username: process.env.KAFKA_USER || 'token',
           password: process.env.KAFKA_PASSWORD
       },
       logLevel: logLevel.INFO,
       connectionTimeout: 5000
   })

   const consumer = kafka.consumer({
       groupId: process.env.KAFKA_TOPIC + '-consumer-group',
   })

   await consumer.connect()
   await consumer.subscribe({
       topic: process.env.KAFKA_TOPIC,
       fromBeginning: true
   })

   await consumer.run({
       eachMessage: async({topic, partition, message, heartbeat}) => {
           const metricObj = JSON.parse(message.value.toString())
           const metricName = metricObj.metric

           let metricRegistry = metricsRegistryMap.get(metricName)
          
           if(metricRegistry == null) {
               metricRegistry = new client.Gauge({
                   name: metricName,
                   help: metricName,
                   labelNames: Object.keys(metricObj.labels)
               })

               register.registerMetric(metricRegistry)
               metricsRegistryMap.set(metricName, metricRegistry)
           }
           metricRegistry.set(metricObj.labels, metricObj.value)
           
       }

       
   })

}

startPrometheusServer().then(() => {
    consumeKafka()
})


