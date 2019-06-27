# iamqp

```js
const IAMQP = require('iamqp')

const config = {
  connection: {
    protocol: 'amqp',
    cluster: [
      '1.1.1.1',
      '1.1.1.2'
    ]
    port: 5672,
    username: 'user',
    password: 'password',
    vhost: '/vhost'
  },
  sockOpts: {
    noDelay: true,
    keepAlive: true
  }
}

var iam = new IAMQP(config)

iam.whenConnected = function () {
  iam.startPublisher()
  // iam.publish(logexchange, key, json, callback)

  iam.startConsumer('cms.websosanh.org.devops', (message, done) => {
    console.log(JSON.parse(message.content.toString()))
    done()
  })
}

iam.start()
```