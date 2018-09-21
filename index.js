// Credit to https://gist.github.com/carlhoerberg/006b01ac17a0a94859ba
// @Laittg

var amqp = require('amqplib/callback_api')

module.exports = IAMQP

function IAMQP (cfg) {
  var iam = this

  iam.config = cfg

  // the connection
  iam.amqpConn = null

  // callback fn after amqp connected
  iam.whenConnected = null

  // the publishing channel
  iam.pubChannel = null

  // array of [exchange, routingKey, content] to retry when lost connection
  iam.offlinePubQueue = []

  // callback fn after amqp publisher started
  iam.startPublishing = null
}

/**
 * Start a connection
 */
IAMQP.prototype.start = function () {
  var iam = this
  amqp.connect(iam.config.connection, iam.config.sockOpts, function (err, conn) {
    // if the connection is closed or fails to be established at all, we will reconnect
    if (err) {
      console.error('[AMQP]', err.message)
      return setTimeout(() => { iam.start() }, 1000)
    }
    conn.on('error', function (err) {
      if (err.message !== 'Connection closing') {
        console.error('[AMQP] conn error', err.message)
      }
    })
    conn.on('close', function () {
      console.error('[AMQP] connection closed')
      iam.amqpConn = null
      return setTimeout(() => { iam.start() }, 1000)
    })

    console.log('[AMQP] connected')
    iam.amqpConn = conn

    // fire tasks
    if (iam.whenConnected) iam.whenConnected()
  })
}

/**
 * Close the connection on error
 * @param {*} err
 */
IAMQP.prototype.closeOnErr = function (err) {
  if (!err) return false
  console.error('[AMQP] error', err)
  this.amqpConn.close()
  return true
}

/**
 * Start a publishing channel with confirmation
 */
IAMQP.prototype.startPublisher = function () {
  var iam = this
  iam.amqpConn.createConfirmChannel(function (err, ch) {
    if (iam.closeOnErr(err)) return
    ch.on('error', function (err) {
      console.error('[AMQP] pub-channel error', err.message)
    })
    ch.on('close', function () {
      console.log('[AMQP] pub-channel closed')
    })

    console.log('[AMQP] pub-channel opened')
    iam.pubChannel = ch

    // fire tasks
    if (iam.startPublishing) iam.startPublishing()

    // retry not published messages (e.g. failed when amqp is offline)
    while (iam.offlinePubQueue.length > 0) {
      var m = iam.offlinePubQueue.shift()
      iam.publish(m[0], m[1], m[2])
    }
  })
}

/**
 * Publish a message
 * @param {string} exchange
 * @param {string} routingKey
 * @param {*} content
 */
IAMQP.prototype.publish = function (exchange, routingKey, content) {
  var iam = this
  var ctype = typeof content
  var message

  if (ctype === 'object') {
    message = JSON.stringify(content)
  } else if (ctype === 'string') {
    message = content
  } else {
    message = content.toString()
  }

  // queue the message when disconnected
  if (iam.amqpConn === null) return iam.offlinePubQueue.push([exchange, routingKey, message])

  try {
    iam.pubChannel.publish(exchange, routingKey, Buffer.from(message), { persistent: true },
      function (err, ok) {
        if (err) {
          console.error('[AMQP] publish', err)
          iam.offlinePubQueue.push([exchange, routingKey, message])
          iam.pubChannel.connection.close()
        }
      })
  } catch (e) {
    console.error('[AMQP] publish', e.message)
    iam.offlinePubQueue.push([exchange, routingKey, message])
  }
}

/**
 * A consumer that acks messages only if processed succesfully
 * @param {string} queue - queue name
 * @param {function} work - message worker
 */
IAMQP.prototype.startConsumer = function (queue, work) {
  var iam = this
  iam.amqpConn.createChannel(function (err, ch) {
    if (iam.closeOnErr(err)) return
    ch.on('error', function (err) {
      console.error('[AMQP] sub-channel error', err.message)
    })
    ch.on('close', function () {
      console.log('[AMQP] sub-channel closed')
    })

    console.log('[AMQP] sub-channel opened for queue', queue)
    ch.prefetch(10)

    ch.assertQueue(queue, { durable: true }, function (err, _ok) {
      if (iam.closeOnErr(err)) return
      ch.consume(queue, processMsg, { noAck: false })
      console.log('Worker is started to handle messages in queue', queue)
    })

    function processMsg (msg) {
      work(msg, function (ok) {
        try {
          if (ok) { ch.ack(msg) } else { ch.reject(msg, true) }
        } catch (e) {
          iam.closeOnErr(e)
        }
      })
    }
  })
}

/**
 * A sample work function
 * @param {object} msg - amqp message
 * @param {function} done - callback true/false
 */
IAMQP.prototype._work = function (msg, done) {
  console.log('Got msg', msg.content.toString())
  done(true)
}
