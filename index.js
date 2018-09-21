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
    if (iam.closeOnErr(err, 'Starting')) return

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
IAMQP.prototype.closeOnErr = function (err, type) {
  var iam = this
  if (!err) return false

  console.error('[AMQP] Error', type || '', err)
  iam.amqpConn.close()

  console.log('[AMQP]', 'Restart')
  iam.start()
  return true
}

/**
 * Start a publishing channel with confirmation
 */
IAMQP.prototype.startPublisher = function () {
  var iam = this
  iam.amqpConn.createConfirmChannel(function (err, ch) {
    if (iam.closeOnErr(err, 'Publisher createConfirmChannel')) return

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

  iam.pubChannel.publish(exchange, routingKey, Buffer.from(message), { persistent: true },
    function (err, ok) {
      if (err) {
        if (err.message.indexOf('NOT-FOUND') === -1) {
          // only requeue if err is not NOT-FOUND of key or exchange
          iam.offlinePubQueue.push([exchange, routingKey, message])
        }
        iam.pubChannel.connection.close()
        iam.closeOnErr(err, 'Publishing')
      }
    })
}

/**
 * A consumer that acks messages only if processed succesfully
 * @param {string} queue - queue name
 * @param {function} work - message worker
 */
IAMQP.prototype.startConsumer = function (queue, work) {
  var iam = this
  iam.amqpConn.createChannel(function (err, ch) {
    if (iam.closeOnErr(err, 'Consumer createChannel')) return
    ch.on('error', function (err) {
      iam.closeOnErr(err, 'Consumer')
    })

    console.log('[AMQP] sub-channel opened for queue', queue)
    ch.prefetch(10)

    ch.assertQueue(queue, { durable: true }, function (err, _ok) {
      if (iam.closeOnErr(err, 'Consumer assertQueue')) return
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
