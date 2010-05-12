package com.twitter.haplocheirus

import java.io.IOException
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.SocketChannel
import scala.actors.Actor._
import com.twitter.xrayspecs.Time
import com.twitter.xrayspecs.TimeConversions._
import net.lag.logging.Logger


class RedisException(message: String) extends IOException(message)

object RedisClient {
  val DEFAULT_PORT = 6379
  val RETRY_DELAY = 10.seconds
}

class RedisClient(hostname: String, port: Int) {
  val log = Logger.get(getClass.getName)

  def this(hostname: String) = this(hostname, RedisClient.DEFAULT_PORT)

  @volatile var delaying: Option[Time] = None

  var connection: Option[SocketChannel] = None
  val readBuffer = ByteBuffer.allocate(8192)
  readBuffer.limit(0)


  def isEjected = synchronized {
    delaying match {
      case None => false
      case Some(delay) =>
        if (Time.now < delay) {
          true
        } else {
          delaying = None
          false
        }
    }
  }

  private def eject() {
    synchronized {
      delaying = Some(Time.now + RedisClient.RETRY_DELAY)
      connection = None
    }
  }

  private def connect() {
    if (isEjected) {
      // not yet.
      return
    }

    try {
      connection = Some(SocketChannel.open(new InetSocketAddress(hostname, port)))
    } catch {
      case e: Exception =>
        log.error("Failed to connect to %s:%d -- %s", hostname, port, e.toString())
        eject()
    }
  }

  private def disconnect(): Unit = {
    try {
      connection.foreach(_.close())
    } catch {
      case e: Exception =>
    }
    connection = None
  }

  private def ensureConnected(): Boolean = {
    connection match {
      case Some(_) => true
      case None =>
        connect()
        connection.isDefined
    }
  }

  private def readLine(): String = {
    println(readBuffer)
    var i = readBuffer.position
    while (i < readBuffer.limit) {
      if (readBuffer.get(i) == 10) {
        // EOL
        if (i > readBuffer.position && readBuffer.get(i - 1) == 13) {
          i -= 1
        }
        val data = new Array[Byte](i - readBuffer.position)
        readBuffer.get(data)
        return new String(data)
      }
      i += 1
    }
    readBuffer.compact()
    if (readBuffer.position == readBuffer.capacity) {
      throw new IOException("Unable to receive a whole line in a single buffer")
    }
    connection.foreach { _.read(readBuffer) }
    readBuffer.flip()
    println(readBuffer)
    readLine()
  }

  private def readResponse(): String = {
    val line = readLine()
    line.charAt(0) match {
      case '-' =>
        throw new RedisException(line.substring(1))
      case '+' =>
        line.substring(1)
      case ':' =>
        line.substring(1)
      // $ => bulk data
      // * => multi-bulk data
    }
  }

  val LF = ByteBuffer.wrap("\r\n".getBytes)

  private def sendRequest(request: String, data: Option[Array[Byte]]): String = {
    if (!ensureConnected) {
      return "phooey"
    }

    val buffer = ByteBuffer.wrap(request.getBytes)
    LF.clear()
    while (LF.remaining() > 0) {
      connection.foreach { _.write(Array(buffer, LF)) }
    }
    println(buffer)
    data.foreach { x =>
      val buffer = ByteBuffer.wrap(x)
      LF.clear()
      while (LF.remaining() > 0) {
        connection.foreach { _.write(Array(buffer, LF)) }
      }
    }

    readResponse()
  }


  case class Request(request: String, data: Option[Array[Byte]])

  val actoryThing = actor {
    receive {
      case Request(request, data) =>
        reply(sendRequest(request, data))
    }
  }

  def send(request: String, data: Array[Byte]): String = {
    (actoryThing !? Request(request, Some(data))).asInstanceOf[String]
  }

  def send(request: String): String = {
    (actoryThing !? Request(request, None)).asInstanceOf[String]
  }

  def rpush(key: String, data: Array[Byte]) = send("RPUSH " + key + " " + data.length, data)
}
