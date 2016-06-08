/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.storage

import java.io.OutputStream
import java.util
import java.util.ArrayList
import io.netty.buffer.Unpooled
import io.netty.buffer._

import org.apache.spark.Logging
import org.apache.spark.serializer.{SerializerInstance, SerializationStream}
import org.apache.spark.executor.ShuffleWriteMetrics

/**
 * A class for writing JVM objects directly to a file on disk. This class allows data to be appended
 * to an existing block and can guarantee atomicity in the case of faults as it allows the caller to
 * revert partial writes.
 *
 * This class does not support concurrent writes. Also, once the writer has been opened it cannot be
 * reopened again.
 */
private[spark] class NVMBufferObjectWriter(
    blockManager: BlockManager,
    serializerInstance: SerializerInstance,
    compressStream: OutputStream => OutputStream,
    // These write metrics concurrently shared with other active NVMBufferObjectWriters who
    // are themselves performing writes. All updates must be relative.
    writeMetrics: ShuffleWriteMetrics,
    val blockId: BlockId = null)
  extends OutputStream
  with Logging {

  private var bs: OutputStream = null
  private var objOut: SerializationStream = null
  private var initialized = false
  private var commitAndCloseHasBeenCalled = false
  private val bytebufSize = blockManager.getgranularity()
  private val maxcapacity = blockManager.getmaxcapacity()
  private val autoscaling = blockManager.getautoscaling()
  private val minspaceleft = blockManager.getminspaceleft()
  val arraylist = new util.ArrayList[ByteBuf]()

  /**
   * Cursors used to represent positions in the file.
   *
   * xxxxxxxx|--------|---       |
   *         ^        ^          ^
   *         |        |        finalPosition
   *         |      reportedPosition
   *       initialPosition
   *
   * initialPosition: Offset in the file where we start writing. Immutable.
   * reportedPosition: Position at the time of the last update to the write metrics.
   * finalPosition: Offset where we stopped writing. Set on closeAndCommit() then never changed.
   * -----: Current writes to the underlying file.
   * xxxxx: Existing contents of the file.
   */
  private val initialPosition = 0
  private var reportedPosition = initialPosition

  /**
   * Keep track of number of records written and also use this to periodically
   * output bytes written since the latter is expensive to do for each record.
   */
  private var numRecordsWritten = 0

  def open(): NVMBufferObjectWriter = {
    val serbytebuf = if (autoscaling)
                        // the max size is 4G
                        new ByteBufOutputStream(Unpooled.directBuffer(bytebufSize))
                     else
                        // the max size is maxcapacity
                        new ByteBufOutputStream(Unpooled.directBuffer(bytebufSize, maxcapacity))
    bs = compressStream(serbytebuf)
    objOut = serializerInstance.serializeStream(bs)
    arraylist.add(serbytebuf.buffer())
    initialized = true
    this
  }

  override def close() {
    if (initialized) {
      objOut.close()
      bs = null
      objOut = null
      initialized = false
    }
  }

  def isOpen: Boolean = objOut != null

  /**
   * Flush the partial writes and commit them as a single atomic block.
   */
  def commitAndClose(): Unit = {
    if (initialized) {
      // NOTE: Because Kryo doesn't flush the underlying stream we explicitly flush both the
      //       serializer stream and the lower level stream.
      objOut.flush()
      bs.flush()
      close()
      // finalPosition = 0
      // In certain compression codecs, more bytes are written after close() is called
      // writeMetrics.incShuffleBytesWritten(finalPosition - reportedPosition)
    } else {
     // finalPosition = 0
    }
    blockManager.nvmbufferManager.putIfAbsent(blockId.toString, arraylist)
    commitAndCloseHasBeenCalled = true
  }


  /**
   * Reverts writes that haven't been flushed yet. Callers should invoke this function
   * when there are runtime exceptions. This method will not throw, though it may be
   * unsuccessful in truncating written data.
   *
   * @return the file that this DiskBlockObjectWriter wrote to.
   */
  def cleararraylist(): Unit = {
    if (arraylist.size() != 0) {
      for(i <- 0 until arraylist.size()) {
        arraylist.get(i).release()
      }
     arraylist.clear()
    }
  }
  /**
   * Writes a key-value pair.
    assert(arraylist.size() > 0)
    val index = arraylist.size()
    System.out.println("NVMBufferObjectWriter index " + index)
    if (!arraylist.get(index-1).isWritable(256)) {
      System.out.println("not enough space@panda")
      objOut.flush()
      bs.flush()
      close()
    }
   */
  def write(key: Any, value: Any) {
    if (!initialized) {
      open()
    }
    objOut.writeKey(key)
    objOut.writeValue(value)
    /*
    Todo: better algorithm to allocate bytebuf
    * */
    if (!autoscaling) {
      val index = arraylist.size()
      if (arraylist.get(index -1).writableBytes() < minspaceleft) {
        objOut.flush()
        bs.flush()
        close()
      }
    }
  }

  override def write(b: Int): Unit = throw new UnsupportedOperationException()

  override def write(kvBytes: Array[Byte], offs: Int, len: Int): Unit = {
    if (!initialized) {
      open()
    }
    bs.write(kvBytes, offs, len)
  }

  /**
   * Notify the writer that a record worth of bytes has been written with OutputStream#write.
   */
  /*
  def recordWritten(): Unit = {
    numRecordsWritten += 1
    writeMetrics.incShuffleRecordsWritten(1)

    if (numRecordsWritten % 32 == 0) {
      updateBytesWritten()
    }
  }
  */

  /**
   * Returns the file segment of committed data that this Writer has written.
   * This is only valid after commitAndClose() has been called.
   */
  /*
  def fileSegment(): FileSegment = {
    if (!commitAndCloseHasBeenCalled) {
      throw new IllegalStateException(
        "fileSegment() is only valid after commitAndClose() has been called")
    }
    new FileSegment(file, initialPosition, finalPosition - initialPosition)
  }
  */
  /**
   * Report the number of bytes written in this writer's shuffle write metrics.
   * Note that this is only valid before the underlying streams are closed.
   */
  /*
  private def updateBytesWritten() {
    val pos = channel.position()
    writeMetrics.incShuffleBytesWritten(pos - reportedPosition)
    reportedPosition = pos
  }
  */

  // For testing
  private[spark] override def flush() {
    objOut.flush()
    bs.flush()
  }
}
