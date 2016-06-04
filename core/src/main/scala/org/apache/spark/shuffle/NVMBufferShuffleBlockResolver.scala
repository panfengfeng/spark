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

package org.apache.spark.shuffle

import java.io.{InputStream, OutputStream}
import java.nio.{ByteOrder, ByteBuffer}
import java.nio.channels.{ScatteringByteChannel, GatheringByteChannel}
import java.nio.charset.Charset
import org.apache.spark.serializer.Serializer

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Seq
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._
import io.netty.buffer.Unpooled
import io.netty.buffer._
import org.apache.spark.network.buffer.{NettyManagedBuffer, ManagedBuffer}
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.storage._
import org.apache.spark.{SparkEnv, Logging, SparkConf}

/**
 * Create and maintain the shuffle blocks' mapping between logic block and physical file location.
 * Data of shuffle blocks from the same map task are stored in a single consolidated data file.
 * The offsets of the data blocks in the data file are stored in a separate index file.
 *
 * We use the name of the shuffle data's shuffleBlockId with reduce ID set to 0 and add ".data"
 * as the filename postfix for data file, and ".index" as the filename postfix for index file.
 *
 */
// Note: Changes to the format in this file should be kept in sync with
// org.apache.spark.network.shuffle.ExternalShuffleBlockResolver#getSortBasedShuffleBlockData().
private[spark] class NVMBufferShuffleBlockResolver(
    conf: SparkConf,
    _blockManager: BlockManager = null)
  extends ShuffleBlockResolver
  with Logging {

  private lazy val blockManager = Option(_blockManager).getOrElse(SparkEnv.get.blockManager)

  private val transportConf = SparkTransportConf.fromSparkConf(conf, "shuffle")

  /**
   * Remove data file and index file that contain the output data from one map.
   * */
  def removeDataByMap(shuffleId: Int, mapId: Int): Unit = {

  }

  /**
   * Write an index file with the offsets of each block, plus a final offset at the end for the
   * end of the output file. This will be used by getBlockData to figure out where each block
   * begins and ends.
   *
   * It will commit the data and index file as an atomic operation, use the existing ones, or
   * replace them with new ones.
   *
   * Note: the `lengths` will be updated to match the existing index file if use the existing ones.
   * */
  def writeNVMBufferAndCommit(
      shuffleId: Int,
      mapId: Int,
      lengths: Array[Long] ): Unit = {
    val size: Long = 0
    for (i <- 0 until lengths.length) {
      lengths(i) = size
    }
  }

  override def getBlockData(blockId: ShuffleBlockId): ManagedBuffer = {
    // The block is actually going to be a range of a single map output file for this map, so
    // find out the consolidated file, then the offset within that from our index
    val arraylist = blockManager.nvmbufferManager.get(blockId.toString)
    val index = arraylist.size()
    System.out.println("NVMBufferShuffleBlockResolver getBlockData index@panda " + index + " blockId " + blockId.toString)
    var sum: Int = 0
    for(ele <- arraylist.asScala) {
      System.out.println("Bytebuf size@panda " + ele.readableBytes() + " " + ele.readerIndex() + " " + ele.writerIndex() + " " + ele.isDirect + " " + ele.writableBytes() + " " + ele.capacity())
      sum += ele.readableBytes()
    }
    val wrappedNVMBuffer = Unpooled.wrappedBuffer(arraylist.size(), arraylist.asScala:_*)
    val nettymanagedbuffer = new NettyManagedBuffer(wrappedNVMBuffer)
    System.out.println("sum is " + sum + " wrappedNVMBuffer size@panda " + wrappedNVMBuffer.readableBytes() + " nettymanagedbuffer size " + nettymanagedbuffer.size())
    nettymanagedbuffer
  }

  override def stop(): Unit = {}
}

