/**
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

package ly.stealth.mesos.kafka

import java.util

import scala.Some
import scala.collection.JavaConversions._
import scala.collection.{Map, Seq, mutable}
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.ZkNodeExistsException
import kafka.admin._
import kafka.common.{AdminCommandFailedException, TopicAndPartition}
import kafka.utils.{ZKStringSerializer, ZkUtils}
import net.elodina.mesos.util.Period
import org.apache.log4j.Logger

class Rebalancer {
  private val logger: Logger = Logger.getLogger(this.getClass)

  @volatile private var assignment: Map[TopicAndPartition, Seq[Int]] = null
  @volatile private var reassignment: Map[TopicAndPartition, Seq[Int]] = null

  private def newZkUtils: ZkUtils = ZkUtils(Config.zk, 30000, 30000, isZkSecurityEnabled = false)//todo: parametrize isZkSecurityEnabled

  def running: Boolean = {
    val zkClient = newZkUtils.zkClient
    try { zkClient.exists(ZkUtils.ReassignPartitionsPath) }
    finally { zkClient.close() }
  }

  def start(topics: util.List[String], brokers: util.List[String], replicas: Int = -1): Unit = {
    if (topics.isEmpty) throw new Rebalancer.Exception("no topics")

    logger.info(s"Starting rebalance for topics ${topics.mkString(",")} on brokers ${brokers.mkString(",")} with ${if (replicas == -1) "<default>" else replicas} replicas")
    val zkUtils = newZkUtils
    try {
      val assignment: Map[TopicAndPartition, Seq[Int]] = zkUtils.getReplicaAssignmentForTopics(topics)
      val reassignment: Map[TopicAndPartition, Seq[Int]] = getReassignments(brokers.map(b => BrokerMetadata(b.toInt, None)), topics, assignment, replicas)

      reassignPartitions(zkUtils, reassignment)

      this.reassignment = reassignment
      this.assignment = assignment
    } finally {
      zkUtils.close()
    }
  }

  def state: String = {
    if (assignment == null) return ""
    var s = ""

    val zkUtils = newZkUtils
    try {
      val reassigning: Map[TopicAndPartition, Seq[Int]] = zkUtils.getPartitionsBeingReassigned.mapValues(_.newReplicas)

      val byTopic: Map[String, Map[TopicAndPartition, Seq[Int]]] = assignment.groupBy(tp => tp._1.topic)
      for (topic <- byTopic.keys.to[List].sorted) {
        s += topic + "\n"

        val partitions: Map[TopicAndPartition, Seq[Int]] = byTopic.getOrElse(topic, null)
        for (topicAndPartition <- partitions.keys.to[List].sortBy(_.partition)) {
          val brokers = partitions.getOrElse(topicAndPartition, null)
          s += "  " + topicAndPartition.partition + ": " + brokers.mkString(",")
          s += " -> "
          s += reassignment.getOrElse(topicAndPartition, null).mkString(",")
          s += " - " + getReassignmentState(zkUtils, topicAndPartition, reassigning)
          s += "\n"
        }
      }
    } finally {
      zkUtils.close()
    }

    s
  }
  
  def waitFor(running: Boolean, timeout: Period): Boolean = {
    def matches: Boolean = this.running == running

    var t = timeout.ms
    while (t > 0 && !matches) {
      val delay = Math.min(100, t)
      Thread.sleep(delay)
      t -= delay
    }

    matches
  }

  private def getReassignments(brokerMetadata: Seq[BrokerMetadata], topics: util.List[String], assignment: Map[TopicAndPartition, Seq[Int]], replicas: Int = -1): Map[TopicAndPartition, Seq[Int]] = {
    var reassignment : Map[TopicAndPartition, Seq[Int]] = new mutable.HashMap[TopicAndPartition, List[Int]]()

    val byTopic: Map[String, Map[TopicAndPartition, Seq[Int]]] = assignment.groupBy(tp => tp._1.topic)
    byTopic.foreach { entry =>
      val topic = entry._1
      val rf: Int = if (replicas != -1) replicas else entry._2.valuesIterator.next().size
      val partitions: Int = entry._2.size
    
      val assignedReplicas: Map[Int, Seq[Int]] = AdminUtils.assignReplicasToBrokers(brokerMetadata, partitions, rf, 0, 0)
      reassignment ++= assignedReplicas.map(replicaEntry => TopicAndPartition(topic, replicaEntry._1) -> replicaEntry._2)
    }

    reassignment.toMap
  }

  private def getReassignmentState(zkUtils: ZkUtils, topicAndPartition: TopicAndPartition, reassigning: Map[TopicAndPartition, Seq[Int]]): String = {
    reassigning.get(topicAndPartition) match {
      case Some(partition) => "running"
      case None =>
        // check if the current replica assignment matches the expected one after reassignment
        val assignedBrokers = reassignment(topicAndPartition)
        val brokers = zkUtils.getReplicasForPartition(topicAndPartition.topic, topicAndPartition.partition)

        if(brokers.sorted == assignedBrokers.sorted) "done"
        else "error"
    }
  }

  private def reassignPartitions(zkUtils: ZkUtils, partitions: Map[TopicAndPartition, Seq[Int]]): Unit = {
    try {
      val reassignPartitionsCommand = new ReassignPartitionsCommand(zkUtils, partitions)
      reassignPartitionsCommand.reassignPartitions()
    } catch {
      case ze: AdminCommandFailedException => throw new Rebalancer.Exception(ze.getMessage)
    }
  }
}

object Rebalancer {
  class Exception(message: String) extends java.lang.Exception(message)
}
