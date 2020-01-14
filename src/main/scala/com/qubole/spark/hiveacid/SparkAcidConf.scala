/*
 * Copyright 2019 Qubole, Inc.  All rights reserved.
 *
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

package com.qubole.spark.hiveacid

import org.apache.spark.sql.SparkSession

/**
  * Spark specific configuration container to be used by Hive Acid module
  */
case class SparkAcidConfigEntry[T](configName: String /* Name of the config */ ,
                                   defaultValue: String /* Default value of config in String*/ ,
                                   description: String /* Description of the config*/ ,
                                   converter: Option[(String, String) => T] /* function to convert from String to Config's Type T*/)


case class SparkAcidConfigBuilder[T](configName: String) {
  private var defaultValue: Option[String] = None
  def defaultValue(value: String): SparkAcidConfigBuilder[T] = {
    defaultValue = Some(value)
    this
  }

  private var description = ""
  def description(desc : String): SparkAcidConfigBuilder[T] = {
    description = desc
    this
  }

  private var converter: Option[(String, String) => T] = None
  def converter(func: (String, String) => T): SparkAcidConfigBuilder[T] = {
    converter = Some(func)
    this
  }

  def create(): SparkAcidConfigEntry[T] = {
    require(!defaultValue.isEmpty, "Default Value for the Spark Acid Config needs to be specified")
    new SparkAcidConfigEntry[T](configName, defaultValue.get, description, converter)
  }
}

case class SparkAcidConf(@transient sparkSession: SparkSession, @transient parameters: Map[String, String]) {
  @transient val configMap = sparkSession.sessionState.conf.getAllConfs

  val predicatePushdownEnabled = getConf(SparkAcidConf.PREDICATE_PUSHDOWN_CONF)
  val metastorePartitionPruningEnabled = sparkSession.sessionState.conf.metastorePartitionPruning
  val includeRowIds = parameters.getOrElse("includeRowIds", "false").toBoolean

  def getConf[T](configEntry: SparkAcidConfigEntry[T]): T = {
    val value = configMap.getOrElse(configEntry.configName, configEntry.defaultValue)
    configEntry.converter match {
      case Some(f) => f(value, configEntry.configName)
      case None => value.asInstanceOf[T]
    }
  }
}

object SparkAcidConf {
  val PREDICATE_PUSHDOWN_CONF = SparkAcidConfigBuilder[Boolean]("spark.sql.hiveAcid.enablePredicatePushdown")
    .defaultValue("true")
    .converter(toBoolean)
    .description("Configuration to enable Predicate PushDown for Hive Acid Reader")
    .create()

  def toBoolean(s: String, key: String): Boolean = {
    try {
      s.trim.toBoolean
    } catch {
      case _: IllegalArgumentException =>
        throw new IllegalArgumentException(s"$key should be boolean, but was $s")
    }
  }
}