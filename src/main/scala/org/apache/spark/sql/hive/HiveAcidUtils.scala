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

package org.apache.spark.sql.hive

import scala.collection.JavaConverters._
import com.qubole.spark.hiveacid.hive.HiveAcidMetadata
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTablePartition, CatalogUtils}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, BoundReference, Expression, InterpretedPredicate, PrettyAttribute}

object HiveAcidUtils {

  /**
    * This is adapted from [[org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils.prunePartitionsByFilter]]
    * Instead of [[org.apache.spark.sql.catalyst.catalog.CatalogTable]] this function will be using [[HiveAcidMetadata]]
    * @param hiveAcidMetadata
    * @param inputPartitions
    * @param predicates
    * @param defaultTimeZoneId
    * @return
    */
  def prunePartitionsByFilter(
                               hiveAcidMetadata: HiveAcidMetadata,
                               inputPartitions: Seq[CatalogTablePartition],
                               predicates: Option[Expression],
                               defaultTimeZoneId: String): Seq[CatalogTablePartition] = {
    if (predicates.isEmpty) {
      inputPartitions
    } else {
      val partitionSchema = hiveAcidMetadata.partitionSchema
      val partitionColumnNames = hiveAcidMetadata.partitionSchema.fieldNames.toSet

      val nonPartitionPruningPredicates = predicates.filterNot {
        _.references.map(_.name).toSet.subsetOf(partitionColumnNames)
      }
      if (nonPartitionPruningPredicates.nonEmpty) {
        throw new AnalysisException("Expected only partition pruning predicates: " +
          nonPartitionPruningPredicates)
      }

      val boundPredicate =
        InterpretedPredicate.create(predicates.get.transform {
          case att: Attribute =>
            val index = partitionSchema.indexWhere(_.name == att.name)
            BoundReference(index, partitionSchema(index).dataType, nullable = true)
        })

      inputPartitions.filter { p =>
        boundPredicate.eval(p.toRow(partitionSchema, defaultTimeZoneId))
      }
    }
  }

  def convertToCatalogTablePartition(hp: org.apache.hadoop.hive.ql.metadata.Partition): CatalogTablePartition = {
    val apiPartition = hp.getTPartition
    val properties: Map[String, String] = if (hp.getParameters != null) {
      hp.getParameters.asScala.toMap
    } else {
      Map.empty
    }
    CatalogTablePartition(
      spec = Option(hp.getSpec).map(_.asScala.toMap).getOrElse(Map.empty),
      storage = CatalogStorageFormat(
        locationUri = Option(CatalogUtils.stringToURI(apiPartition.getSd.getLocation)),
        inputFormat = Option(apiPartition.getSd.getInputFormat),
        outputFormat = Option(apiPartition.getSd.getOutputFormat),
        serde = Option(apiPartition.getSd.getSerdeInfo.getSerializationLib),
        compressed = apiPartition.getSd.isCompressed,
        properties = Option(apiPartition.getSd.getSerdeInfo.getParameters)
          .map(_.asScala.toMap).orNull),
      createTime = apiPartition.getCreateTime.toLong * 1000,
      lastAccessTime = apiPartition.getLastAccessTime.toLong * 1000,
      parameters = properties,
      stats = None) // TODO: need to implement readHiveStats
  }
}
