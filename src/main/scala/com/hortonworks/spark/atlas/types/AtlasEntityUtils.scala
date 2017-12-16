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

package com.hortonworks.spark.atlas.types

import org.apache.spark.sql.execution.QueryExecution

import scala.collection.JavaConverters._

import org.apache.atlas.AtlasClient
import org.apache.atlas.model.instance.AtlasEntity
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, CatalogStorageFormat, CatalogTable}
import org.apache.spark.sql.types.StructType

import com.hortonworks.spark.atlas.utils.{Logging, SparkUtils}

object AtlasEntityUtils extends Logging {

  def dbUniqueAttribute(db: String): String = SparkUtils.getUniqueQualifiedPrefix() + db

  def dbToEntity(dbDefinition: CatalogDatabase): AtlasEntity = {
    val entity = new AtlasEntity(metadata.DB_TYPE_STRING)

    entity.setAttribute(
      AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, dbUniqueAttribute(dbDefinition.name))
    entity.setAttribute("name", dbDefinition.name)
    entity.setAttribute("description", dbDefinition.description)
    entity.setAttribute("locationUri", dbDefinition.locationUri.toString)
    entity.setAttribute("properties", dbDefinition.properties.asJava)
    entity
  }

  def storageFormatUniqueAttribute(db: String, table: String): String = {
    SparkUtils.getUniqueQualifiedPrefix() + s"$db.$table.storageFormat"
  }

  def storageFormatToEntity(
      storageFormat: CatalogStorageFormat,
      db: String,
      table: String): AtlasEntity = {
    val entity = new AtlasEntity(metadata.STORAGEDESC_TYPE_STRING)

    entity.setAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
      storageFormatUniqueAttribute(db, table))
    storageFormat.locationUri.foreach(entity.setAttribute("locationUri", _))
    storageFormat.inputFormat.foreach(entity.setAttribute("inputFormat", _))
    storageFormat.outputFormat.foreach(entity.setAttribute("outputFormat", _))
    storageFormat.serde.foreach(entity.setAttribute("serde", _))
    entity.setAttribute("compressed", storageFormat.compressed)
    entity.setAttribute("properties", storageFormat.properties.asJava)
    entity.setAttribute("name", storageFormat.toString())
    entity
  }

  def columnUniqueAttribute(db: String, table: String, col: String): String = {
    SparkUtils.getUniqueQualifiedPrefix() + s"$db.$table.col-$col"
  }

  def schemaToEntity(schema: StructType, db: String, table: String): List[AtlasEntity] = {
    schema.map { struct =>
      val entity = new AtlasEntity(metadata.COLUMN_TYPE_STRING)

      entity.setAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
        columnUniqueAttribute(db, table, struct.name))
      entity.setAttribute("name", struct.name)
      entity.setAttribute("type", struct.dataType.typeName)
      entity.setAttribute("nullable", struct.nullable)
      entity.setAttribute("metadata", struct.metadata.toString())
      entity
    }.toList
  }

  def tableUniqueAttribute(db: String, table: String): String = {
    SparkUtils.getUniqueQualifiedPrefix() + s"$db.$table"
  }

  def tableToEntity(
      tableDefinition: CatalogTable,
      db: AtlasEntity,
      schema: List[AtlasEntity],
      storageFormat: AtlasEntity): AtlasEntity = {
    val entity = new AtlasEntity(metadata.TABLE_TYPE_STRING)

    entity.setAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
      tableUniqueAttribute(tableDefinition.identifier.database.getOrElse("default"),
        tableDefinition.identifier.table))
    entity.setAttribute("name", tableDefinition.identifier.table)
    entity.setAttribute("database", db)
    entity.setAttribute("tableType", tableDefinition.tableType.name)
    entity.setAttribute("storage", storageFormat)
    entity.setAttribute("schema", schema.asJava)
    tableDefinition.provider.foreach(entity.setAttribute("provider", _))
    entity.setAttribute("partitionColumnNames", tableDefinition.partitionColumnNames.asJava)
    tableDefinition.bucketSpec.foreach(
      b => entity.setAttribute("bucketSpec", b.toLinkedHashMap.asJava))
    entity.setAttribute("owner", tableDefinition.owner)
    entity.setAttribute("createTime", tableDefinition.createTime)
    entity.setAttribute("lastAccessTime", tableDefinition.lastAccessTime)
    entity.setAttribute("properties", tableDefinition.properties.asJava)
    tableDefinition.viewText.foreach(entity.setAttribute("viewText", _))
    tableDefinition.comment.foreach(entity.setAttribute("comment", _))
    entity.setAttribute("unsupportedFeatures", tableDefinition.unsupportedFeatures.asJava)

    entity
  }

  def processUniqueAttribute(executionId: Long): String = {
    SparkUtils.sparkSession.sparkContext.applicationId + "." + executionId
  }

  def processToEntity(qe: QueryExecution,
      inputs: List[AtlasEntity],
      outputs: List[AtlasEntity],
      inputTables: List[String],
      outputTables: List[String]): AtlasEntity = {
    val entity = new AtlasEntity(metadata.PROCESS_TYPE_STRING)
    entity.setAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
      SparkUtils.sparkSession.sparkContext.applicationId + "."
        + inputTables.toString() + "." + outputTables.toString())
    entity.setAttribute("name", inputTables.toString() + "." + outputTables.toString())
    entity.setAttribute("inputs", inputs.asJava)
    entity.setAttribute("outputs", outputs.asJava)
    entity.setAttribute("details", qe.toString())
    entity.setAttribute("sparkPlanDescription", qe.sparkPlan.toString())
    entity
  }

}
