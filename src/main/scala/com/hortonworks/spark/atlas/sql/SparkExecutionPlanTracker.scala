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

package com.hortonworks.spark.atlas.sql

import com.hortonworks.spark.atlas.types.AtlasEntityUtils
import com.hortonworks.spark.atlas.utils.SparkUtils
import com.hortonworks.spark.atlas.{RestAtlasClient, AtlasClientConf}
import org.apache.spark.sql.catalyst.catalog.{HiveTableRelation, CatalogTable}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.hive.execution._
import org.apache.spark.sql.util.QueryExecutionListener

class SparkExecutionPlanTracker extends QueryExecutionListener {

    // Skeleton to track QueryExecution of Spark SQL/DF

    // For integration testing only
    private lazy val atlasClientConf = new AtlasClientConf()
      .set(AtlasClientConf.CHECK_MODEL_IN_START.key, "false")
    private lazy val atlasClient = new RestAtlasClient(atlasClientConf)

    override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
      // TODO: We should consider multiple inputs and multiple outs.
      // TODO: We should handle OVERWRITE to remove the old lineage.
      // TODO: We should consider LLAPRelation later

      val relations = qe.sparkPlan.collect {
        case p: LeafExecNode => p
      }
      relations.foreach {
        case r: ExecutedCommandExec =>
          r.cmd match {
            case c : CreateTableCommand =>
              println("Table name in CREATE query: " + c.table.identifier.table)
              val tIdentifier = c.table.identifier
              val db = tIdentifier.database.getOrElse("default")
              val table = tIdentifier.table
              val tableDefinition = SparkUtils.getExternalCatalog().getTable(db, table)
              val dbDefinition = SparkUtils.getExternalCatalog().getDatabase(db)
              val schemaEntities = AtlasEntityUtils.schemaToEntity(tableDefinition.schema, db, table)
              val storageFormatEntity =
                AtlasEntityUtils.storageFormatToEntity(tableDefinition.storage, db, table)
              val dbEntity = AtlasEntityUtils.dbToEntity(dbDefinition)
              val tableEntity = AtlasEntityUtils.tableToEntity(tableDefinition, dbEntity,
                schemaEntities, storageFormatEntity)
              atlasClient.createEntities(
                Seq(dbEntity, storageFormatEntity, tableEntity) ++ schemaEntities)

            case c: InsertIntoHiveTable =>
              println("Table name in INSERT query: " + c.table.identifier.table)
              val child = c.query.asInstanceOf[Project].child
              child match {
                // Case 3. INSERT INTO VALUES
                case ch : LocalRelation => println("Insert table from values()")
                // Case 4. INSERT INTO SELECT
                case ch : SubqueryAlias => println("Insert table from select * from")
                  // FromTable
                  val fromTableIdentifier = ch.child.asInstanceOf[HiveTableRelation].tableMeta.identifier
                  val fromTable = fromTableIdentifier.table
                  val fromDB = fromTableIdentifier.database.getOrElse("default")
                  val tableDefinition = SparkUtils.getExternalCatalog().getTable(fromDB, fromTable)
                  val dbDefinition = SparkUtils.getExternalCatalog().getDatabase(fromDB)
                  val schemaEntities = AtlasEntityUtils.schemaToEntity(tableDefinition.schema, fromDB, fromTable)
                  val storageFormatEntity =
                    AtlasEntityUtils.storageFormatToEntity(tableDefinition.storage, fromDB, fromTable)
                  val dbEntity = AtlasEntityUtils.dbToEntity(dbDefinition)
                  val tableEntity = AtlasEntityUtils.tableToEntity(tableDefinition, dbEntity,
                    schemaEntities, storageFormatEntity)
                  val inputs = Seq(dbEntity, storageFormatEntity, tableEntity) ++ schemaEntities
                  atlasClient.createEntities(inputs)

                  // OutTable
                  val outTableIdentifier = ch.child.asInstanceOf[HiveTableRelation].tableMeta.identifier
                  val outTable = outTableIdentifier.table
                  val outDB = outTableIdentifier.database.getOrElse("default")
                  val outTableDef = SparkUtils.getExternalCatalog().getTable(outDB, outTable)
                  val outDBDef = SparkUtils.getExternalCatalog().getDatabase(outDB)
                  val outTableSchemaEntities = AtlasEntityUtils.schemaToEntity(outTableDef.schema, outDB, outTable)
                  val outTableStorageFormatEntity =
                    AtlasEntityUtils.storageFormatToEntity(outTableDef.storage, outDB, outTable)
                  val outDBEntity = AtlasEntityUtils.dbToEntity(outDBDef)
                  val outTableEntity = AtlasEntityUtils.tableToEntity(outTableDef, outDBEntity,
                    outTableSchemaEntities, outTableStorageFormatEntity)
                  val outputs =
                    Seq(outDBEntity, outTableStorageFormatEntity, outTableEntity) ++ outTableSchemaEntities
                  atlasClient.createEntities(outputs)

                  // create process entity
                  val pEntity = AtlasEntityUtils.processToEntityForTmpTesting(qe, inputs.toList, outputs.toList)
                  atlasClient.createEntities(Seq(pEntity))

                case _ => None
              }

            case c : CreateHiveTableAsSelectCommand =>
              // Case 6. CREATE TABLE AS SELECT
              println("Table name in CTAS query: "
                + c.tableDesc.asInstanceOf[CatalogTable].identifier.table)

            case c : LoadDataCommand =>
              // Case 1. LOAD DATA LOCAL INPATH (from local)
              // Case 2. LOAD DATA INPATH (from HDFS)
              println("Table name in Load (local file) query: " + c.table + c.path)

            case c: CreateDataSourceTableAsSelectCommand =>
              // Case 7. DF.saveAsTable
              println("Table name in saveAsTable query: " + c.table.identifier.table)

            case _ =>
              println("Unknown command")
              None
          }
        case c =>
          None
            // Case 5. FROM ... INSERT (OVERWRITE) INTO t2 INSERT INTO t3
            // CASE LLAP:
            //            case r: RowDataSourceScanExec
            //              if (r.relation.getClass.getCanonicalName.endsWith("dd")) =>
            //              println("close hive connection via " + r.relation.getClass.getCanonicalName)
      }

    }

    override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
      println(s"onFailure: $funcName, $exception")
      println(qe)
    }
}
