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

import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.hive.execution._
import org.apache.spark.sql.util.QueryExecutionListener

class SparkExecutionPlanTracker extends QueryExecutionListener {

  // Skeleton to track QueryExecution of Spark SQL/DF

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
              println("Table name in CREATE query: "
                + r.cmd.asInstanceOf[CreateTableCommand].table.identifier.table)

            case c: InsertIntoHiveTable =>
              // Case 3. INSERT INTO VALUES
              // Case 4. INSERT INTO SELECT
              println("Table name in INSERT query: "
                + r.cmd.asInstanceOf[InsertIntoHiveTable].table.identifier.table)
              val child = r.cmd.asInstanceOf[InsertIntoHiveTable].query.asInstanceOf[Project].child
              child match {
                case ch : LocalRelation => println("Insert table from values()")
                case ch : SubqueryAlias => println("Insert table from select * from")
                case _ => None
              }

            case c : CreateHiveTableAsSelectCommand =>
              // Case 6. CREATE TABLE AS SELECT
              println("Table name in CTAS query: "
                + r.cmd.asInstanceOf[CreateHiveTableAsSelectCommand]
                .tableDesc.asInstanceOf[CatalogTable].identifier.table)

            case c : LoadDataCommand =>
              // Case 1. LOAD DATA LOCAL INPATH (from local)
              // Case 2. LOAD DATA INPATH (from HDFS)
              println("Table name in Load (local file) query: "
                + r.cmd.asInstanceOf[LoadDataCommand].table + r.cmd.asInstanceOf[LoadDataCommand].path)

            case c: CreateDataSourceTableAsSelectCommand =>
              // Case 7. DF.saveAsTable
              println("Table name in saveAsTable query: "
                + r.cmd.asInstanceOf[CreateDataSourceTableAsSelectCommand].table.identifier.table)

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
