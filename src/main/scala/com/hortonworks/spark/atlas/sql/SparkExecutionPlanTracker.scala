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

import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener

class SparkExecutionPlanTracker extends QueryExecutionListener {

  // Skeleton to track QueryExecution of Spark SQL/DF

    override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
      println(s"onSuccess: $funcName, $durationNs")
      println(qe) 
      // TODO: We should consider multiple inputs and multiple outs.
      // TODO: We should handle OVERWRITE to remove the old lineage.
      // TODO: We should consider LLAPRelation later

      // Case 1. LOAD DATA LOCAL INPATH (from local)
      // Case 2. LOAD DATA INPATH (from HDFS)
      // Case 3. INSERT INTO VALUES
      // Case 4. INSERT INTO SELECT
      // Case 5. FROM ... INSERT (OVERWRITE) INTO t2 INSERT INTO t3
      // Case 6. CREATE TABLE AS SELECT
      // Case 7. DF.saveAsTable
    }

    override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
      println(s"onFailure: $funcName, $exception")
      println(qe)
    }
}
