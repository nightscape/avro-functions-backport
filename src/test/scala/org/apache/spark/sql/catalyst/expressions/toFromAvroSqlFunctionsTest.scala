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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.SparkSession
import RegisterFunction.fromAvroFunctionName
import com.sksamuel.avro4s._
import java.io.ByteArrayOutputStream

class RegisterFunctionSuite extends munit.FunSuite {

  val sparkFixture = FunFixture[SparkSession](
    setup = { test =>
      SparkSession
        .builder()
        .appName("test")
        .master("local")
        .config("spark.sql.extensions", classOf[AvroExtensions].getName)
        .getOrCreate()
    },
    teardown = { spark =>
      spark.stop()
    }
  )
  sparkFixture.test("registers with SparkSession") { spark =>
    import spark.implicits._
    val obtained = spark.catalog
      .listFunctions()
      .where($"name".contains("from"))
      .collect()
      .map(_.name)
    assert(clue(obtained).contains(RegisterFunction.fromAvroFunctionName))
  }
  sparkFixture.test("can be called from SQL") { spark =>
    import spark.implicits._
    val schema = AvroSchema[Ingredient]
    val encoder = Encoder[Ingredient]
    val data = Seq(Ingredient("sugar", 1.0, 2.0))
    val outputStream = new ByteArrayOutputStream()
    val os = AvroOutputStream
      .binary[Ingredient](encoder)
      .to(outputStream)
      .build(schema)
    os.write(data)
    os.flush()
    os.close()

    val bytes = outputStream.toByteArray
    val df = spark.createDataset(Seq(AvroData(bytes))).toDF()
    df.registerTempTable("df")
    df.show()
    val parsed = spark
      .sql(
        s"""WITH parsed AS (SELECT $fromAvroFunctionName(data, '$schema', map()) AS p from df) SELECT p.* FROM parsed"""
      )
      .as[Ingredient]
    parsed.show()
    assert(clue(parsed.collect()).contains(data.head))
  }
}

case class Ingredient(nam: String, sugar: Double, fat: Double)

case class AvroData(data: Array[Byte])
