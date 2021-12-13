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

package org.apache.kyuubi.engine.spark

import java.util

import scala.collection.JavaConverters._

import org.apache.kyuubi.WithKyuubiServer
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.operation.HiveJDBCTestHelper
import org.apache.kyuubi.plugin.SessionConfAdvisor

class SparkSqlEngineSuite extends WithKyuubiServer with HiveJDBCTestHelper {
  override protected val conf: KyuubiConf = {
    KyuubiConf()
      .set(SESSION_CONF_IGNORE_LIST.key, "kyuubi.abc.xyz,spark.sql.abc.xyz,spark.sql.abc.var")
      .set(SESSION_CONF_RESTRICT_LIST.key, "kyuubi.xyz.abc,spark.sql.xyz.abc,spark.sql.xyz.abc.var")
      .set(SESSION_CONF_ADVISOR.key, classOf[TestSessionConfAdvisor].getName)
  }

  test("ignore config via system settings") {
    val sessionConf = Map("kyuubi.abc.xyz" -> "123", "kyuubi.abc.xyz0" -> "123")
    val sparkHiveConfs = Map("spark.sql.abc.xyz" -> "123", "spark.sql.abc.xyz0" -> "123")
    val sparkHiveVars = Map("spark.sql.abc.var" -> "123", "spark.sql.abc.var0" -> "123")
    withSessionConf(sessionConf)(sparkHiveConfs)(sparkHiveVars) {
      withJdbcStatement() { statement =>
        Seq("spark.sql.abc.xyz", "spark.sql.abc.var").foreach { key =>
          val rs1 = statement.executeQuery(s"SET ${key}0")
          assert(rs1.next())
          assert(rs1.getString("value") === "123")
          val rs2 = statement.executeQuery(s"SET $key")
          assert(rs2.next())
          assert(rs2.getString("value") === "<undefined>", "ignored")
        }
      }
    }
  }

  test("restricted config via system settings") {
    val sessionConfMap = Map("kyuubi.xyz.abc" -> "123", "kyuubi.abc.xyz" -> "123")
    withSessionConf(sessionConfMap)(Map.empty)(Map.empty) {
      withJdbcStatement() { statement =>
        sessionConfMap.keys.foreach { key =>
          val rs = statement.executeQuery(s"SET $key")
          assert(rs.next())
          assert(
            rs.getString("value") === "<undefined>",
            "session configs do not reach on server-side")
        }

      }
    }

    withSessionConf(Map.empty)(Map("spark.sql.xyz.abc" -> "123"))(Map.empty) {
      assertJDBCConnectionFail()
    }

    withSessionConf(Map.empty)(Map.empty)(Map("spark.sql.xyz.abc.var" -> "123")) {
      assertJDBCConnectionFail()
    }
  }

  test("Fail connections on invalid sub domains") {
    Seq("/", "/tmp", "", "abc/efg", ".", "..").foreach { invalid =>
      val sparkHiveConfigs = Map(
        ENGINE_SHARE_LEVEL.key -> "USER",
        ENGINE_SHARE_LEVEL_SUBDOMAIN.key -> invalid)
      withSessionConf(Map.empty)(sparkHiveConfigs)(Map.empty) {
        assertJDBCConnectionFail()
      }
    }
  }

  test("Engine isolation with sub domain configurations") {
    val sparkHiveConfigs = Map(
      ENGINE_SHARE_LEVEL.key -> "USER",
      ENGINE_SHARE_LEVEL_SUBDOMAIN.key -> "spark",
      "spark.driver.memory" -> "1000M")
    var mem: String = null
    withSessionConf(Map.empty)(sparkHiveConfigs)(Map.empty) {
      withJdbcStatement() { statement =>
        val rs = statement.executeQuery(s"SET spark.driver.memory")
        assert(rs.next())
        mem = rs.getString(2)
        assert(mem === "1000M")
      }
    }

    val sparkHiveConfigs2 = Map(
      ENGINE_SHARE_LEVEL.key -> "USER",
      ENGINE_SHARE_LEVEL_SUBDOMAIN.key -> "spark",
      "spark.driver.memory" -> "1001M")
    withSessionConf(Map.empty)(sparkHiveConfigs2)(Map.empty) {
      withJdbcStatement() { statement =>
        val rs = statement.executeQuery(s"SET spark.driver.memory")
        assert(rs.next())
        mem = rs.getString(2)
        assert(mem === "1000M", "The sub-domain is same, so the engine reused")
      }
    }

    val sparkHiveConfigs3 = Map(
      ENGINE_SHARE_LEVEL.key -> "USER",
      ENGINE_SHARE_LEVEL_SUBDOMAIN.key -> "kyuubi",
      "spark.driver.memory" -> "1002M")
    withSessionConf(Map.empty)(sparkHiveConfigs3)(Map.empty) {
      withJdbcStatement() { statement =>
        val rs = statement.executeQuery(s"SET spark.driver.memory")
        assert(rs.next())
        mem = rs.getString(2)
        assert(mem === "1002M", "The sub-domain is changed, so the engine recreated")
      }
    }
  }

  test("test session conf plugin") {
    withSessionConf()(Map())(Map("spark.k1" -> "v0", "spark.k3" -> "v4")) {
      withJdbcStatement() { statement =>
        val r1 = statement.executeQuery("set spark.k1")
        assert(r1.next())
        assert(r1.getString(2) == "v0")

        val r2 = statement.executeQuery("set spark.k2")
        assert(r2.next())
        assert(r2.getString(2) == "v2")

        val r3 = statement.executeQuery("set spark.k3")
        assert(r3.next())
        assert(r3.getString(2) == "v3")

        val r4 = statement.executeQuery("set spark.k4")
        assert(r4.next())
        assert(r4.getString(2) == "v4")
      }
    }
  }

  override protected def jdbcUrl: String = getJdbcUrl

}

class TestSessionConfAdvisor extends SessionConfAdvisor {
  override def getConfDefault(
      user: String, sessionConf: util.Map[String, String]): util.Map[String, String] = {
    Map("spark.k1" -> "v1", "spark.k2" -> "v2").asJava
  }

  override def getConfOverlay(
      user: String, sessionConf: util.Map[String, String]): util.Map[String, String] = {
    Map("spark.k3" -> "v3", "spark.k4" -> "v4").asJava
  }
}
