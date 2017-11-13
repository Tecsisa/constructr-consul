/*
 * Copyright 2016, 2017 TECNOLOGIA, SISTEMAS Y APLICACIONES S.L. <http://www.tecsisa.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tecsisa.constructr.coordination.consul

import akka.Done
import akka.actor.{ ActorSystem, AddressFromURIString }
import akka.testkit.TestDuration
import com.typesafe.config.ConfigFactory
import org.scalatest.{ BeforeAndAfterAll, Matchers, WordSpec }

import scala.concurrent.duration.{ Duration, DurationInt, FiniteDuration }
import scala.concurrent.{ Await, Awaitable }
import scala.util.Random

object ConsulCoordinationSpec {

  private val coordinationHost = {
    val dockerHostPattern = """tcp://(\S+):\d{1,5}""".r
    sys.env
      .get("DOCKER_HOST")
      .collect { case dockerHostPattern(address) => address }
      .getOrElse("127.0.0.1")
  }
}

class ConsulCoordinationSpec extends WordSpec with Matchers with BeforeAndAfterAll {
  import ConsulCoordinationSpec._

  private implicit val system = {
    val config =
      ConfigFactory
        .parseString(s"constructr.coordination.host = $coordinationHost")
        .withFallback(ConfigFactory.load())
    ActorSystem("default", config)
  }

  private val address1 = AddressFromURIString("akka.tcp://default@a:2552")
  private val address2 = AddressFromURIString("akka.tcp://default@b:2552")

  "ConsulCoordination" should {
    "correctly interact with consul" in {
      val coordination = new ConsulCoordination(randomString(), system)

      // Getting nodes
      resultOf(coordination.getNodes()) shouldBe 'empty

      // Lock (ttl >= 10s)
      resultOf(coordination.lock(address1, 10.seconds)) shouldBe true
      resultOf(coordination.lock(address1, 10.seconds)) shouldBe true
      resultOf(coordination.lock(address2, 10.seconds)) shouldBe false

      // Add self
      resultOf(coordination.addSelf(address1, 10.seconds)) shouldBe Done
      resultOf(coordination.getNodes()) shouldBe Set(address1)

      // Refresh
      resultOf(coordination.refresh(address1, 10.seconds)) shouldBe Done
      resultOf(coordination.getNodes()) shouldBe Set(address1)

      // Close
      resultOf(coordination.close()) shouldBe Done
      resultOf(coordination.getNodes()) shouldBe 'empty
    }
  }

  override protected def afterAll() = {
    Await.ready(system.terminate(), Duration.Inf)
    super.afterAll()
  }

  private def resultOf[A](awaitable: Awaitable[A], max: FiniteDuration = 3.seconds.dilated) =
    Await.result(awaitable, max)

  private def randomString() = math.abs(Random.nextInt).toString
}
