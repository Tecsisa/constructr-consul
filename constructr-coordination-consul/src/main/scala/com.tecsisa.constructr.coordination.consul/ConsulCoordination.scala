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

package com.tecsisa.constructr.coordination
package consul

import akka.Done
import akka.actor.{ ActorSystem, Address, AddressFromURIString }
import akka.event.{ Logging, LoggingAdapter }
import akka.http.scaladsl.Http
import akka.http.scaladsl.client.RequestBuilding.{ Get, Put }
import akka.http.scaladsl.model.MediaTypes.`application/json`
import akka.http.scaladsl.model.StatusCodes.{ NotFound, OK }
import akka.http.scaladsl.model.headers.{ ModeledCustomHeader, ModeledCustomHeaderCompanion }
import akka.http.scaladsl.model.{ HttpEntity, HttpRequest, HttpResponse, RequestEntity, ResponseEntity, StatusCode, Uri }
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{ Flow, Sink, Source }
import io.circe.Json
import io.circe.parser.parse
import de.heikoseeberger.constructr.coordination.Coordination
import scala.concurrent.Future
import scala.concurrent.duration.{ Duration, FiniteDuration }
import scala.util.{ Success, Try }
import java.nio.charset.StandardCharsets.UTF_8
import java.util.Base64
import java.util.Base64.{ getUrlDecoder, getUrlEncoder }

object ConsulCoordination {

  private final case class ConsulCoordinationSettings(
      host: String,
      port: Int,
      agentName: String,
      https: Boolean = false,
      accessToken: Option[String] = None
  )

  private object ConsulCoordinationSettings {
    def apply(actorSystem: ActorSystem): ConsulCoordinationSettings = {
      val config = actorSystem.settings.config
      val host   = config.getString("constructr.coordination.host")
      val port   = config.getInt("constructr.coordination.port")
      val agentName = Try(config.getString("constructr.consul.agent-name"))
        .getOrElse("")
      val https = Try(config.getBoolean("constructr.consul.https"))
        .getOrElse(false)
      val accessToken = Try(config.getString("constructr.consul.access-token")).toOption

      ConsulCoordinationSettings(host, port, agentName, https, accessToken)
    }
  }

  private final class ConsulToken(val value: String) extends ModeledCustomHeader[ConsulToken] {
    def companion: ModeledCustomHeaderCompanion[ConsulToken] = ConsulToken
    def renderInRequests: Boolean                            = true
    def renderInResponses: Boolean                           = false
  }

  private object ConsulToken extends ModeledCustomHeaderCompanion[ConsulToken] {
    val name: String                           = "X-Consul-Token"
    def parse(value: String): Try[ConsulToken] = Success(new ConsulToken(value))
  }

  final case class UnexpectedStatusCode(uri: Uri, statusCode: StatusCode)
      extends RuntimeException(s"Unexpected status code $statusCode for URI $uri")

  private def toSeconds(duration: Duration) = (duration.toSeconds + 1).toString
}

final class ConsulCoordination(
    clusterName: String,
    system: ActorSystem
) extends Coordination {

  import ConsulCoordination._

  private implicit val mat = ActorMaterializer()(system)

  import mat.executionContext

  type SessionId = String

  @volatile var stateSession: Option[SessionId] = None

  private implicit val logger: LoggingAdapter = Logging.getLogger(system, this)

  private val settings = ConsulCoordinationSettings(system)
  logger.debug("Initializing Consul Coordination using settings: {}", settings)

  private val v1Uri = Uri("/v1")

  private val kvUri = v1Uri.withPath(v1Uri.path / "kv")

  private val sessionUri = v1Uri.withPath(v1Uri.path / "session")

  private val baseUri = kvUri.withPath(kvUri.path / "constructr" / clusterName)

  private val nodesUri = baseUri.withPath(baseUri.path / "nodes")

  implicit private val outgoingConnection
    : Flow[HttpRequest, HttpResponse, Future[Http.OutgoingConnection]] =
    createOutgoingConnection(system)

  def getNodes(): Future[Set[Address]] = {
    def unmarshalNodes(entity: ResponseEntity): Future[Set[Address]] = {
      def toNodes(s: String) = {
        def jsonToNode(json: Json) = {
          val init = nodesUri.path.toString.stripPrefix(kvUri.path.toString)
          val key =
            json.hcursor
              .get[String]("Key")
              .fold(throw _, identity)
              .substring(init.length)
          val uri = new String(getUrlDecoder.decode(key), UTF_8)
          AddressFromURIString(uri)
        }
        parseJson[Address](s, jsonToNode)
      }
      Unmarshal(entity).to[String].map(toNodes)
    }
    val uri = nodesUri.withQuery(Uri.Query("recurse"))
    send(Get(uri)).flatMap {
      case HttpResponse(OK, _, entity, _) => unmarshalNodes(entity)
      case HttpResponse(NotFound, _, entity, _) =>
        ignore(entity).map(_ => Set.empty)
      case HttpResponse(other, _, entity, _) =>
        ignore(entity).map(_ => throw UnexpectedStatusCode(uri, other))
    }
  }

  def lock(self: Address, ttl: FiniteDuration): Future[Boolean] = {
    val uriLock = baseUri.withPath(baseUri.path / "lock")
    def readLock() = {
      def unmarshallLockHolder(entity: ResponseEntity) = {
        def toLockHolder(s: String) = {
          def jsonToNode(json: Json) = {
            val value =
              json.hcursor.get[String]("Value").fold(throw _, identity)
            new String(Base64.getUrlDecoder.decode(value), UTF_8)
          }
          parseJson[String](s, jsonToNode).head
        }
        Unmarshal(entity).to[String].map(toLockHolder)
      }
      send(Get(uriLock)).flatMap {
        case HttpResponse(OK, _, entity, _) =>
          unmarshallLockHolder(entity).map(Some(_))
        case HttpResponse(NotFound, _, entity, _) =>
          ignore(entity).map(_ => None)
        case HttpResponse(other, _, entity, _) =>
          ignore(entity).map(_ => throw UnexpectedStatusCode(nodesUri, other))
      }
    }
    def writeLock() = {
      val lockWithNewSession = for {
        sessionId <- createSession(ttl)
        result <- putKeyWithSession(
          uriLock,
          sessionId,
          HttpEntity(self.toString)
        )
      } yield result
      lockWithNewSession.map(isLocked => if (isLocked) true else false)
    }
    def updateLock() = {
      val lockWithPreviousSession = for {
        Some(sessionId) <- retrieveSessionForKey(uriLock)
        result          <- renewSession(sessionId) if result
      } yield sessionId
      lockWithPreviousSession.map(_ => true) fallbackTo writeLock()
    }
    readLock().flatMap {
      case Some(lockHolder) if lockHolder == self.toString => updateLock()
      case Some(_)                                         => Future.successful(false)
      case None                                            => writeLock()
    }
  }

  def addSelf(self: Address, ttl: FiniteDuration): Future[Done] =
    createIfNotExist(self, ttl)

  def createIfNotExist(self: Address, ttl: FiniteDuration): Future[Done] = {
    val node   = getUrlEncoder.encodeToString(self.toString.getBytes(UTF_8))
    val keyUri = nodesUri.withPath(nodesUri.path / node)
    val addSelfWithPreviousSession = for {
      Some(sessionId) <- retrieveSessionForKey(keyUri)
      result          <- renewSession(sessionId) if result
    } yield sessionId // it will fail if there's no session or the renewal went wrong
    val addSelftWithNewSession = for {
      sessionId <- createSession(ttl)
      result    <- putKeyWithSession(keyUri, sessionId) if result
    } yield sessionId // it will fail if it couldn't acquire the key with the new session
    addSelfWithPreviousSession.fallbackTo(addSelftWithNewSession).map { res =>
      stateSession = Some(res)
      Done
    }
  }

  def refresh(self: Address, ttl: FiniteDuration): Future[Done] =
    Future.fromTry(getSessionId).flatMap { sessionId =>
      val uri = sessionUri.withPath(sessionUri.path / "renew" / sessionId)
      send(Put(uri)).flatMap {
        case HttpResponse(OK, _, entity, _) => ignore(entity).map(_ => Done)
        case HttpResponse(NotFound, _, entity, _) =>
          ignore(entity)
            .flatMap { _ =>
              logger.warning(
                "Unable to refresh, session {} not found. Creating a new one",
                sessionId
              )
              createIfNotExist(self, ttl)
            }
            .map(_ => Done)
        case HttpResponse(other, _, entity, _) =>
          ignore(entity).map(_ => throw UnexpectedStatusCode(uri, other))
      }
    }

  def close(): Future[Done] =
    Future.fromTry(getSessionId).flatMap(destroySession)

  private def getSessionId: Try[SessionId] =
    Try(
      stateSession.getOrElse(
        throw new IllegalStateException(
          "It wasn't possible to get a valid Consul `sessionId` for refreshing"
        ))
    )

  private def putKeyWithSession(
      keyUri: Uri,
      sessionId: SessionId,
      content: RequestEntity = HttpEntity.Empty
  ) = {
    val uri = keyUri.withQuery(Uri.Query("acquire" -> sessionId))
    send(Put(uri, content)).flatMap {
      case HttpResponse(OK, _, entity, _) =>
        Unmarshal(entity).to[String].map(_.toBoolean)
      case HttpResponse(other, _, entity, _) =>
        ignore(entity).map(_ => throw UnexpectedStatusCode(uri, other))
    }
  }

  private def retrieveSessionForKey(keyUri: Uri) = {
    def unmarshalSessionKey(entity: ResponseEntity) = {
      def toSession(s: String) = {
        def jsonToNode(json: Json) =
          json.hcursor.get[String]("Session").fold(throw _, identity)
        parseJson[String](s, jsonToNode).head
      }
      Unmarshal(entity).to[String].map(toSession)
    }
    send(Get(keyUri)).flatMap {
      case HttpResponse(OK, _, entity, _) =>
        unmarshalSessionKey(entity).map(Some(_))
      case HttpResponse(NotFound, _, entity, _) =>
        ignore(entity).map(_ => None)
      case HttpResponse(other, _, entity, _) =>
        ignore(entity).map(_ => throw UnexpectedStatusCode(keyUri, other))
    }
  }

  private def renewSession(sessionId: SessionId) = {
    val uri = sessionUri.withPath(sessionUri.path / "renew" / sessionId)
    send(Put(uri)).flatMap {
      case HttpResponse(OK, _, entity, _) => ignore(entity).map(_ => true)
      case HttpResponse(NotFound, _, entity, _) =>
        ignore(entity).map(_ => false)
      case HttpResponse(other, _, entity, _) =>
        ignore(entity).map(_ => throw UnexpectedStatusCode(uri, other))
    }
  }

  private def createSession(ttl: FiniteDuration) = {
    def unmarshalSessionId(entity: ResponseEntity) = {
      def toSession(s: String) =
        parse(s)
          .fold(throw _, identity)
          .hcursor
          .get[String]("ID")
          .fold(throw _, identity)
      Unmarshal(entity).to[String].map(toSession)
    }
    val sessionEntity = {
      val base = s"""{"behavior": "delete", "ttl": "${toSeconds(ttl)}s""""
      val data = if (settings.agentName.isEmpty) {
        logger.warning(
          "If agent-name is not defined, this may cause problems (see Consul session internals)"
        )
        base + "}"
      } else base + s""", "node": "${settings.agentName}"}"""
      HttpEntity(`application/json`, data)
    }
    val createSessionUri = sessionUri.withPath(sessionUri.path / "create")
    send(Put(createSessionUri, sessionEntity)).flatMap {
      case HttpResponse(OK, _, entity, _) => unmarshalSessionId(entity)
      case HttpResponse(other, _, entity, _) =>
        ignore(entity).map(
          _ => throw UnexpectedStatusCode(createSessionUri, other)
        )
    }
  }

  private def destroySession(sessionId: SessionId): Future[Done] = {
    // Create new ActorSystem so that we can use akka-http to destroy the Consul session.
    // That's because the original ActorSystem is terminating, so it is useless for akka-http
    // purposes (would throw a java.lang.IllegalStateException: cannot create children while
    // terminating or terminated)
    import com.typesafe.config.ConfigFactory
    val cleanupSystem: ActorSystem =
      ActorSystem("constructr-cleanup", ConfigFactory.defaultReference())
    implicit val mat: ActorMaterializer = ActorMaterializer()(cleanupSystem)
    val uri: Uri                        = sessionUri.withPath(sessionUri.path / "destroy" / sessionId)

    implicit val outgoingConnection
      : Flow[HttpRequest, HttpResponse, Future[Http.OutgoingConnection]] =
      createOutgoingConnection(cleanupSystem)
    val logger = cleanupSystem.log

    send(Put(uri))
      .flatMap {
        case HttpResponse(OK, _, entity, _) =>
          Unmarshal(entity)
            .to[String]
            .map { destroyed =>
              if (destroyed == "true") {
                logger.info("Consul session successfully destroyed")
              } else {
                logger.warning("Consul session could not be destroyed: " +
                  "session might not exist or be already expired")
              }
              Done
            }
        case HttpResponse(_, _, entity, _) =>
          Unmarshal(entity).to[String].map { response =>
            val msg =
              s"Consul session could not be destroyed because something went wrong: $response"
            logger.error(msg)
            throw new RuntimeException(msg)
          }
      }
      .andThen {
        case _ =>
          mat.shutdown()
          cleanupSystem.terminate()
      }
  }

  private def createOutgoingConnection(
      system: ActorSystem): Flow[HttpRequest, HttpResponse, Future[Http.OutgoingConnection]] =
    if (settings.https)
      Http(system).outgoingConnectionHttps(settings.host, settings.port)
    else
      Http(system).outgoingConnection(settings.host, settings.port)

  private def parseJson[T](s: String, f: Json => T) = {
    import cats.syntax.either._ // for Scala 2.11
    parse(s).fold(throw _, identity).as[Set[Json]].getOrElse(Set.empty).map(f)
  }

  private def send(baseRequest: HttpRequest)(
      implicit mat: ActorMaterializer,
      oc: Flow[HttpRequest, HttpResponse, Future[Http.OutgoingConnection]]) = {
    def request: HttpRequest =
      settings.accessToken.map { token =>
        val newHeaders = baseRequest.headers :+ ConsulToken(token)
        baseRequest.copy(headers = newHeaders)
      } getOrElse baseRequest

    Source
      .single(request)
      .log("constructr-coordination-consul-requests")
      .via(oc)
      .log("constructr-coordination-consul-responses")
      .runWith(Sink.head)
  }

  private def ignore(entity: ResponseEntity) =
    entity.dataBytes.runWith(Sink.ignore)
}
