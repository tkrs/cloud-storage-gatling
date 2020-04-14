package loadtest

import java.io.ByteArrayOutputStream
import java.{util => ju}

import com.google.cloud.storage.Storage.BlobListOption
import com.google.cloud.storage.{Option => _, _}
import io.gatling.commons.stats.KO
import io.gatling.commons.stats.OK
import io.gatling.commons.util.Clock
import io.gatling.commons.validation.Failure
import io.gatling.commons.validation.Success
import io.gatling.commons.validation.Validation
import io.gatling.core.CoreComponents
import io.gatling.core.Predef._
import io.gatling.core.action.Action
import io.gatling.core.action.RequestAction
import io.gatling.core.action.builder.ActionBuilder
import io.gatling.core.check.Check
import io.gatling.core.check.CheckResult
import io.gatling.core.config.GatlingConfiguration
import io.gatling.core.protocol.Protocol
import io.gatling.core.protocol.ProtocolComponents
import io.gatling.core.protocol.ProtocolKey
import io.gatling.core.protocol.Protocols
import io.gatling.core.session.Expression
import io.gatling.core.session.StaticStringExpression
import io.gatling.core.stats.StatsEngine
import io.gatling.core.structure.ScenarioContext

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.util.Random

class CloudStorageSimulation extends Simulation {
  import CloudStorageSimulation._

  val service = StorageOptions.getDefaultInstance().getService()
  val names: Vector[String] =
    service
      .list(bucket, BlobListOption.prefix(prefix))
      .iterateAll()
      .asScala
      .map(_.getName())
      .toVector

  val random = new Random()
  def feeder(i: Int) =
    Iterator.continually(Map("randomName" -> names(random.nextInt(names.size))))
  val storageProtocol = CloudStorageProtocol(service)

  def scn(i: Int) =
    scenario(s"cloudstorage-$i")
      .feed(feeder(i))
      .exec(CloudStorageBuilder("GET"))

  setUp(
    (1 to 16)
      .map(i => scn(i).inject(constantConcurrentUsers(500).during(10.seconds)))
      .toList
  ).protocols(CloudStorageProtocol(service))
}

object CloudStorageSimulation {
  val bucket = sys.env.getOrElse("CLOUD_STORAGE_BUCKET", "load-test-3u90f754rw8wu")
  val prefix = "ids"

  case class CloudStorageProtocol(servie: Storage) extends Protocol
  object CloudStorageProtocol {
    val protocolKey =
      new ProtocolKey[CloudStorageProtocol, CloudStorageComponents] {
        def protocolClass: Class[Protocol] = classOf[CloudStorageProtocol].asInstanceOf[Class[Protocol]]
        def defaultProtocolValue(configuration: GatlingConfiguration): CloudStorageProtocol = ???
        def newComponents(coreComponents: CoreComponents): CloudStorageProtocol => CloudStorageComponents =
          CloudStorageComponents(coreComponents, _)
      }
  }

  case class CloudStorageComponents(
      coreComponents: CoreComponents,
      protocol: CloudStorageProtocol
  ) extends ProtocolComponents {
    def onStart: Session => Session = ProtocolComponents.NoopOnStart
    def onExit: Session => Unit = ProtocolComponents.NoopOnExit
  }

  case class CloudStorageBuilder(tag: String) extends ActionBuilder {
    def build(ctx: ScenarioContext, next: Action): Action = {

      val components = ctx.protocolComponentsRegistry.components(
        CloudStorageProtocol.protocolKey
      )
      GetAction(ctx, tag, next, components)
    }
  }

  case class GetAction(
      ctx: ScenarioContext,
      name: String,
      next: Action,
      components: CloudStorageComponents
  ) extends RequestAction {

    def statsEngine: StatsEngine = ctx.coreComponents.statsEngine
    def clock: Clock = ctx.coreComponents.clock

    def requestName: Expression[String] =
      session => Success(session.attributes("randomName").asInstanceOf[String])

    def sendRequest(requestName: String, session: Session): Validation[Unit] = {
      if (ctx.throttled)
        ctx.coreComponents.throttler
          .throttle(
            session.scenario,
            () => run(components.protocol.servie, session, requestName)
          )
      else
        run(components.protocol.servie, session, requestName)
    }

    private[this] val check: Check[Option[Array[Byte]]] =
      new Check[Option[Array[Byte]]] {
        def check(
            response: Option[Array[Byte]],
            session: Session,
            preparedCache: ju.Map[Any, Any]
        ): Validation[CheckResult] = {
          response match {
            case v @ Some(value) if value.nonEmpty =>
              Success(CheckResult(v, None))
            case _ =>
              Failure("response is null")
          }
        }
      }

    private def run(
        service: Storage,
        session: Session,
        requestName: String
    ): Unit = {
      val startTimestamp = clock.nowMillis
      val blobId = BlobId.of(bucket, requestName)
      val result = for {
        blob <- Option(service.get(blobId))
      } yield {
        val out = new ByteArrayOutputStream(1024 * 300)
        blob.downloadTo(out)
        out.toByteArray()
      }
      val endTimestamp = clock.nowMillis

      val (checkSaveUpdated, checkError) = Check.check[Option[Array[Byte]]](
        result,
        session,
        check :: Nil,
        null
      )

      val status = if (checkError.isEmpty) OK else KO
      val errorMessage = checkError.map(_.message)
      val withStatus =
        if (status == KO) checkSaveUpdated.markAsFailed else checkSaveUpdated
      statsEngine.logResponse(
        withStatus,
        requestName,
        startTimestamp = startTimestamp,
        endTimestamp = endTimestamp,
        status = status,
        responseCode = None,
        message = errorMessage
      )
      val newSession =
        withStatus.logGroupRequestTimings(
          startTimestamp = startTimestamp,
          endTimestamp = endTimestamp
        )
      if (status == KO) {
        logger.info(
          s"Request '$requestName' failed for user ${session.userId}: ${errorMessage.getOrElse("")}"
        )
      }
      next ! newSession
    }
  }
}
