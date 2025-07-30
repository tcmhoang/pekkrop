package actor

import org.apache.pekko.actor.typed.receptionist.Receptionist.{Find, Register}
import org.apache.pekko.actor.typed.receptionist.{Receptionist, ServiceKey}
import org.apache.pekko.actor.typed.scaladsl.AskPattern.Askable
import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors}
import org.apache.pekko.actor.typed.{ActorRef, ActorSystem, Behavior, Scheduler}
import org.apache.pekko.cluster.ClusterEvent.{MemberRemoved, MemberUp}
import org.apache.pekko.cluster.typed.{Cluster, Subscribe}
import org.apache.pekko.util.Timeout

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.{Either, Failure, Left, Random, Right, Success}

object FileShareGuardian:

  import model.DDProtocol.{DDCommand, Response}
  import model.LocalFileProtocol.{CheckFileAvailability, LocalFileCommand}
  import model.ShareProtocol.*
  import model.ShareProtocol.Response.*
  import model.{DDProtocol, LocalFileProtocol}
  import scala.concurrent.duration.*

  def apply(): Behavior[Command] = init().narrow

  def init(): Behavior[InternalCommand_] = Behaviors setup: context =>
    given key: ServiceKey[Command] = ServiceKey("pekkrop")
    context.system.receptionist ! Register(key, context.self)
    val memUpAdapter = context.messageAdapter[MemberUp](InternalMemUp_.apply)
    Cluster(context.system).subscriptions ! Subscribe(
      memUpAdapter,
      classOf[MemberUp]
    )

    val memDownAdapter =
      context.messageAdapter[MemberRemoved](InternalMemRm_.apply)
    Cluster(context.system).subscriptions ! Subscribe(
      memDownAdapter,
      classOf[MemberRemoved]
    )

    given dd: ActorRef[DDCommand] = context spawn (
      DistributedDataCoordinator(),
      "distributed-data-coordinator"
    )
    context watch dd

    given lm: ActorRef[LocalFileCommand] =
      context spawn (LocalFileManager(), "local-file-manager")

    context watch lm

    run

  def run(using
      dd: ActorRef[DDCommand],
      lm: ActorRef[LocalFileCommand],
      ck: ServiceKey[Command]
  ): Behavior[InternalCommand_] =
    Behaviors receive: (context, message) =>
      given ActorSystem[_] = context.system
      given ActorContext[InternalCommand_] = context
      given ExecutionContextExecutor = context.system.executionContext
      import org.apache.pekko.actor.typed.scaladsl.AskPattern.schedulerFromActorSystem

      message match
        case InternalMemUp_(e) =>
          context.log.info(s"Node is UP: ${e.member.address.hostPort}")
          Behaviors.same
        case InternalMemRm_(e) =>
          context.log.info(s"Node is Removed: ${e.member.address.hostPort}")
          dd ! DDProtocol.RemoveNodeFiles(e.member.address.hostPort)
          Behaviors.same

        case cmd: Command => handleCommand(cmd)

  private def handleCommand(
      command: Command
  )(using
      dd: ActorRef[DDCommand],
      lm: ActorRef[LocalFileCommand],
      context: ActorContext[InternalCommand_],
      ec: ExecutionContext,
      sys: ActorSystem[_],
      ck: ServiceKey[Command],
      sc: Scheduler,
      tm: Timeout = Timeout(300.milliseconds),
  ): Behavior[InternalCommand_] = command match
    case RegisterFile(filePath) =>
      lm ! LocalFileProtocol
        .RegisterFile(
          filePath,
          dd
        )
      Behaviors.same

    case ListAvailableFiles(replyTo) =>
      dd ? DDProtocol.GetFileListing.apply
      Behaviors.same

    case RequestFile(fileName, replyTo) =>
      context.log.info(s"Received request to download file: $fileName")


      val maybeAvailableHostname = (lm ? (CheckFileAvailability(fileName, _)))
        .map[Either[String, Unit]]:
          case _: LocalFileProtocol.Response.FileFound =>
            Left(s"File $fileName already existed in local node, abort!")
          case _: LocalFileProtocol.Response.FileNotFound => Right(())
        .flatMap[Either[String, Set[String]]]:
          case Left(expectedFailure) =>
            Future.successful(Left(expectedFailure))
          case _: Right[_, _] =>
            (
              dd ? (DDProtocol.GetFileLocations(fileName, _))
            ).map:
              case Response.FileLocation(fileName, hostNodes) =>
                Right(hostNodes)
              case _: DDProtocol.Response.NotFound =>
                Left(s"File $fileName not found in dd")
              case _: AvailableFiles =>
                Left("Cannot resolved with such protocol")

      val currentInstances = context.system.receptionist ? Find(ck)

      val maybeCommand: Future[Either[String, InitiateDownload]] =
        for (mbHosts <- maybeAvailableHostname; ins <- currentInstances)
          yield for host <- mbHosts yield ins match
            case ck.Listing(refs) =>
              InitiateDownload(
                fileName,
                refs.filter(ref => host.contains(ref.path.address.hostPort)),
                replyTo
              )

      val logger = context.log
      val self = context.self

      maybeCommand.onComplete:
        case Failure(exception) => logger error exception.getMessage
        case Success(mbCmd)     =>
          mbCmd match
            case Left(value) => logger warn value
            case Right(cmd)  => self ! cmd

      Behaviors.same

    case SendFileTo(fileName, recipientNode, recipientActor) =>
      lm ! LocalFileProtocol.SendFileTo(
        fileName,
        recipientNode,
        recipientActor
      )
      Behaviors.same

    case InitiateDownload(fileName, hostNodes, replyTo) =>
      if hostNodes.isEmpty then
        context.log.warn(
          "Could not download, there's no host to chose from"
        )
      else
        val chosen = Random nextInt hostNodes.size
        val remoteNode = hostNodes toList chosen
        val workerRef = context spawnAnonymous FileDownloadWorker(
          context.self,
          remoteNode.path.address.toString
        )

        replyTo ! FileTransferInitiated(fileName)
        remoteNode ! SendFileTo(
          fileName,
          context.self.path.address.hostPort,
          workerRef
        )

      Behaviors.same
