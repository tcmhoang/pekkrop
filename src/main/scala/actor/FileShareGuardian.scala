package actor

import model.{DDProtocol, LocalFileProtocol}
import model.DDProtocol.Response
import model.LocalFileProtocol.{CheckFileAvailability, LocalFileCommand}
import org.apache.pekko.actor.typed.receptionist.Receptionist.Register
import org.apache.pekko.actor.typed.receptionist.ServiceKey
import org.apache.pekko.actor.typed.scaladsl.AskPattern.Askable
import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors}
import org.apache.pekko.actor.typed.{ActorRef, ActorSystem, Behavior}
import org.apache.pekko.actor.{ActorPath, RootActorPath}
import org.apache.pekko.cluster.ClusterEvent.{MemberRemoved, MemberUp}
import org.apache.pekko.cluster.ddata.typed.scaladsl.DistributedData
import org.apache.pekko.cluster.ddata.SelfUniqueAddress
import org.apache.pekko.cluster.typed.{Cluster, Subscribe}
import org.apache.pekko.util.Timeout

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.{Either, Failure, Left, Right, Success}

object FileShareGuardian:

  import model.ShareProtocol.*
  import model.DDProtocol.DDCommand

  def apply(): Behavior[Command] = Behaviors setup: context =>
    val key = ServiceKey[Command]("pekkrop")
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

    val distributedDataCoordinator = context spawn (
      DistributedDataCoordinator(),
      "distributed-data-coordinator"
    )
    context.watch(distributedDataCoordinator)

    val localFileManager =
      context.spawn(LocalFileManager(), "local-file-manager")

    context.watch(localFileManager)

    run(localFileManager, distributedDataCoordinator)

  def run(
      localFileManager: ActorRef[LocalFileCommand],
      ddCoordinator: ActorRef[DDCommand]
  ): Behavior[Command] =
    Behaviors.receive: (context, message) =>
      given ActorSystem[_] = context.system
      import org.apache.pekko.actor.typed.scaladsl.AskPattern.schedulerFromActorSystem
      given ActorContext[Command] = context
      given node: SelfUniqueAddress =
        DistributedData(context.system).selfUniqueAddress
      given ExecutionContextExecutor = context.system.executionContext

      message match
        case RegisterFile(filePath) =>
          localFileManager ! LocalFileProtocol
            .RegisterFile(
              filePath,
              ddCoordinator
            )
          Behaviors.same

        case ListAvailableFiles(replyTo) =>
          ddCoordinator ! DDProtocol.GetFileListing(replyTo)
          Behaviors.same

        case RequestFile(fileName, replyTo) =>
          context.log.info(s"Received request to download file: $fileName")
          import scala.concurrent.duration.*
          given Timeout = Timeout(300.milliseconds)
          val logger = context.log
          val self = context.self

          (localFileManager ? (CheckFileAvailability(fileName, _)))
            .map[Either[String, Unit]]:
              case _: LocalFileProtocol.Response.FileFound =>
                Left(s"File $fileName already existed in local node, abort!")
              case _: LocalFileProtocol.Response.FileNotFound => Right(())
            .flatMap[Either[String, (String, Set[String])]]:
              case Left(expectedFailure) =>
                Future.successful(Left(expectedFailure))
              case _: Right[_, _] =>
                (
                  ddCoordinator ? (DDProtocol.GetFileLocations(fileName, _))
                ).map:
                  case Response.DDFileLocation(fileName, hostNodes) =>
                    Right((fileName, hostNodes))
                  case _: model.DDProtocol.Response.DDNotFound =>
                    Left(s"File $fileName not found in dd")
                  case _: model.AvailableFiles =>
                    Left("Cannot resolved with such protocol")
            .onComplete:
              case Failure(exception) => logger.error(exception.getMessage)
              case Success(res)       =>
                res match
                  case Left(value)          => logger.warn(value)
                  case Right((file, nodes)) =>
                    self ! InitiateDownload(file, nodes, replyTo)

          Behaviors.same

        case SendFileTo(fileName, recipientNode, recipientActor) =>
          localFileManager ! LocalFileProtocol.SendFileTo(
            fileName,
            recipientNode,
            recipientActor
          )
          Behaviors.same

        case InitiateDownload(fileName, hostNodes, replyTo) =>
          hostNodes.headOption match // TODO: Implement a more sophisticated selection
            case Some(hostNodeAddress) =>
              val (host, port) = parseHostPort(hostNodeAddress)
              if host.isEmpty || port.isEmpty then
                context.log.warn(
                  s"Could not parse host and port from address: $hostNodeAddress"
                )
                replyTo ! FileTransferFailed(
                  fileName,
                  s"Invalid remote address format for $fileName"
                )
                Behaviors.same
              else
                context.log.info(
                  s"File $fileName found on node: $hostNodeAddress. Initiating download."
                )
                val classicSys =
                  context.system.classicSystem
                val path = RootActorPath(
                  context.self.path.address
                    copy (host = host, port = port),
                  context.system.name
                )
                val castedPath: ActorPath = path
                val remoteActorPath = context.self.path.elements
                  .foldLeft[ActorPath](castedPath)(_.child(_))

                val workerRef = context.spawnAnonymous(
                  FileDownloadWorker(context.self)
                )

                context.log.info(
                  s"Attempting to resolve remote actor via classic selection: $remoteActorPath"
                )

                val logger = context.log
                val self = context.self

                import scala.concurrent.duration.*
                classicSys
                  .actorSelection(remoteActorPath)
                  .resolveOne(3.seconds)
                  .onComplete:
                    case Success(remoteLocalFileManagerRef) =>
                      logger.info(
                        s"Resolved remote actor: $remoteLocalFileManagerRef. Sending HandleSendFileTo command."
                      )
                      replyTo ! FileTransferInitiated(fileName)
                      remoteLocalFileManagerRef ! SendFileTo(
                        fileName,
                        self.path.address.hostPort,
                        workerRef
                      )
                    case Failure(ex) =>
                      logger.error(
                        s"Failed to resolve remote actor for $fileName on $hostNodeAddress: ${ex.getMessage}"
                      )
                      replyTo ! FileTransferFailed(
                        fileName,
                        s"Could not connect to host node: ${ex.getMessage}"
                      )
            case None =>
              context.log.warn(
                "Could not download, there's no host to chose from"
              )
          Behaviors.same
        case cmd: InternalCommand_ => handleInternalCommand(cmd, ddCoordinator)

  private def handleInternalCommand(
      replicatorCmd: InternalCommand_,
      ddCoordinator: ActorRef[DDCommand]
  )(implicit
      context: ActorContext[Command],
      node: SelfUniqueAddress,
      ec: ExecutionContext
  ): Behavior[Command] = replicatorCmd match
    case InternalMemUp_(e) =>
      context.log.info(s"Node is UP: ${e.member.address.hostPort}")
      Behaviors.same
    case InternalMemRm_(e) =>
      context.log.info(s"Node is Removed: ${e.member.address.hostPort}")
      ddCoordinator ! DDProtocol.RemoveNodeFiles(e.member.address.hostPort)
      Behaviors.same

  private def parseHostPort(hostPort: String): (Option[String], Option[Int]) =
    hostPort.split(":").toList match
      case hostStr :: portStr :: Nil =>
        (hostStr.split("@").lastOption, portStr.toIntOption)
      case _ => (None, None)
