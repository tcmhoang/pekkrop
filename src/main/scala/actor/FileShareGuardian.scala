package actor

import model.DDProtocol
import model.DDProtocol.{GetFileListing, RemoveNodeFiles}
import org.apache.pekko.actor.typed.receptionist.Receptionist.Register
import org.apache.pekko.actor.typed.receptionist.ServiceKey
import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors}
import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.actor.{ActorPath, ActorSystem, RootActorPath}
import org.apache.pekko.cluster.ClusterEvent.{MemberRemoved, MemberUp}
import org.apache.pekko.cluster.ddata.typed.scaladsl.Replicator.*
import org.apache.pekko.cluster.ddata.typed.scaladsl.{
  DistributedData,
  Replicator
}
import org.apache.pekko.cluster.ddata.{
  LWWMap,
  LWWMapKey,
  ORSet,
  SelfUniqueAddress
}
import org.apache.pekko.cluster.typed.{Cluster, Subscribe}

import java.nio.file.{Files, Path}
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}
import scala.util.{Failure, Success}

object FileShareGuardian:

  import model.ShareProtocol.*
  import model.UploadProtocol.UploadFile
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

    running(distributedDataCoordinator)

  def running(
      ddCoordinator: ActorRef[DDCommand],
      localFiles: Map[String, Path] = Map.empty
  ): Behavior[Command] =
    Behaviors.receive: (context, message) =>
      given ActorContext[Command] = context

      given node: SelfUniqueAddress =
        DistributedData(context.system).selfUniqueAddress

      given ExecutionContextExecutor = context.system.executionContext

      message match
        case RegisterFile(filePath) =>
          val fileName = filePath.getFileName.toString
          if Files.exists(filePath) && Files.isReadable(filePath) then
            context.log.info(s"Registered local file: $fileName at $filePath")
            ddCoordinator ! DDProtocol.RegisterFile(
              fileName,
              context.self.path.address.hostPort
            )
            running(ddCoordinator, localFiles + (fileName -> filePath))
          else
            context.log.warn(
              s"Cannot register file: $filePath. It does not exist or is not readable."
            )
            Behaviors.same

        case ListAvailableFiles(replyTo) =>
          ddCoordinator ! DDProtocol.GetFileListing(replyTo)
          Behaviors.same

        case RequestFile(fileName, replyTo) =>
          context.log.info(s"Received request to download file: $fileName")

          if localFiles contains fileName then
            context.log.warn(s"Already has file $fileName abort!")
            replyTo ! FileTransferFailed(
              fileName,
              s"File $fileName already existed"
            )
          else
            // ddCoordinator ! DDProtocol.GetFileLocations(fileName, replyTo)
            // TODO: Implement local file actor
            Unit
          Behaviors.same

        case SendFileTo(fileName, recipientNode, recipientActor) =>
          val uploadWorker =
            context spawnAnonymous FileUploadWorker(localFiles)
          uploadWorker ! UploadFile(fileName, recipientNode, recipientActor)
          Behaviors.same

        case FileSaved(fileName, filePath) =>
          context.log.info(s"File $fileName successfully saved to $filePath")
          Behaviors.same

        case FileSaveFailed(fileName, reason) =>
          context.log.error(s"Failed to save file $fileName: $reason")
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
