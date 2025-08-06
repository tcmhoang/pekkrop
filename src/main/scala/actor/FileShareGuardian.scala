package actor

import org.apache.pekko.actor.typed.*
import org.apache.pekko.actor.typed.receptionist.Receptionist.{Find, Register}
import org.apache.pekko.actor.typed.receptionist.{Receptionist, ServiceKey}
import org.apache.pekko.actor.typed.scaladsl.AskPattern.Askable
import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors}
import org.apache.pekko.cluster.ClusterEvent.{MemberRemoved, MemberUp}
import org.apache.pekko.cluster.typed.{Cluster, Subscribe}
import org.apache.pekko.util.Timeout

import scala.concurrent.ExecutionContextExecutor
import scala.util.Random

object FileShareGuardian:

  import model.DDProtocol.{DDCommand, Response}
  import model.LocalFileProtocol.LocalFileCommand
  import model.ShareProtocol.*
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
      Behaviors supervise DistributedDataCoordinator() onFailure SupervisorStrategy.restart,
      "distributed-data-coordinator"
    )
    context watch dd

    given lm: ActorRef[LocalFileCommand] =
      context spawn (
        Behaviors supervise LocalFileManager() onFailure SupervisorStrategy.restart,
        "local-file-manager"
      )
    context watch lm

    run

  def run(using
      dd: ActorRef[DDCommand],
      lm: ActorRef[LocalFileCommand],
      ck: ServiceKey[Command]
  ): Behavior[InternalCommand_] =
    Behaviors
      .receive[InternalCommand_]: (context, message) =>
        given ActorSystem[_] = context.system

        given ActorContext[InternalCommand_] = context

        import org.apache.pekko.actor.typed.scaladsl.AskPattern.schedulerFromActorSystem

        message match
          case InternalMemUp_(e) =>
            context.log info s"Node is UP: ${e.member.address.hostPort}"
            Behaviors.same

          case InternalMemRm_(e) =>
            context.log info s"Node is Removed: ${e.member.address.hostPort}"
            dd ! DDProtocol.RemoveNodeFiles(e.member.address.hostPort)
            Behaviors.same

          case cmd: Command => handleCommand(cmd)
      .receiveSignal:
        case (_, Terminated(_)) => Behaviors.stopped

  private def handleCommand(
      command: Command
  )(using
      dd: ActorRef[DDCommand],
      lm: ActorRef[LocalFileCommand],
      context: ActorContext[InternalCommand_],
      ck: ServiceKey[Command],
      sc: Scheduler,
      tm: Timeout = Timeout(300.milliseconds)
  ): Behavior[InternalCommand_] = command match
    case RegisterFile(filePath) =>
      lm ! LocalFileProtocol
        .RegisterFile(
          filePath,
          dd
        )
      Behaviors.same

    case ListAvailableFiles(replyTo) =>
      dd ! DDProtocol.GetFileListing(replyTo)
      Behaviors.same

    case RequestFile(fileName) =>
      context.log info s"Received request to download file: $fileName"

      given ExecutionContextExecutor = context.system.dispatchers.lookup(
        DispatcherSelector.fromConfig("pekko.actor.cpu-bound-dispatcher")
      )

      val checkLocalResult =
        lm ? (LocalFileProtocol.CheckFileAvailability(fileName, _))

      val checkDDResult = dd ? (DDProtocol.GetFileLocations(fileName, _))

      val currentInstances = context.system.receptionist ? Find(ck)

      for (
        localResult <- checkLocalResult;
        ddRes <- checkDDResult;
        ins <- currentInstances
      ) yield localResult match
        case _: LocalFileProtocol.Response.FileFound =>
          context.log warn s"File $fileName already existed in local node, abort!"
        case _: LocalFileProtocol.Response.FileNotFound =>
          ddRes match
            case _: DDProtocol.Response.NotFound =>
              context.log warn s"$fileName not exist in current cluster, please request again!"
            case DDProtocol.Response.FileLocation(_, hosts) =>
              ins match
                case ck.Listing(refs) =>
                  context.self ! InitiateDownload(
                    fileName,
                    refs.filter(ref =>
                      hosts.contains(ref.path.address.hostPort)
                    )
                  )

      Behaviors.same

    case resp: SendFileTo =>
      lm ! LocalFileProtocol.SendFileTo.apply.tupled(
        Tuple.fromProductTyped(resp)
      )
      Behaviors.same

    case InitiateDownload(fileName, hostNodes) =>
      if hostNodes.isEmpty then
        context.log warn "Could not download, there's no host to chose from"
      else
        val chosen = Random nextInt hostNodes.size
        val remoteNode = hostNodes toList chosen
        val workerRef = context spawnAnonymous FileDownloadWorker(
          fileName,
          context.self,
          remoteNode.path.address.toString
        )

        remoteNode ! SendFileTo(
          fileName,
          context.self.path.address.hostPort,
          workerRef
        )

      Behaviors.same
