package actor

import model.TransferProtocol.{DownloadChunk, FileDownloadStart}
import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors}
import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.actor.{ActorPath, ActorSystem, RootActorPath}
import org.apache.pekko.cluster.ClusterEvent.MemberUp
import org.apache.pekko.cluster.ddata.typed.scaladsl.Replicator.*
import org.apache.pekko.cluster.ddata.typed.scaladsl.{DistributedData, Replicator}
import org.apache.pekko.cluster.ddata.{LWWMap, LWWMapKey, ORSet, SelfUniqueAddress}
import org.apache.pekko.cluster.typed.{Cluster, Subscribe}
import org.apache.pekko.stream.scaladsl.{FileIO, Sink}
import org.apache.pekko.stream.Materializer
import org.apache.pekko.util.ByteString

import java.nio.file.{Files, Path}
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}
import scala.util.{Failure, Success}

object FileShareActor {

  import model.ShareProtocol.*


  // File -> Node addr
  private val AvailableFilesKey = LWWMapKey.create[String, ORSet[String]]("available-files")

  def apply(): Behavior[Command] = Behaviors.setup { context =>
    given ActorContext[Command] = context

    given node: SelfUniqueAddress = DistributedData(context.system).selfUniqueAddress

    given ExecutionContextExecutor = context.system.executionContext

    given Materializer = Materializer(context.system)

    val replicator = DistributedData(context.system).replicator

    var localFiles: Map[String, Path] = Map.empty

    val memUpAdapter = context.messageAdapter[MemberUp](InternalMemUp_.apply)
    Cluster(context.system).subscriptions ! Subscribe(memUpAdapter, classOf[MemberUp])

    val replicatorAdapter =
      context.messageAdapter[SubscribeResponse[LWWMap[String, ORSet[String]]]](InternalSubscribeReplicator_.apply)


    replicator ! Replicator.Subscribe(AvailableFilesKey, replicatorAdapter)


    Behaviors.receiveMessage {
      case RegisterFile(filePath) =>
        val fileName = filePath.getFileName.toString
        if (Files.exists(filePath) && Files.isReadable(filePath)) {
          localFiles += (fileName -> filePath)
          context.log.info(s"Registered local file: $fileName at $filePath")

          val replicatorUpdateAdapter = context.messageAdapter[UpdateResponse[LWWMap[String, ORSet[String]]]](InternalKeyUpdate_.apply)
          replicator ! Replicator.Update(
            AvailableFilesKey,
            LWWMap.empty[String, ORSet[String]],
            WriteLocal,
            replyTo = replicatorUpdateAdapter
          )(old => old :+ (fileName -> (old.get(fileName).getOrElse(ORSet.empty[String]) :+ node.uniqueAddress.address.hostPort)))
        } else {
          context.log.warn(s"Cannot register file: $filePath. It does not exist or is not readable.")
        }
        Behaviors.same

      case ListAvailableFiles(replyTo) =>
        val listRequestAdapter = context.messageAdapter[Replicator.GetResponse[LWWMap[String, ORSet[String]]]](InternalListRequest_(_, replyTo))
        replicator ! Replicator.Get(AvailableFilesKey, ReadLocal, listRequestAdapter)
        Behaviors.same

      case RequestFile(fileName, replyTo)
      =>
        context.log.info(s"Received request to download file: $fileName")
        val fileReqAdapter = context.messageAdapter[Replicator.GetResponse[LWWMap[String, ORSet[String]]]](InternalFileRequest_(_, fileName, replyTo))
        replicator ! Get(AvailableFilesKey, ReadLocal, fileReqAdapter)
        Behaviors.same

      case SendFileTo(fileName, recipientNode, recipientActor)
      =>
        localFiles.get(fileName) match {
          case Some(filePath) =>
            context.log.info(s"Initiating transfer of $fileName to $recipientNode")
            val fileSize = Files.size(filePath)
            recipientActor ! FileDownloadStart(fileName, fileSize)

            val source = FileIO.fromPath(filePath)
            var sequenceNr = 0L
            val logger = context.log
            source.runWith(
              Sink.foreach[ByteString] { chunk =>
                sequenceNr += 1
                recipientActor ! DownloadChunk(fileName, chunk, sequenceNr, isLast = false)
              }
            ).onComplete {
              case Success(_) =>
                logger.info(s"Successfully streamed file $fileName to $recipientNode")
                recipientActor ! DownloadChunk(fileName, ByteString.empty, sequenceNr + 1, isLast = true)
              case Failure(ex) =>
                logger.error(s"Error in file streaming pipeline for $fileName to $recipientNode: ${ex.getMessage}")
            }
          case None =>
            context.log.warn(s"Requested file $fileName not found locally for sending.")
        }
        Behaviors.same

      case FileSaved(fileName, filePath)
      =>
        context.log.info(s"File $fileName successfully saved to $filePath")
        Behaviors.same

      case FileSaveFailed(fileName, reason)
      =>
        context.log.error(s"Failed to save file $fileName: $reason")
        Behaviors.same

      case cmd: InternalCommand_ => handleInternalCommand(cmd)
    }
  }


  private def handleInternalCommand(replicatorCmd: InternalCommand_)(implicit context: ActorContext[Command], node: SelfUniqueAddress, ec: ExecutionContext
  ):
  Behavior[Command] = replicatorCmd match {
    // Distributed Data Replicator Responses
    case InternalListRequest_(resp, replyTo) =>
      resp match
        case successResp@Replicator.GetSuccess(key) =>
          val data = successResp.get(key)
          val filesMap = data.entries.map { case (fileName, nodesSet) =>
            fileName -> nodesSet.elements
          }
          replyTo ! AvailableFiles(filesMap)
          Behaviors.same
        case Replicator.GetFailure(key) =>
          context.log.warn("Failed to retrieve available files from Distributed Data.")
          replyTo ! AvailableFiles(Map.empty)
          Behaviors.same
        case other =>
          context.log.warn("Got " + other)
          Behaviors.stopped


    case InternalFileRequest_(resp, fileName, replyTo) =>
      resp match
        case successResp@Replicator.GetSuccess(key) =>
          successResp.get(key).get(fileName) match
            case Some(nodesSet) if nodesSet.elements.nonEmpty =>
              val hostNodeAddress = nodesSet.elements.head // TODO: implement a more sophisticated selection
              hostNodeAddress.split(":").toList match {
                case hostAndName :: port :: Nil =>
                  context.log.info(s"File $fileName found on node: $hostNodeAddress. Initiating download.")
                  val host = hostAndName.split("@").tail.head
                  // Resolve the remote actor path and send a request to that actor
                  // TODO: Use typed
                  val sys: ActorSystem = context.system.classicSystem
                  val path = RootActorPath(context.self.path.address.copy(host = Some(host), port = port.toIntOption), context.system.name)
                  val castedPath: ActorPath = path
                  val remoteActorPath = context.self.path.elements.foldLeft[ActorPath](castedPath)(_.child(_))
                  val workerRef = context.spawnAnonymous(FileDownloadWorker(context.self))

                  context.log.info(s"Attempting to resolve remote actor: $remoteActorPath")

                  import scala.concurrent.duration.*
                  sys.actorSelection(remoteActorPath).resolveOne(3.seconds).onComplete {
                    case Success(remoteActorRef) =>
                      sys.log.info(s"Resolved remote actor: $remoteActorRef. Sending SendFileTo command.")
                      remoteActorRef ! SendFileTo(fileName, node.uniqueAddress.address.hostPort, workerRef)
                      replyTo ! FileTransferInitiated(fileName)
                    case Failure(ex) =>
                      context.log.error(s"Failed to resolve remote actor for $fileName on $hostNodeAddress: ${ex.getMessage}")
                      replyTo ! FileTransferFailed(fileName, s"Could not connect to host node: ${ex.getMessage}")
                  }
                case _ => Behaviors.stopped
              }

            case _ =>
              context.log.warn(s"File $fileName not found or no hosts available in Distributed Data.")
              replyTo ! FileNotAvailable(fileName)
          Behaviors.same


        case Replicator.GetFailure(key) =>
          context.log.warn(s"Failed to retrieve available files from Distributed Data for $fileName.")
          replyTo ! FileNotAvailable(fileName)
          Behaviors.same
        case _ => Behaviors.stopped


    case InternalKeyUpdate_(e) =>
      context.log.debug(s"Distributed Data updated for AvailableFilesKey: ${e.key}")
      Behaviors.same
    case InternalSubscribeReplicator_(e) =>
      e match
        case c@Changed(key) =>
          val data = c.get(key)
          context.log.debug(s"Distributed Data Changed for AvailableFilesKey: ${data.entries}")
          Behaviors.same
        case _ => Behaviors.same

    case InternalMemUp_(e) =>
      context.log.info(s"Node is UP: ${e.member.address.hostPort}")
      Behaviors.same

  }


}
