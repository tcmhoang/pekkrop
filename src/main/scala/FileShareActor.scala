import org.apache.pekko.actor.{ActorPath, ActorSystem, RootActorPath}
import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors}
import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.cluster.ClusterEvent.MemberUp
import org.apache.pekko.cluster.ddata.typed.scaladsl.{DistributedData, Replicator}
import org.apache.pekko.cluster.ddata.typed.scaladsl.Replicator.*
import org.apache.pekko.cluster.typed.{Cluster, Subscribe}
import org.apache.pekko.cluster.ddata.{LWWMap, LWWMapKey, ORSet, SelfUniqueAddress}
import org.apache.pekko.stream.{IOResult, Materializer}
import org.apache.pekko.stream.scaladsl.{FileIO, Sink, Source}
import org.apache.pekko.util.ByteString

import java.nio.file.{Files, Path, Paths, StandardOpenOption}
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}
import scala.util.{Failure, Success}

object FileShareActor {

  import model.FileProtocol._


  // File -> Node addr
  private val AvailableFilesKey = LWWMapKey.create[String, ORSet[String]]("available-files")

  def apply(): Behavior[Command] = Behaviors.setup { context =>
    given ActorContext[Command] = context

    implicit val node: SelfUniqueAddress = DistributedData(context.system).selfUniqueAddress
    val replicator = DistributedData(context.system).replicator
    implicit val ec: ExecutionContextExecutor = context.system.executionContext
    implicit val materializer: Materializer = Materializer(context.system)


    var localFiles: Map[String, Path] = Map.empty // fileName -> localPath
    var activeDownloads: Map[String, ActorRef[FileDataMessage]] = Map.empty // fileName -> senderRef
    var activeUploads: Map[String, ActorRef[FileDataMessage]] = Map.empty // fileName -> receiverRef

    val memUpAdapter = context.messageAdapter[MemberUp](InternalMemUp_.apply)
    Cluster(context.system).subscriptions ! Subscribe(memUpAdapter, classOf[MemberUp])

    val replicatorAdapter =
      context.messageAdapter[SubscribeResponse[LWWMap[String, ORSet[String]]]]({
        case ins: UpdateResponse[LWWMap[String, ORSet[String]]] => InternalKeyUpdate_(ins)
      })

    val replicatorUpdateAdapter = context.messageAdapter[UpdateResponse[LWWMap[String, ORSet[String]]]](InternalKeyUpdate_.apply);
    replicator ! Replicator.Subscribe(AvailableFilesKey, replicatorAdapter)


    Behaviors.receiveMessage {
      case RegisterFile(filePath) =>
        val fileName = filePath.getFileName.toString
        if (Files.exists(filePath) && Files.isReadable(filePath)) {
          localFiles += (fileName -> filePath)
          context.log.info(s"Registered local file: $fileName at $filePath")

          val getResponseAdapter: ActorRef[Replicator.GetResponse[_]] =
            context.messageAdapter(InternalGetRequest_.apply)

          replicator ! Replicator.Update(
            AvailableFilesKey,
            LWWMap.empty[String, ORSet[String]],
            WriteLocal,
            replyTo = replicatorUpdateAdapter
          )(_ :+ (fileName -> (ORSet.empty[String] :+ (node.uniqueAddress.address.hostPort))))
        } else {
          context.log.warn(s"Cannot register file: $filePath. It does not exist or is not readable.")
        }
        Behaviors.same

      case ListAvailableFiles(replyTo) =>
        val listRequestAdapter = context.messageAdapter[Replicator.GetResponse[LWWMap[String, ORSet[String]]]](InternalListRequest_(_, replyTo));
        replicator ! Replicator.Get(AvailableFilesKey, ReadLocal, listRequestAdapter)
        Behaviors.same

      case RequestFile(fileName, replyTo)
      =>
        context.log.info(s"Received request to download file: $fileName")
        val fileReqAdapter = context.messageAdapter[Replicator.GetResponse[LWWMap[String, ORSet[String]]]](InternalFileRequest_(_, fileName, replyTo));
        replicator ! Get(AvailableFilesKey, ReadLocal, fileReqAdapter)
        Behaviors.same

      case SendFileTo(fileName, recipientNode, recipientActor)
      =>
        localFiles.get(fileName) match {
          case Some(filePath) =>
            context.log.info(s"Initiating transfer of $fileName to $recipientNode")
            val fileSize = Files.size(filePath)
            recipientActor ! FileTransferStart(fileName, fileSize) // Notify recipient of start

            val source = FileIO.fromPath(filePath)
            var sequenceNr = 0L
            source.runWith(
              Sink.foreach[ByteString] { chunk =>
                sequenceNr += 1
                recipientActor ! FileChunk(fileName, chunk, sequenceNr, isLast = false)
              }
            ).onComplete {
              case Success(IOResult(_, Success(_))) =>
                context.log.info(s"Successfully streamed file $fileName to $recipientNode")
                recipientActor ! FileChunk(fileName, ByteString.empty, sequenceNr + 1, isLast = true) 
                recipientActor ! FileTransferFinished(fileName)
              case Success(IOResult(_, Failure(ex))) =>
                context.log.error(s"Failed to stream file $fileName to $recipientNode: ${ex.getMessage}")
                recipientActor ! FileTransferError(fileName, ex.getMessage)
              case Failure(ex) =>
                context.log.error(s"Error in file streaming pipeline for $fileName to $recipientNode: ${ex.getMessage}")
                recipientActor ! FileTransferError(fileName, ex.getMessage)
            }
          case None =>
            context.log.warn(s"Requested file $fileName not found locally for sending.")
          // No direct way to tell the original requester, but the recipient might time out or get an error.
          // TODO: send an error back to the requester.
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
      case cmd: FileDataMessage =>
        val (nextActiveDownloads, nextBehavior) = handleFileData(cmd, activeDownloads)
        activeDownloads = nextActiveDownloads
        nextBehavior
      case MemberUp(mem)
      =>
        context.log.info(s"Node is UP: ${mem.address.hostPort}")
        Behaviors.same

      case other =>
        context.log.debug(s"Received unexpected message: $other")
        Behaviors.same
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
        case Replicator.GetDataDeleted(key) => ???
        case Replicator.NotFound(key) => ???

    case InternalFileRequest_(resp, fileName, replyTo) =>
      resp match
        case successResp@Replicator.GetSuccess(key) =>
          successResp.get(key).get(fileName) match
            case Some(nodesSet) if nodesSet.elements.nonEmpty =>
              val hostNodeAddress = nodesSet.elements.head // TODO: implement a more sophisticated selection
              hostNodeAddress.split(":").toList match {
                case host :: port :: Nil =>
                  context.log.info(s"File $fileName found on node: $hostNodeAddress. Initiating download.")

                  // Resolve the remote actor path and send a request to that actor
                  // TODO: Use typed
                  val sys: ActorSystem = context.system.asInstanceOf[ActorSystem];
                  val path = RootActorPath(context.self.path.address.copy(host = Some(host), port = port.toIntOption), context.system.name)
                  val castedPath: ActorPath = path
                  val remoteActorPath = context.self.path.elements.foldLeft[ActorPath](castedPath)(_.child(_))

                  context.log.info(s"Attempting to resolve remote actor: $remoteActorPath")

                  import scala.concurrent.duration._
                  sys.actorSelection(remoteActorPath).resolveOne(3.seconds).onComplete {
                    case Success(remoteActorRef) =>
                      context.log.info(s"Resolved remote actor: $remoteActorRef. Sending SendFileTo command.")
                      remoteActorRef ! SendFileTo(fileName, node.uniqueAddress.address.hostPort, context.self)
                      replyTo ! FileTransferInitiated(fileName)
                    case Failure(ex) =>
                      context.log.error(s"Failed to resolve remote actor for $fileName on $hostNodeAddress: ${ex.getMessage}")
                      replyTo ! FileTransferFailed(fileName, s"Could not connect to host node: ${ex.getMessage}")
                  }
              }

            case _ =>
              context.log.warn(s"File $fileName not found or no hosts available in Distributed Data.")
              replyTo ! FileNotAvailable(fileName)
          Behaviors.same


        case Replicator.GetFailure(key) =>
          context.log.warn(s"Failed to retrieve available files from Distributed Data for $fileName.")
          replyTo ! FileNotAvailable(fileName)
          Behaviors.same
        case Replicator.NotFound(key) => ???
        case Replicator.GetDataDeleted(key) => ???

    
    case InternalKeyUpdate_(e) =>
      context.log.debug(s"Distributed Data updated for AvailableFilesKey: ${e.key}")
      Behaviors.same

    case c
      @Changed(AvailableFilesKey) =>
      val data = c.get(AvailableFilesKey)
      context.log.debug(s"Distributed Data Changed for AvailableFilesKey: ${data.entries}")
      Behaviors.same
  }

  def handleFileData(cmd: FileDataMessage, state: Map[String, ActorRef[FileDataMessage]])(implicit context: ActorContext[Command], material: Materializer, ec: ExecutionContext): (Map[String, ActorRef[FileDataMessage]], Behavior[Command]) =
    var activeDownloads = state
    cmd match
      // File Data Messages (received by this actor when it's the receiver)
      case FileTransferStart(fileName, fileSize)
      =>
        context.log.info(s"Starting download of file: $fileName (size: $fileSize bytes)")
        // Create a temporary file to write to
        val tempFilePath = Paths.get(s"downloaded_files/$fileName.tmp")
        Files.createDirectories(tempFilePath.getParent) 
        if (Files.exists(tempFilePath)) Files.delete(tempFilePath) 
        Files.createFile(tempFilePath) 

        val sink = FileIO.toPath(tempFilePath, Set(StandardOpenOption.APPEND))
        activeDownloads += (fileName -> context.self) // Mark as active download
        (activeDownloads, Behaviors.same)

      case FileChunk(fileName, chunk, sequenceNr, isLast)
      =>
        val tempFilePath = Paths.get(s"downloaded_files/$fileName.tmp")
        if (Files.exists(tempFilePath)) {
          val futureWrite = Source.single(chunk).runWith(FileIO.toPath(tempFilePath, Set(StandardOpenOption.APPEND)))
          futureWrite.onComplete {
            case Success(_) =>
              context.log.debug(s"Written chunk $sequenceNr for $fileName")
              if (isLast) {
                context.log.info(s"Received last chunk for $fileName. Finalizing transfer.")
                val finalPath = Paths.get(s"downloaded_files/$fileName")
                try {
                  Files.move(tempFilePath, finalPath, java.nio.file.StandardCopyOption.REPLACE_EXISTING)
                  context.log.info(s"File $fileName successfully downloaded and saved to $finalPath")
                  activeDownloads -= fileName
                  context.self ! RegisterFile(finalPath)
                } catch {
                  case ex: Exception =>
                    context.log.error(s"Failed to move temporary file for $fileName: ${ex.getMessage}")
                    context.self ! FileSaveFailed(fileName, ex.getMessage)
                }
              }
            case Failure(ex) =>
              context.log.error(s"Failed to write chunk $sequenceNr for $fileName: ${ex.getMessage}")
              context.self ! FileSaveFailed(fileName, ex.getMessage)
          }
        } else {
          context.log.error(s"Temporary file for $fileName not found. Cannot write chunk.")
          activeDownloads -= fileName
        }
        (activeDownloads, Behaviors.same)

      case FileTransferFinished(fileName)
      =>
        context.log.info(s"File transfer for $fileName completed by sender.")
        (activeDownloads, Behaviors.same)

      case FileTransferError(fileName, reason)
      =>
        context.log.error(s"File transfer for $fileName failed: $reason")
        activeDownloads -= fileName
        val tempFilePath = Paths.get(s"downloaded_files/$fileName.tmp")
        if (Files.exists(tempFilePath)) {
          try {
            Files.delete(tempFilePath)
            context.log.info(s"Cleaned up temporary file for $fileName.")
          } catch {
            case ex: Exception => context.log.warn(s"Failed to delete temporary file for $fileName: ${ex.getMessage}")
          }
        }
        (activeDownloads, Behaviors.same)


}
