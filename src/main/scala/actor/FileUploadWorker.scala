package actor

import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, Behavior, DispatcherSelector}
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{FileIO, Sink}
import org.apache.pekko.util.ByteString

import java.nio.file.{Files, Path}
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}

object FileUploadWorker:

  import model.DownloadProtocol.{DownloadChunk, DownloadStart}
  import model.UploadProtocol.*

  def apply(state: Map[String, Path]): Behavior[UploadCommand] =
    Behaviors setup:
      context =>
        given ExecutionContextExecutor = context.system.dispatchers.lookup(
          DispatcherSelector.fromConfig("pekko.actor.cpu-bound-dispatcher")
        )
        given Materializer = Materializer(context.system)
        
        Behaviors receiveMessage :
          case UploadFile(fileName, recipientNode, recipientActor) =>
            state get fileName match
              case Some(filePath) =>
                context.log info s"Initiating transfer of $fileName to $recipientNode"
                val fileSize = Files size filePath
                recipientActor ! DownloadStart(fileName, fileSize, context.self)

                val source = FileIO fromPath filePath
                val logger = context.log

                val fileSink = Sink
                  .foldAsync[Long, ByteString](0L):
                    case (seqNr, chunk) =>
                      recipientActor ! DownloadChunk(
                        fileName,
                        chunk.toVector,
                        seqNr + 1,
                        context.self,
                        isLast = false
                      )
                      Future.successful(seqNr + 1)
                  .mapMaterializedValue:
                    _.map: lastIdx =>
                      recipientActor ! DownloadChunk(
                        fileName,
                        Vector.empty[Byte],
                        lastIdx + 1,
                        context.self,
                        true
                      )

                source runWith fileSink onComplete :
                  case Success(_) =>
                    logger info s"Successfully streamed file $fileName to $recipientNode"
                  case Failure(ex) =>
                    logger error s"Error in file streaming pipeline for $fileName to $recipientNode: ${ex.getMessage}"
              case None =>
                context.log warn s"Requested file $fileName not found locally for sending."
            Behaviors.stopped
    