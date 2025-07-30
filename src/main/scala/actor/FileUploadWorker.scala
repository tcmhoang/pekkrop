package actor

import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{FileIO, Sink}
import org.apache.pekko.util.ByteString

import scala.util.{Failure, Success}
import scala.concurrent.{ExecutionContextExecutor, Future}
import java.nio.file.{Files, Path}

object FileUploadWorker:

  import model.UploadProtocol.*
  import model.DownloadProtocol.{DownloadChunk, DownloadStart}

  def apply(state: Map[String, Path]): Behavior[UploadCommand] =
    Behaviors setup:
      context =>
        given Materializer = Materializer(context.system)

        given ExecutionContextExecutor = context.system.executionContext

        Behaviors receiveMessage :
          case UploadFile(fileName, recipientNode, recipientActor) =>
            state get fileName match
              case Some(filePath) =>
                context.log.info(
                  s"Initiating transfer of $fileName to $recipientNode"
                )
                val fileSize = Files size filePath
                recipientActor ! DownloadStart(fileName, fileSize, context.self)

                val source = FileIO fromPath filePath
                val logger = context.log

                val fileSink = Sink
                  .foldAsync[Long, ByteString](0L):
                    case (seqNr, chunk) =>
                      recipientActor ! DownloadChunk(
                        fileName,
                        chunk,
                        seqNr + 1,
                        context.self,
                        isLast = false
                      )
                      Future.successful(seqNr + 1)
                  .mapMaterializedValue:
                    _.map: lastIdx =>
                      recipientActor ! DownloadChunk(
                        fileName,
                        ByteString.empty,
                        lastIdx + 1,
                        context.self,
                        true
                      )

                source runWith fileSink onComplete :
                  case Success(_) =>
                    logger.info(
                      s"Successfully streamed file $fileName to $recipientNode"
                    )
                  case Failure(ex) =>
                    logger error s"Error in file streaming pipeline for $fileName to $recipientNode: ${ex.getMessage}"
              case None =>
                context.log.warn(
                  s"Requested file $fileName not found locally for sending."
                )
            Behaviors.stopped
    