package actor

import model.DownloadProtocol.{DownloadChunk, DownloadStart}
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{FileIO, Sink}
import org.apache.pekko.util.ByteString

import scala.util.{Failure, Success}
import scala.concurrent.ExecutionContextExecutor

import java.nio.file.{Files, Path}

object FileUploadWorker {

  import model.UploadProtocol.*

  def apply(state: Map[String, Path]): Behavior[UploadCommand] =
    Behaviors.setup { context =>
      given Materializer = Materializer(context.system)

      given ExecutionContextExecutor = context.system.executionContext

      Behaviors.receiveMessage {
        case UploadFile(fileName, recipientNode, recipientActor) =>
          state.get(fileName) match {
            case Some(filePath) =>
              context.log.info(
                s"Initiating transfer of $fileName to $recipientNode"
              )
              val fileSize = Files.size(filePath)
              recipientActor ! DownloadStart(fileName, fileSize)

              val source = FileIO.fromPath(filePath)
              var sequenceNr = 0L
              val logger = context.log
              source
                .runWith(
                  Sink.foreach[ByteString] { chunk =>
                    sequenceNr += 1
                    recipientActor ! DownloadChunk(
                      fileName,
                      chunk,
                      sequenceNr,
                      isLast = false
                    )
                  }
                )
                .onComplete {
                  case Success(_) =>
                    logger.info(
                      s"Successfully streamed file $fileName to $recipientNode"
                    )
                    recipientActor ! DownloadChunk(
                      fileName,
                      ByteString.empty,
                      sequenceNr + 1,
                      isLast = true
                    )
                  case Failure(ex) =>
                    logger.error(
                      s"Error in file streaming pipeline for $fileName to $recipientNode: ${ex.getMessage}"
                    )
                }
            case None =>
              context.log.warn(
                s"Requested file $fileName not found locally for sending."
              )
          }
          Behaviors.stopped

      }
    }
}
