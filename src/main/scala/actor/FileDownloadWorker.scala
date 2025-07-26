package actor

import model.ShareProtocol.{FileSaveFailed, RegisterFile}
import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{FileIO, Source}

import java.nio.file.{Files, Paths, StandardOpenOption}
import scala.concurrent.ExecutionContextExecutor
import scala.util.{Failure, Success, Try}
import model.ShareProtocol.Command

object FileDownloadWorker {

  import model.TransferProtocol.*


  def apply(replyTo: ActorRef[Command]): Behavior[FileDownloadCommand] = Behaviors.setup {

    context =>
      given ExecutionContextExecutor = context.system.executionContext

      given Materializer = Materializer(context.system)

      Behaviors.receiveMessage {
        case FileDownloadStart(fileName, fileSize)
        =>
          context.log.info(s"Starting download of file: $fileName (size: $fileSize bytes)")
          val tempFilePath = Paths.get(s"downloaded_files_${context.system.address.port.get}/$fileName.tmp")
          Files.createDirectories(tempFilePath.getParent)
          if (Files.exists(tempFilePath)) Files.delete(tempFilePath)
          Files.createFile(tempFilePath)

          val sink = FileIO.toPath(tempFilePath, Set(StandardOpenOption.APPEND))
          Behaviors.same

        case DownloadChunk(fileName, chunk, sequenceNr, isLast)
        =>
          val tempFilePath = Paths.get(s"downloaded_files_${context.system.address.port.get}/$fileName.tmp")
          if (Files.exists(tempFilePath)) {
            val futureWrite = Source.single(chunk).runWith(FileIO.toPath(tempFilePath, Set(StandardOpenOption.APPEND)))
            val logger = context.log
            val self = context.self
            val port = context.system.address.port.get
            futureWrite.onComplete {
              case Success(_) =>
                logger.debug(s"Written chunk $sequenceNr for $fileName")
                if (isLast) {
                  logger.info(s"Received last chunk for $fileName. Finalizing transfer.")
                  val finalPath = Paths.get(s"downloaded_files_$port/$fileName")
                  Try {
                    Files.move(tempFilePath, finalPath, java.nio.file.StandardCopyOption.REPLACE_EXISTING)
                    logger.info(s"File $fileName successfully downloaded and saved to $finalPath")
                    self ! FileDownloadFinished(finalPath.toString)
                  } recover {
                    case ex: Exception =>
                      logger.error(s"Failed to move temporary file for $fileName: ${ex.getMessage}")
                      self ! FileDownloadError(fileName, ex.getMessage)
                  }
                }
              case Failure(ex) =>
                logger.error(s"Failed to write chunk $sequenceNr for $fileName: ${ex.getMessage}")
                self ! FileDownloadError(fileName, ex.getMessage)
            }
          } else {
            context.log.error(s"Temporary file for $fileName not found. Cannot write chunk.")
          }
          Behaviors.same

        case FileDownloadFinished(path)
        =>
          context.log.info(s"File transfer for $path completed by sender.")
          replyTo ! RegisterFile(Paths.get(path))
          Behaviors.stopped

        case FileDownloadError(fileName, reason)
        =>
          context.log.error(s"File transfer for $fileName failed: $reason")
          val tempFilePath = Paths.get(s"downloaded_files_${context.system.address.port.get}/$fileName.tmp")
          if (Files.exists(tempFilePath)) {
            Try {
              Files.delete(tempFilePath)
              context.log.info(s"Cleaned up temporary file for $fileName.")
            } recover {
              case ex: Exception => context.log.warn(s"Failed to delete temporary file for $fileName: ${ex.getMessage}")
            }
          }
          replyTo ! FileSaveFailed(fileName, reason)
          Behaviors.stopped

      }


  }
}

