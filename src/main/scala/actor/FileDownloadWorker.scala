package actor

import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{FileIO, Source}

import java.nio.file.{Files, Paths, StandardCopyOption, StandardOpenOption}
import scala.concurrent.ExecutionContextExecutor
import scala.util.{Failure, Success, Try}

object FileDownloadWorker:

  import model.DownloadProtocol.*
  import model.ShareProtocol.RegisterFile

  def apply(
      replyTo: ActorRef[RegisterFile],
      remoteAddress: String
  ): Behavior[DownloadCommand] =
    Behaviors.setup: context =>
      given ExecutionContextExecutor = context.system.executionContext

      given Materializer = Materializer(context.system)

      Behaviors receiveMessage:
        case DownloadStart(fileName, fileSize, where) =>
          lazy val startDownload =
            context.log.info(
              s"Starting download of file: $fileName (size: $fileSize bytes)"
            )
            val tempFilePath = Paths.get(
              s"downloaded_files_${context.system.address.port.get}/$fileName.tmp"
            )
            Files.createDirectories(tempFilePath.getParent)
            if (Files.exists(tempFilePath)) Files.delete(tempFilePath)
            Files.createFile(tempFilePath)

            val sink =
              FileIO toPath (tempFilePath, Set(StandardOpenOption.APPEND))
            Behaviors.same[DownloadCommand]
          end startDownload

          validateThen(where, remoteAddress)(startDownload).getOrElse(
            Behaviors.same[DownloadCommand]
          )
        case DownloadChunk(fileName, chunk, sequenceNr, where, isLast) =>
          lazy val downloadChunk: Behavior[DownloadCommand] =
            val tempFilePath = Paths.get(
              s"downloaded_files_${context.system.address.port.get}/$fileName.tmp"
            )
            if !Files.exists(tempFilePath) then
              context.log.error(
                s"Temporary file for $fileName not found. Cannot write chunk."
              )
            else
              val futureWrite = Source
                .single(chunk)
                .runWith(
                  FileIO.toPath(tempFilePath, Set(StandardOpenOption.APPEND))
                )
              val logger = context.log
              val self = context.self
              val port = context.system.address.port.get
              futureWrite.onComplete:
                case Success(_) =>
                  logger debug s"Written chunk $sequenceNr for $fileName"
                  if isLast then
                    logger info s"Received last chunk for $fileName. Finalizing transfer."
                    val finalPath =
                      Paths.get(s"downloaded_files_$port/$fileName")
                    Try:
                      Files.move(
                        tempFilePath,
                        finalPath,
                        StandardCopyOption.REPLACE_EXISTING
                      )
                      logger info s"File $fileName successfully downloaded and saved to $finalPath"
                      self ! DownloadFinished(finalPath.toString, replyTo)
                    .recover:
                      case ex: Exception =>
                        logger error s"Failed to move temporary file for $fileName: ${ex.getMessage}"
                        self ! DownloadError(fileName, ex.getMessage)
                case Failure(ex) =>
                  logger error s"Failed to write chunk $sequenceNr for $fileName: ${ex.getMessage}"
                  self ! DownloadError(fileName, ex.getMessage)
            Behaviors.same[DownloadCommand]
          end downloadChunk

          validateThen(where, remoteAddress)(downloadChunk).getOrElse(
            Behaviors.same
          )

        case DownloadFinished(path, replyTo) =>
          context.log.info(s"File transfer for $path completed by sender.")
          replyTo ! RegisterFile(Paths.get(path))
          Behaviors.stopped

        case DownloadError(fileName, reason) =>
          context.log.error(s"File transfer for $fileName failed: $reason")
          val tempFilePath = Paths.get(
            s"downloaded_files_${context.system.address.port.get}/$fileName.tmp"
          )
          if Files.exists(tempFilePath) then
            Try:
              Files.delete(tempFilePath)
              context.log.info(s"Cleaned up temporary file for $fileName.")
            .recover:
              case ex: Exception =>
                context.log.warn(
                  s"Failed to delete temporary file for $fileName: ${ex.getMessage}"
                )
          Behaviors.stopped
  end apply

  private def validateThen(remoteRef: ActorRef[_], address: String)(
      fn: => Behavior[DownloadCommand]
  ): Option[Behavior[DownloadCommand]] =
    if remoteRef.path.address.toString == address then Try(fn).toOption
    else None
