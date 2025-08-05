package actor

import org.apache.pekko.actor.typed.scaladsl.{Behaviors, TimerScheduler}
import org.apache.pekko.actor.typed.{ActorRef, Behavior, DispatcherSelector}
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{FileIO, Source}
import org.apache.pekko.util.ByteString

import java.nio.file.{Files, Paths, StandardCopyOption, StandardOpenOption}
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.*
import scala.util.{Failure, Success, Try}

object FileDownloadWorker:

  import model.DownloadProtocol.*
  import model.ShareProtocol.RegisterFile

  def apply(
      fileName: String,
      replyTo: ActorRef[RegisterFile],
      remoteAddress: String
  ): Behavior[DownloadCommand] =
    val downloadIdleTimerKey = s"download-$remoteAddress-$fileName"
    Behaviors withTimers: timers =>
      timers startSingleTimer (
        downloadIdleTimerKey,
        DownloadError(
          fileName,
          s"Exceeding waiting time for warming up to download $fileName, shutting down"
        ),
        10.seconds
      )
      run(timers, downloadIdleTimerKey, replyTo, remoteAddress, fileName)

  private def run(
      timers: TimerScheduler[DownloadCommand],
      timerKey: String,
      replyTo: ActorRef[RegisterFile],
      remoteAddress: String,
      originFileName: String
  ) =
    Behaviors.setup[DownloadCommand]: context =>
      given ExecutionContextExecutor = context.system.dispatchers.lookup(
        DispatcherSelector.fromConfig("pekko.actor.cpu-bound-dispatcher")
      )

      given Materializer = Materializer(context.system)

      val currentPath =
        s"downloaded_files_${context.system.address.host.get}_${context.system.address.port.get}"

      Behaviors receiveMessage:
        case DownloadStart(fileName, fileSize, where) =>
          lazy val startDownload =
            timers startSingleTimer (
              timerKey,
              DownloadError(
                fileName,
                s"Exceeding waiting time to download $fileName from ${where.path.address.toString}, shutting down"
              ),
              5.seconds
            )
            context.log.info(
              s"Starting download of file: $fileName (size: $fileSize bytes)"
            )
            val tempFilePath = Paths.get(
              s"$currentPath/$fileName.tmp"
            )
            Files.createDirectories(tempFilePath.getParent)
            if (Files.exists(tempFilePath)) Files.delete(tempFilePath)
            Files.createFile(tempFilePath)

            val sink =
              FileIO toPath (tempFilePath, Set(StandardOpenOption.APPEND))
            Behaviors.same[DownloadCommand]
          end startDownload

          validateThen(startDownload)(
            where,
            remoteAddress,
            originFileName,
            fileName
          ) getOrElse Behaviors
            .same[DownloadCommand]
        case DownloadChunk(fileName, chunk, sequenceNr, where, isLast) =>
          lazy val downloadChunk: Behavior[DownloadCommand] =
            timers startSingleTimer (
              timerKey,
              DownloadError(
                fileName,
                s"Exceeding waiting time for download chunk $sequenceNr for $fileName from ${where.path.address.toString}, shutting down"
              ),
              15.seconds
            )
            val tempFilePath = Paths.get(
              s"$currentPath/$fileName.tmp"
            )
            if !Files.exists(tempFilePath) then
              context.log.error(
                s"Temporary file for $fileName not found. Cannot write chunk."
              )
            else
              val futureWrite =
                Source single ByteString(chunk) runWith FileIO.toPath(
                  tempFilePath,
                  Set(StandardOpenOption.APPEND)
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
                      Paths.get(s"$currentPath/$fileName")
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
                  end if
                case Failure(ex) =>
                  logger error s"Failed to write chunk $sequenceNr for $fileName: ${ex.getMessage}"
                  self ! DownloadError(fileName, ex.getMessage)
            Behaviors.same[DownloadCommand]
          end downloadChunk

          validateThen(downloadChunk)(
            where,
            remoteAddress,
            originFileName,
            fileName
          ) getOrElse Behaviors.same

        case DownloadFinished(path, replyTo) =>
          context.log.info(s"File transfer for $path completed by sender.")
          replyTo ! RegisterFile(Paths.get(path))
          Behaviors.stopped

        case DownloadError(fileName, reason) =>
          context.log.error(s"File transfer for $fileName failed: $reason")
          val tempFilePath = Paths.get(
            s"$currentPath/$fileName.tmp"
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
  end run

  private def validateThen(
      fn: => Behavior[DownloadCommand]
  )(
      remoteRef: ActorRef[_],
      address: String,
      originalFileName: String,
      fileName: String
  ): Option[Behavior[DownloadCommand]] =

    if remoteRef.path.address.toString == address && fileName == originalFileName
    then Try(fn).toOption
    else None
