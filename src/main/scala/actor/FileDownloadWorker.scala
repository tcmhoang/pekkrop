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

  opaque type DownloadPath = String

  extension (c: DownloadError | DownloadSuccess)
    def finished: DownloadFinished = DownloadFinished(c)

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
      given (TimerScheduler[DownloadCommand], String) =
        (timers, downloadIdleTimerKey)

      given ActorRef[RegisterFile] = replyTo
      warmingUp(remoteAddress, fileName)

  private def warmingUp(
      remoteAddress: String,
      originFileName: String
  )(using
      timersWithKey: (TimerScheduler[DownloadCommand], String),
      replyTo: ActorRef[RegisterFile]
  ) =
    Behaviors.setup[DownloadCommand]: context =>
      given ExecutionContextExecutor = context.system.dispatchers.lookup(
        DispatcherSelector.fromConfig("pekko.actor.cpu-bound-dispatcher")
      )
      given Materializer = Materializer(context.system)

      val (timers, timerKey) = timersWithKey

      given currentPath: DownloadPath =
        s"downloaded_files_${context.system.address.host.get}_${context.system.address.port.get}"

      Behaviors receiveMessagePartial:
        case DownloadError(fileName, reason) =>
          context.log error s"File transfer for $fileName failed: $reason"
          Behaviors.stopped

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
            context.log info s"Starting download of file: $fileName (size: $fileSize bytes)"
            val tempFilePath = Paths.get(
              s"$currentPath/$fileName.tmp"
            )
            Files.createDirectories(tempFilePath.getParent)
            if Files.exists(tempFilePath) then Files.delete(tempFilePath)
            Files.createFile(tempFilePath)

            download(remoteAddress, originFileName)
          end startDownload

          validateThen(startDownload)(
            where,
            remoteAddress,
            originFileName,
            fileName
          ) getOrElse Behaviors
            .unhandled[DownloadCommand]
  end warmingUp

  private def download(remoteAddress: String, originFileName: String)(using
      timersWithKey: (TimerScheduler[DownloadCommand], String),
      currentPath: DownloadPath,
      mat: Materializer,
      replyTo: ActorRef[RegisterFile],
      ec: ExecutionContextExecutor
  ): Behavior[DownloadCommand] =
    val (timers, timerKey) = timersWithKey
    Behaviors.receive: (context, message) =>
      message match
        case DownloadChunk(fileName, chunk, sequenceNr, where, isLast) =>
          val self: ActorRef[DownloadFinished] = context.self.narrow
          lazy val downloadChunk: Behavior[DownloadCommand] =
            timers startSingleTimer (
              timerKey,
              DownloadError(
                fileName,
                s"Exceeding waiting time for download chunk $sequenceNr for $fileName from ${where.path.address.toString}, shutting down"
              ).finished,
              15.seconds
            )
            val tempFilePath = Paths.get(
              s"$currentPath/$fileName.tmp"
            )
            if !Files.exists(tempFilePath) then
              self ! DownloadError(
                fileName,
                s"Temporary file for $fileName not found. Cannot write chunk."
              ).finished
              Behaviors.same
            else
              val futureWrite =
                Source single chunk map ByteString.apply runWith FileIO.toPath(
                  tempFilePath,
                  Set(StandardOpenOption.APPEND)
                )

              val logger = context.log
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
                      self ! DownloadSuccess(
                        finalPath.toString,
                        replyTo
                      ).finished
                    .recover:
                      case ex: Exception =>
                        logger error s"Failed to move temporary file for $fileName: ${ex.getMessage}"
                        self ! DownloadError(fileName, ex.getMessage).finished
                  end if

                case Failure(ex) =>
                  logger error s"Failed to write chunk $sequenceNr for $fileName: ${ex.getMessage}"
                  self ! DownloadError(fileName, ex.getMessage).finished
              Behaviors.same
          end downloadChunk

          validateThen(downloadChunk)(
            where,
            remoteAddress,
            originFileName,
            fileName
          ) getOrElse Behaviors.same
        case DownloadFinished(cmd) =>
          timers cancel timerKey
          context.self ! cmd
          shutdown
        case _ => Behaviors.ignore
  end download

  private def shutdown(using
      currentPath: DownloadPath
  ): Behavior[DownloadCommand] =
    Behaviors.receive: (context, message) =>
      message match
        case DownloadSuccess(path, replyTo) =>
          context.log info s"File transfer for $path completed by sender."
          replyTo ! RegisterFile(Paths.get(path))
          Behaviors.stopped

        case DownloadError(fileName, reason) =>
          context.log error s"File transfer for $fileName failed: $reason"
          val tempFilePath = Paths.get(
            s"$currentPath/$fileName.tmp"
          )
          if Files.exists(tempFilePath) then
            Try:
              Files.delete(tempFilePath)
              context.log info s"Cleaned up temporary file for $fileName."
            .recover:
              case ex: Exception =>
                context.log warn s"Failed to delete temporary file for $fileName: ${ex.getMessage}"
          Behaviors.stopped
        case _ => Behaviors.ignore

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
