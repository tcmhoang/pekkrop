package actor

import model.LocalFileProtocol.Response.{FileFound, FileNotFound}
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, Behavior, Terminated}
import org.apache.pekko.cluster.ddata.typed.scaladsl.DistributedData

import java.nio.file.{Files, Path}

object LocalFileManager:

  import model.DDProtocol
  import model.LocalFileProtocol.*
  import model.UploadProtocol.UploadFile

  def apply(
      localFiles: Map[String, Path] = Map.empty
  ): Behavior[LocalFileCommand] =
    run(localFiles)

  private def run(
      localFiles: Map[String, Path]
  ): Behavior[LocalFileCommand] =
    Behaviors
      .receive[LocalFileCommand]: (context, message) =>
        val node = DistributedData(
          context.system
        ).selfUniqueAddress.uniqueAddress.address.hostPort

        message match
          case RegisterFile(filePath, replyTo) =>
            val fileName = filePath.getFileName.toString
            if Files.exists(filePath) && Files.isReadable(filePath) then
              replyTo ! DDProtocol.RegisterFile(fileName, node)
              context.log.info(s"Registered local file: $fileName at $filePath")
              run(localFiles + (fileName -> filePath))
            else
              context.log.warn(
                s"Cannot register file: $filePath. It does not exist or is not readable."
              )
              Behaviors.same

          case SendFileTo(fileName, recipientNode, recipientActor) =>
            localFiles get fileName match
              case Some(filePath) =>
                val uploadWorker = context spawnAnonymous FileUploadWorker(
                  Map(fileName -> filePath)
                )
                context watch uploadWorker
                uploadWorker ! UploadFile(
                  fileName,
                  recipientNode,
                  recipientActor
                )
              case None =>
                context.log.warn(s"Requested to send non-local file: $fileName")
            Behaviors.same

          case CheckFileAvailability(fileName, replyTo) =>
            localFiles get fileName match
              case Some(value) => replyTo ! FileFound(fileName, value)
              case None        => replyTo ! FileNotFound(fileName)
            Behaviors.same
      .receiveSignal:
        case (context, Terminated(_)) =>
          context.log.info("Upload worker's done w/ update, killed!")
          Behaviors.same
