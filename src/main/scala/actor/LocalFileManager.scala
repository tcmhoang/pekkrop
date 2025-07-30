package actor

import model.DDProtocol
import model.LocalFileProtocol.Response.{FileFound, FileNotFound}
import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors}
import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.cluster.ddata.typed.scaladsl.DistributedData

import java.nio.file.{Files, Path}
import scala.concurrent.ExecutionContext

object LocalFileManager:

  import model.LocalFileProtocol.*
  import model.UploadProtocol.UploadFile

  def apply(): Behavior[LocalFileCommand] = Behaviors.setup: context =>
    run()

  private def run(
      localFiles: Map[String, Path] = Map.empty
  ): Behavior[LocalFileCommand] =
    Behaviors receive: (context, message) =>
      given ActorContext[LocalFileCommand] = context
      given ExecutionContext = context.system.executionContext

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
          localFiles.get(fileName) match
            case Some(filePath) =>
              val uploadWorker = context.spawnAnonymous(
                FileUploadWorker(Map(fileName -> filePath))
              )
              uploadWorker ! UploadFile(fileName, recipientNode, recipientActor)
            case None =>
              context.log.warn(s"Requested to send non-local file: $fileName")
          Behaviors.same

        case SaveFile(fileName, filePath) =>
          context.log.info(s"File $fileName successfully saved to $filePath")
          run(
            localFiles + (fileName -> filePath)
          )
        case SaveFileFailed(fileName, reason) =>
          context.log.error(s"Failed to save file $fileName: $reason")
          Behaviors.same

        case CheckFileAvailability(fileName, replyTo) =>
          if localFiles.contains(fileName) then
            localFiles.get(fileName).foreach { path =>
              replyTo ! FileFound(fileName, path)
            }
          else replyTo ! FileNotFound(fileName)
          Behaviors.same
