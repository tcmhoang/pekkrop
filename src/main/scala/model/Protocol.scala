package model

import org.apache.pekko.actor.typed.ActorRef
import org.apache.pekko.cluster.ClusterEvent.{MemberRemoved, MemberUp}
import org.apache.pekko.cluster.ddata.{LWWMap, ORSet}
import org.apache.pekko.cluster.ddata.typed.scaladsl.Replicator.{
  GetResponse,
  SubscribeResponse,
  UpdateResponse
}
import org.apache.pekko.util.ByteString

import java.nio.file.Path

object ShareProtocol:

  sealed trait InternalCommand_

  final case class InternalMemUp_(upEvent: MemberUp) extends InternalCommand_

  final case class InternalMemRm_(upEvent: MemberRemoved)
      extends InternalCommand_

  sealed trait Command extends InternalCommand_

  final case class RegisterFile(file: Path) extends Command

  final case class ListAvailableFiles(
      replyTo: ActorRef[Response.AvailableFiles]
  ) extends Command

  final case class RequestFile(
      file: String,
      replyTo: ActorRef[Response.FileTransferStatus]
  ) extends Command

  final case class SendFileTo(
      file: String,
      recipientId: String,
      downloadTo: ActorRef[DownloadProtocol.DownloadCommand]
  ) extends Command

  final case class InitiateDownload(
      fileName: String,
      hostNodes: Set[ActorRef[Command]],
      originalReplyTo: ActorRef[Response.FileTransferStatus]
  ) extends Command

  object Response:
    sealed trait Response

    sealed trait FileTransferStatus extends Response

    final case class FileTransferInitiated(file: String)
        extends FileTransferStatus

    final case class FileTransferCompleted(file: String, localPath: Path)
        extends FileTransferStatus

    final case class FileTransferFailed(file: String, reason: String)
        extends FileTransferStatus

    final case class AvailableFiles(files: Map[String, Set[String]])
        extends Response

    final case class FileNotAvailable(file: String) extends FileTransferStatus
  end Response

end ShareProtocol

object DDProtocol:
  sealed trait InternalDDCommand_

  final case class InternalDDCommandSubscribeReplicator_(
      upEvent: SubscribeResponse[LWWMap[String, ORSet[String]]]
  ) extends InternalDDCommand_

  final case class InternalDDCommandKeyUpdate_(
      upEvent: UpdateResponse[LWWMap[String, ORSet[String]]]
  ) extends InternalDDCommand_

  final case class InternalFileListing_(
      response: GetResponse[LWWMap[String, ORSet[String]]],
      replyTo: ActorRef[ShareProtocol.Response.AvailableFiles]
  ) extends InternalDDCommand_

  final case class InternalFileRequest_(
      response: GetResponse[LWWMap[String, ORSet[String]]],
      file: String,
      replyTo: ActorRef[Response.DDResponse]
  ) extends InternalDDCommand_

  sealed trait DDCommand extends InternalDDCommand_

  final case class RegisterFile(fileName: String, hostPort: String)
      extends DDCommand

  final case class GetFileListing(
      replyTo: ActorRef[ShareProtocol.Response.AvailableFiles]
  ) extends DDCommand

  final case class GetFileLocations(
      fileName: String,
      replyTo: ActorRef[Response.DDResponse]
  ) extends DDCommand

  final case class RemoveNodeFiles(removedHostPort: String) extends DDCommand

  object Response:
    sealed trait DDResponse

    final case class FileLocation(
        fileName: String,
        hostNodes: Set[String] // TODO: Get ref here
    ) extends DDResponse

    final case class NotFound(fileName: String) extends DDResponse
  end Response

end DDProtocol

object LocalFileProtocol:
  sealed trait LocalFileCommand

  final case class RegisterFile(
      filePath: Path,
      replyTo: ActorRef[DDProtocol.DDCommand]
  ) extends LocalFileCommand

  final case class SendFileTo(
      fileName: String,
      recipientNode: String,
      recipientActor: ActorRef[DownloadProtocol.DownloadCommand]
  ) extends LocalFileCommand

  final case class CheckFileAvailability(
      fileName: String,
      replyTo: ActorRef[Response.FileCheckResponse]
  ) extends LocalFileCommand

  object Response:
    sealed trait FileCheckResponse

    final case class FileFound(fileName: String, filePath: Path)
        extends FileCheckResponse

    final case class FileNotFound(fileName: String) extends FileCheckResponse

end LocalFileProtocol

object DownloadProtocol:

  sealed trait DownloadCommand

  final case class DownloadChunk(
      file: String,
      chunk: ByteString,
      sequenceNr: Long,
      where: ActorRef[UploadProtocol.UploadCommand],
      isLast: Boolean
  ) extends DownloadCommand

  final case class DownloadStart(
      file: String,
      fileSize: Long,
      where: ActorRef[UploadProtocol.UploadCommand]
  ) extends DownloadCommand

  final case class DownloadFinished(
      file: String,
      replyTo: ActorRef[ShareProtocol.RegisterFile]
  ) extends DownloadCommand

  final case class DownloadError(file: String, reason: String)
      extends DownloadCommand
end DownloadProtocol

object UploadProtocol:

  sealed trait UploadCommand

  final case class UploadFile(
      file: String,
      recipientId: String,
      downloadTo: ActorRef[DownloadProtocol.DownloadCommand]
  ) extends UploadCommand
end UploadProtocol
