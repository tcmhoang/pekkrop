package model

import model.ShareProtocol.InternalCommand_
import org.apache.pekko.actor.typed.ActorRef
import org.apache.pekko.cluster.ClusterEvent.{MemberRemoved, MemberUp}
import org.apache.pekko.cluster.ddata.{LWWMap, ORSet}
import org.apache.pekko.cluster.ddata.typed.scaladsl.Replicator.{
  GetResponse,
  SubscribeResponse,
  Update,
  UpdateResponse
}
import org.apache.pekko.util.ByteString

import java.nio.file.Path

object ShareProtocol:

  sealed trait Command

  final case class RegisterFile(file: Path) extends Command

  final case class ListAvailableFiles(replyTo: ActorRef[AvailableFiles])
      extends Command

  final case class RequestFile(
      file: String,
      replyTo: ActorRef[FileTransferStatus]
  ) extends Command

  final case class SendFileTo(
      file: String,
      recipientId: String,
      downloadTo: ActorRef[DownloadProtocol.DownloadCommand]
  ) extends Command

  final case class FileSaved(file: String, filePath: Path) extends Command

  final case class FileSaveFailed(file: String, reason: String) extends Command

  sealed trait InternalCommand_ extends Command

  final case class InternalMemUp_(upEvent: MemberUp) extends InternalCommand_

  final case class InternalMemRm_(upEvent: MemberRemoved)
      extends InternalCommand_

  sealed trait Response

  sealed trait FileTransferStatus extends Response

  final case class FileTransferInitiated(file: String)
      extends FileTransferStatus

  final case class FileTransferCompleted(file: String, localPath: Path)
      extends FileTransferStatus

  final case class FileTransferFailed(file: String, reason: String)
      extends FileTransferStatus

  final case class FileNotAvailable(file: String) extends FileTransferStatus
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
      replyTo: ActorRef[AvailableFiles]
  ) extends InternalDDCommand_

  final case class InternalFileRequest_(
      response: GetResponse[LWWMap[String, ORSet[String]]],
      file: String,
      replyTo: ActorRef[DDResponse]
  ) extends InternalDDCommand_

  sealed trait DDCommand extends InternalDDCommand_

  final case class RegisterFile(fileName: String, hostPort: String)
      extends DDCommand

  final case class GetFileListing(
      replyTo: ActorRef[AvailableFiles]
  ) extends DDCommand

  final case class GetFileLocations(
      fileName: String,
      replyTo: ActorRef[DDResponse]
  ) extends DDCommand

  final case class RemoveFile(removedHostPort: String) extends DDCommand

  sealed trait DDResponse

  final case class DDFileLocation(
      fileName: String,
      hostPorts: Set[String] // TODO: Get ref here
  ) extends DDResponse

  final case class DDNotFound(fileName: String) extends DDResponse

  final case class RemoveNodeFiles(removedHostPort: String) extends DDCommand
end DDProtocol

final case class AvailableFiles(files: Map[String, Set[String]])
    extends DDProtocol.DDResponse
    with ShareProtocol.Response

object DownloadProtocol:

  sealed trait DownloadCommand

  final case class DownloadChunk(
      file: String,
      chunk: ByteString,
      sequenceNr: Long,
      isLast: Boolean
  ) extends DownloadCommand

  final case class DownloadStart(file: String, fileSize: Long)
      extends DownloadCommand

  final case class DownloadFinished(file: String) extends DownloadCommand

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
