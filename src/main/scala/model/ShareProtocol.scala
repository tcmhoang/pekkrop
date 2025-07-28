package model

import org.apache.pekko.actor.typed.ActorRef
import org.apache.pekko.cluster.ClusterEvent.{MemberRemoved, MemberUp}
import org.apache.pekko.cluster.ddata.{LWWMap, ORSet}
import org.apache.pekko.cluster.ddata.Replicator.UpdateResponse
import org.apache.pekko.cluster.ddata.typed.scaladsl.Replicator.{
  GetResponse,
  SubscribeResponse
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

  final case class FileSaved(file: String, filePath: Path) extends Command

  final case class FileSaveFailed(file: String, reason: String) extends Command

  final case class SendFileTo(
      file: String,
      recipientId: String,
      downloadTo: ActorRef[DownloadProtocol.DownloadCommand]
  ) extends Command

  sealed trait InternalCommand_ extends Command

  final case class InternalMemUp_(upEvent: MemberUp) extends InternalCommand_

  final case class InternalMemRm_(upEvent: MemberRemoved)
      extends InternalCommand_

  final case class InternalSubscribeReplicator_(
      upEvent: SubscribeResponse[LWWMap[String, ORSet[String]]]
  ) extends InternalCommand_

  final case class InternalKeyUpdate_(
      upEvent: UpdateResponse[LWWMap[String, ORSet[String]]]
  ) extends InternalCommand_

  final case class InternalListRequest_(
      response: GetResponse[LWWMap[String, ORSet[String]]],
      replyTo: ActorRef[AvailableFiles]
  ) extends InternalCommand_

  final case class InternalFileRequest_(
      response: GetResponse[LWWMap[String, ORSet[String]]],
      file: String,
      replyTo: ActorRef[FileTransferStatus]
  ) extends InternalCommand_

  sealed trait Response

  final case class AvailableFiles(files: Map[String, Set[String]])
      extends Response

  sealed trait FileTransferStatus extends Response

  final case class FileTransferInitiated(file: String)
      extends FileTransferStatus

  final case class FileTransferCompleted(file: String, localPath: Path)
      extends FileTransferStatus

  final case class FileTransferFailed(file: String, reason: String)
      extends FileTransferStatus

  final case class FileNotAvailable(file: String) extends FileTransferStatus

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

object UploadProtocol:

  sealed trait UploadCommand

  final case class UploadFile(
      file: String,
      recipientId: String,
      downloadTo: ActorRef[DownloadProtocol.DownloadCommand]
  ) extends UploadCommand
