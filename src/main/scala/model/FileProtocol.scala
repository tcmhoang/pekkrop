package model

import org.apache.pekko.actor.typed.ActorRef
import org.apache.pekko.cluster.ClusterEvent.MemberUp
import org.apache.pekko.cluster.ddata.{LWWMap, ORSet}
import org.apache.pekko.cluster.ddata.Replicator.UpdateResponse
import org.apache.pekko.cluster.ddata.typed.scaladsl.Replicator.{GetResponse, SubscribeResponse}
import org.apache.pekko.util.ByteString

import java.nio.file.Path

object FileProtocol {

  sealed trait Command

  final case class RegisterFile(filePath: Path) extends Command

  final case class ListAvailableFiles(replyTo: ActorRef[AvailableFiles]) extends Command

  final case class RequestFile(fileName: String, replyTo: ActorRef[FileTransferStatus]) extends Command

  final case class SendFileTo(fileName: String, recipientNode: String, recipientActor: ActorRef[Command]) extends Command

  final case class FileSaved(fileName: String, filePath: Path) extends Command

  final case class FileSaveFailed(fileName: String, reason: String) extends Command

  sealed trait InternalCommand_ extends Command

  final case class InternalMemUp_(upEvent: MemberUp) extends InternalCommand_

  final case class InternalSubscribeReplicator_(upEvent: SubscribeResponse[LWWMap[String, ORSet[String]]]) extends InternalCommand_

  final case class InternalKeyUpdate_(upEvent: UpdateResponse[LWWMap[String, ORSet[String]]]) extends InternalCommand_

  final case class InternalListRequest_(response: GetResponse[LWWMap[String, ORSet[String]]],
                                        replyTo: ActorRef[AvailableFiles]) extends InternalCommand_

  final case class InternalFileRequest_(response: GetResponse[LWWMap[String, ORSet[String]]],
                                        fileName: String,
                                        replyTo: ActorRef[FileTransferStatus]) extends InternalCommand_


  sealed trait Response

  final case class AvailableFiles(files: Map[String, Set[String]]) extends Response

  sealed trait FileTransferStatus extends Response

  final case class FileTransferInitiated(fileName: String) extends FileTransferStatus

  final case class FileTransferCompleted(fileName: String, localPath: Path) extends FileTransferStatus

  final case class FileTransferFailed(fileName: String, reason: String) extends FileTransferStatus

  final case class FileNotAvailable(fileName: String) extends FileTransferStatus


  sealed trait FileDataMessage extends Command

  final case class FileChunk(fileName: String, chunk: ByteString, sequenceNr: Long, isLast: Boolean) extends FileDataMessage

  final case class FileTransferStart(fileName: String, fileSize: Long) extends FileDataMessage

  final case class FileTransferFinished(fileName: String) extends FileDataMessage

  final case class FileTransferError(fileName: String, reason: String) extends FileDataMessage

}
