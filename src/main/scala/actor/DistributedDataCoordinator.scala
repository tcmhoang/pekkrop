package actor

import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors}
import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.cluster.ddata.typed.scaladsl.Replicator.{
  ReadLocal,
  SubscribeResponse,
  WriteLocal
}
import org.apache.pekko.cluster.ddata.typed.scaladsl.{
  DistributedData,
  Replicator
}
import org.apache.pekko.cluster.ddata.{
  LWWMap,
  LWWMapKey,
  ORSet,
  SelfUniqueAddress,
  Replicator as UntypedReplicator
}

import scala.concurrent.ExecutionContext

object DistributedDataCoordinator:

  import model.DDProtocol
  import model.DDProtocol.*
  import model.DDProtocol.Response.*
  import model.ShareProtocol.Response.AvailableFiles

  private val AvailableFilesKey =
    LWWMapKey.create[String, ORSet[String]]("available-files")

  private def handleCommand(
      command: DDCommand,
      context: ActorContext[InternalDDCommand_]
  )(using
      replicator: ActorRef[Replicator.Command],
      node: SelfUniqueAddress
  ): Behavior[InternalDDCommand_] = command match
    case RegisterFile(fileName, hostPort) =>
      context.log info s"Registering file '$fileName' from '$hostPort' in Distributed Data."
      val replicatorUpdateAdapter = context
        .messageAdapter[Replicator.UpdateResponse[
          LWWMap[String, ORSet[String]]
        ]](
          InternalKeyUpdate_.apply
        )

      replicator ! Replicator.Update(
        AvailableFilesKey,
        LWWMap.empty[String, ORSet[String]],
        WriteLocal,
        replicatorUpdateAdapter
      )(old =>
        old :+ (fileName -> ((old get fileName getOrElse ORSet
          .empty[String]) :+ hostPort))
      )
      Behaviors.same

    case GetFileListing(replyTo) =>
      val listRequestAdapter = context.messageAdapter[
        Replicator.GetResponse[LWWMap[String, ORSet[String]]]
      ](InternalFileListing_(_, replyTo))
      replicator ! Replicator.Get(
        AvailableFilesKey,
        ReadLocal,
        listRequestAdapter
      )
      Behaviors.same

    case GetFileLocations(fileName, replyTo) =>
      val fileReqAdapter = context.messageAdapter[Replicator.GetResponse[
        LWWMap[String, ORSet[String]]
      ]](
        InternalFileRequest_(_, fileName, replyTo.narrow)
      )
      replicator ! Replicator.Get(AvailableFilesKey, ReadLocal, fileReqAdapter)
      Behaviors.same

    case RemoveNodeFiles(removedHostPort) =>
      context.log info s"Removing files associated with removed node: $removedHostPort"
      val replicatorUpdateAdapter = context
        .messageAdapter[Replicator.UpdateResponse[
          LWWMap[String, ORSet[String]]
        ]](
          InternalKeyUpdate_.apply
        )
      replicator ! Replicator.Update(
        AvailableFilesKey,
        LWWMap.empty[String, ORSet[String]],
        WriteLocal,
        replicatorUpdateAdapter
      )(old =>
        old.entries
          .filter((k, s) => s.contains(removedHostPort))
          .toList
          .foldLeft(old)((m, keyWithSet) =>
            keyWithSet match
              case (key, set) =>
                val res = set.remove(removedHostPort)
                if !res.isEmpty then m.put(node, key, res)
                else m.remove(node, key)
          )
      )
      Behaviors.same

  def apply(): Behavior[DDCommand] =
    init().narrow

  private def init(): Behavior[InternalDDCommand_] = Behaviors.setup: context =>
    given replicator: ActorRef[Replicator.Command] =
      DistributedData(context.system).replicator
    given SelfUniqueAddress =
      DistributedData(context.system).selfUniqueAddress

    val replicatorSubscribeAdapter =
      context
        .messageAdapter[SubscribeResponse[LWWMap[String, ORSet[String]]]](
          InternalSubscribe_.apply
        )

    replicator ! Replicator.Subscribe(
      AvailableFilesKey,
      replicatorSubscribeAdapter
    )

    run

  private def run(using
      node: SelfUniqueAddress,
      replicator: ActorRef[Replicator.Command]
  ): Behavior[InternalDDCommand_] =
    Behaviors receive: (context, message) =>
      given ExecutionContext = context.system.executionContext

      message match
        case c @ (_: DDCommand)                  => handleCommand(c, context)
        case InternalFileListing_(resp, replyTo) =>
          resp match
            case successResp @ Replicator.GetSuccess(key) =>
              val data = successResp get key
              val filesMap = data.entries.map:
                case (fileName, nodesSet) =>
                  fileName -> nodesSet.elements
              replyTo ! AvailableFiles(filesMap)
            case _: Replicator.GetFailure[_] | _: Replicator.NotFound[_] =>
              context.log warn "Failed to retrieve available files from Distributed Data."
              replyTo ! AvailableFiles(Map.empty)
            case UntypedReplicator.GetSuccess(_, _) |
                UntypedReplicator.GetDataDeleted(_, _) =>
              context.log debug "Got untyped response from replicator"
              replyTo ! AvailableFiles(Map.empty)
          Behaviors.same

        case InternalFileRequest_(resp, fileName, replyTo) =>
          resp match
            case successResp @ Replicator.GetSuccess(key) =>
              successResp get key get fileName match
                case Some(nodesSet) if nodesSet.elements.nonEmpty =>
                  replyTo ! FileLocation(fileName, nodesSet.elements)
                case None | _: Some[_] =>
                  replyTo ! DDProtocol.Response.NotFound(fileName)
            case Replicator.GetFailure(key) =>
              context.log warn s"Failed to retrieve file locations for $fileName from Distributed Data."
              replyTo ! DDProtocol.Response.NotFound(fileName)
            case UntypedReplicator.NotFound(_, _) |
                UntypedReplicator.GetFailure(_, _) |
                UntypedReplicator.GetSuccess(_, _) |
                UntypedReplicator.GetDataDeleted(_, _) =>
              context.log debug "Got untyped response from replicator"
              replyTo ! DDProtocol.Response.NotFound(fileName)
          Behaviors.same

        case InternalKeyUpdate_(e) =>
          context.log debug s"Distributed Data updated for AvailableFilesKey: ${e.key}"
          Behaviors.same

        case InternalSubscribe_(e) =>
          e match
            case c @ Replicator.Changed(key) =>
              val data = c.get(key)
              context.log debug s"Distributed Data Changed for AvailableFilesKey: ${data.entries}"
              Behaviors.same
            case _: Replicator.Deleted[_] | _: Replicator.Changed[_] =>
              Behaviors.same
