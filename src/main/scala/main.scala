import actor.FileShareGuardian
import com.typesafe.config.ConfigFactory
import org.apache.pekko.actor.typed.scaladsl.AskPattern.Askable
import org.apache.pekko.actor.typed.{ActorRef, ActorSystem}
import org.apache.pekko.util.Timeout
import org.slf4j.LoggerFactory

import java.nio.file.Paths
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.io.StdIn
import scala.util.CommandLineParser.FromString
import scala.util.{CommandLineParser, Failure, Success}

given FromString[Array[String]] with
  def fromString(s: String): Array[String] =
    s split ' ' map (_.trim) filter (_.nonEmpty)

val defaultPort = 0
val localHost = "127.0.0.1"
val authority = "pekkrop"

@main
def main(args: Array[String]): Unit =

  import model.ShareProtocol
  import model.ShareProtocol.*
  import model.ShareProtocol.Response.*

  val (maybePort, host) = args.toList match
    case port :: host :: Nil =>
      (port.toIntOption.getOrElse(defaultPort), host)
    case port :: Nil =>
      (port.toIntOption.getOrElse(defaultPort), localHost)
    case _ => (defaultPort, localHost)

  LoggerFactory.getLogger(this.getClass).debug("SLF4J initialized early.")

  val config = ConfigFactory parseString s"""
      pekko.remote.artery.canonical.port = $maybePort
      pekko.remote.artery.canonical.hostname = "$host"
    """ withFallback ConfigFactory.load()

  import scala.concurrent.duration.*
  given Timeout = Timeout(5.seconds)
  given system: ActorSystem[ShareProtocol.Command] =
    ActorSystem(FileShareGuardian(), authority, config)
  import org.apache.pekko.actor.typed.scaladsl.AskPattern.schedulerFromActorSystem

  given executor: ExecutionContextExecutor = system.executionContext

  val port =
    if maybePort == 0 then system.address.port.get else maybePort

  system.log.info(s"Pekkrop Node started on port $port")
  var running = true
  system.whenTerminated.onComplete: _ =>
    println("System terminated")
    running = false

  sys addShutdownHook:
    println("Shutting down system...")

  Future:
    while (running) {
      println(
        s"\nNode ${system.address}: Enter command (join <node1 node2 ...>,register <file paths>, list, request <file names>, exit):"
      )
      val input = StdIn.readLine()

      input.split(" ").toList match
        case "register" :: filePaths if filePaths.nonEmpty =>
          println(s"Attempting to register file: $filePaths")
          for filePathStr <- filePaths
          yield system ! RegisterFile(
            Paths.get(filePathStr)
          )

        case "list" :: Nil =>
          (system ? ListAvailableFiles.apply).onComplete:
            case Success(AvailableFiles(files)) =>
              if files.isEmpty then
                println("No files currently available in the cluster.")
              else
                println("Available files in cluster:")
                files foreach:
                  case (fileName, nodes) =>
                    println(
                      s"- $fileName (available on nodes: ${nodes.mkString(", ")})"
                    )
            case Failure(ex) =>
              println(s"Failed to list files: ${ex.getMessage}")

        case "request" :: fileNames if fileNames.nonEmpty =>
          println(s"Requesting files: $fileNames")
          for fileName <- fileNames
          yield system ! RequestFile(fileName)

        case "exit" :: Nil =>
          running = false
          system.terminate()
          println(s"Node $port shutting down.")

        case _ =>
          println(
            "Unknown command. Please use 'join <node1 node2 ...>, register <file paths>', 'list', 'request <file names>', or 'exit'."
          )
    }
