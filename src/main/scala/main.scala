import actor.FileShareGuardian
import com.typesafe.config.ConfigFactory
import org.apache.pekko.actor.typed.scaladsl.AskPattern.Askable
import org.apache.pekko.actor.typed.{ActorRef, ActorSystem}
import org.apache.pekko.util.Timeout
import org.slf4j.LoggerFactory

import java.nio.file.{Files, Paths}
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.io.StdIn
import scala.util.CommandLineParser.FromString
import scala.util.{CommandLineParser, Failure, Success}

given FromString[Array[String]] with
  def fromString(s: String): Array[String] =
    s split ',' map (_.trim) filter (_.nonEmpty)

@main
def main(args: Array[String]): Unit =

  import model.ShareProtocol
  import model.ShareProtocol.*
  import model.ShareProtocol.Response.*

  val maybePort = if args.nonEmpty then args(0).toInt else 0

  LoggerFactory.getLogger(this.getClass).debug("SLF4J initialized early.")

  val config = ConfigFactory parseString s"""
      pekko.remote.artery.canonical.port = $maybePort
      pekko.remote.artery.canonical.hostname = "127.0.0.1"
    """ withFallback ConfigFactory.load()

  import scala.concurrent.duration.*
  given Timeout = Timeout(5.seconds)
  given system: ActorSystem[ShareProtocol.Command] =
    ActorSystem(FileShareGuardian(), "pekkrop", config)
  import org.apache.pekko.actor.typed.scaladsl.AskPattern.schedulerFromActorSystem

  given executor: ExecutionContextExecutor = system.executionContext

  val port = if maybePort == 0 then system.address.port.get else maybePort
  Files.createDirectories(Paths.get(s"downloaded_files_$port"))
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
        case "join" :: nodes if nodes.nonEmpty =>
          system ! Join(nodes)
          println(s"Attempting to join nodes: $nodes")
          
        case "register" :: filePaths if filePaths.nonEmpty =>
          println(s"Attempting to register file: $filePaths")
          for filePathStr <- filePaths
          yield
            system ! RegisterFile(
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
          yield (system ? (RequestFile(fileName, _))).onComplete:
            case Success(status) =>
              status match
                case FileTransferInitiated(name) =>
                  println(
                    s"File transfer initiated for $name. Check 'downloaded_files_$port' directory."
                  )
                case FileTransferCompleted(name, path) =>
                  println(s"File $name successfully downloaded to $path.")
                case FileTransferFailed(name, reason) =>
                  println(s"File transfer failed for $name: $reason")
                case FileNotAvailable(name) =>
                  println(s"File $name is not available in the cluster.")
            case Failure(ex) =>
              println(s"Error requesting file $fileName: ${ex.getMessage}")

        case "exit" :: Nil =>
          running = false
          system.terminate()
          println(s"Node $port shutting down.")

        case _ =>
          println(
            "Unknown command. Please use 'join <node1 node2 ...>, register <file paths>', 'list', 'request <file names>', or 'exit'."
          )
    }
