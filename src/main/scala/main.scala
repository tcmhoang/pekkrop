import actor.FileShareGuardian
import com.typesafe.config.ConfigFactory

import org.apache.pekko.actor.typed.{ActorRef, ActorSystem, Scheduler}
import org.apache.pekko.actor.typed.scaladsl.AskPattern.Askable
import org.apache.pekko.util.Timeout
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success}
import java.nio.file.{Files, Paths}
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.io.StdIn
import scala.util.CommandLineParser
import scala.util.CommandLineParser.FromString

given FromString[Array[String]] with
  def fromString(s: String): Array[String] =
    s.split(',').map(_.trim).filter(_.nonEmpty)

@main
def main(args: Array[String]): Unit =

  import model.ShareProtocol
  import model.ShareProtocol.*
  import model.ShareProtocol.Response.*

  val port = if args.nonEmpty then args(0).toInt else 0

  /*
  val loggerContext = LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext]
  val configurator = new JoranConfigurator()
  configurator.setContext(loggerContext)
  loggerContext.reset()
   */

  LoggerFactory.getLogger(this.getClass).debug("SLF4J initialized early.")

  val config = ConfigFactory
    .parseString(s"""
      pekko.remote.artery.canonical.port = $port
      pekko.remote.artery.canonical.hostname = "127.0.0.1"
    """)
    .withFallback(ConfigFactory.load())

  import scala.concurrent.duration.*
  given Timeout = Timeout(3.seconds)
  given system: ActorSystem[ShareProtocol.Command] =
    ActorSystem(FileShareGuardian(), "pekkrop", config)
  import org.apache.pekko.actor.typed.scaladsl.AskPattern.schedulerFromActorSystem

  given executor: ExecutionContextExecutor = system.executionContext

  Files.createDirectories(Paths.get(s"downloaded_files_$port"))
  system.log.info(s"Pekkrop Node started on port $port")
  var running = true
  system.whenTerminated.onComplete: _ =>
    println("System terminated")
    running = false

  sys.addShutdownHook:
    println("Shutting down system...")

  Future:
    while (running) {
      println(
        s"\nNode ${system.address}: Enter command (register <filepath>, list, request <filename>, exit):"
      )
      val input = StdIn.readLine()

      input.split(" ").toList match
        case "register" :: filePathStr :: Nil =>
          val filePath = Paths.get(filePathStr)
          system ! RegisterFile(filePath)
          println(s"Attempting to register file: $filePath")

        case "list" :: Nil =>
          (system ? ListAvailableFiles.apply).onComplete:
            case Success(AvailableFiles(files)) =>
              if files.isEmpty then
                println("No files currently available in the cluster.")
              else
                println("Available files in cluster:")
                files.foreach:
                  case (fileName, nodes) =>
                    println(
                      s"- $fileName (available on nodes: ${nodes.mkString(", ")})"
                    )
            case Failure(ex) =>
              println(s"Failed to list files: ${ex.getMessage}")

        case "request" :: fileName :: Nil =>
          println(s"Requesting file: $fileName")
          (system ? (RequestFile(fileName, _))).onComplete:
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
            "Unknown command. Please use 'register <filepath>', 'list', 'request <filename>', or 'exit'."
          )
    }
