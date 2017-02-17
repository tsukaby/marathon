package mesosphere.marathon
package core.storage.backup.impl

import java.io.{ Closeable, File, FileOutputStream }
import java.time.OffsetDateTime
import java.util.zip.{ ZipEntry, ZipFile, ZipOutputStream }

import akka.stream._
import akka.stream.scaladsl.{ Sink, Source }
import akka.stream.stage.{ GraphStage, GraphStageLogic, InHandler, OutHandler }
import akka.util.ByteString
import akka.{ Done, NotUsed }
import com.google.common.io.ByteStreams
import com.typesafe.scalalogging.StrictLogging
import mesosphere.marathon.core.storage.backup.BackupItem

import scala.concurrent.Future

object ZipFileStorage extends StrictLogging {

  class CloseStage[T](closeable: Closeable) extends GraphStage[FlowShape[T, T]] {
    val out: Outlet[T] = Outlet("close-out")
    val in: Inlet[T] = Inlet("close-in")
    override def shape = FlowShape(in, out)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
      setHandler(in, new InHandler { override def onPush() = push(out, grab(in)) })
      setHandler(out, new OutHandler { override def onPull() = pull(in) })

      override def postStop(): Unit = {
        super.postStop()
        closeable.close()
      }
    }
  }

  class ZipFileReader(file: File) extends Iterator[(String, ByteString)] with Closeable {
    require(file.exists(), s"zip file does not exist: ${file.getAbsolutePath}")
    private[this] val zipFile = new ZipFile(file)
    private[this] val entries = zipFile.entries()

    def close(): Unit = zipFile.close()

    override def hasNext: Boolean = {
      val result = entries.hasMoreElements
      if (!result) zipFile.close()
      result
    }

    override def next(): (String, ByteString) = {
      val entry = entries.nextElement()
      val data = ByteString(ByteStreams.toByteArray(zipFile.getInputStream(entry)))
      entry.getName -> data
    }
  }

  class ZipFileWriter(file: File) extends Closeable {
    private[this] val zipOutputStream = new ZipOutputStream(new FileOutputStream(file))

    def writeNext(key: String, data: ByteString): Unit = {
      zipOutputStream.putNextEntry(new ZipEntry(key))
      zipOutputStream.write(data.toArray)
    }

    def close(): Unit = zipOutputStream.close()
  }

  val CategoryItemRegexp = "^([^/]+)/items/(.+)$".r
  val CategoryItemWithVersionRegexp = "^([^/]+)/versions/([^/]+)/(.+)$".r
  def source(file: File): Source[BackupItem, NotUsed] = {
    lazy val reader = new ZipFileReader(file)
    Source
      .fromIterator(() => reader).named("ZipFileSource")
      .via(new CloseStage[(String, ByteString)](reader))
      .map {
        case (CategoryItemWithVersionRegexp(category, key, version), value) => BackupItem(category, key, Some(OffsetDateTime.parse(version)), value)
        case (CategoryItemRegexp(category, key), value) => BackupItem(category, key, None, value)
        case (key, _) => throw new IllegalArgumentException(s"Can not read: $key")
      }
  }

  def sink(file: File): Sink[BackupItem, Future[Done]] = {
    import scala.concurrent.ExecutionContext.Implicits.global
    lazy val writer = new ZipFileWriter(file)
    def path(item: BackupItem): String = item.version match {
      case Some(dt) => s"${item.category}/versions/${item.key}/$dt"
      case None => s"${item.category}/items/${item.key}"

    }
    Sink
      .foreach[BackupItem](item => writer.writeNext(path(item), item.data))
      .mapMaterializedValue[Future[Done]] { result =>
        result.map { value =>
          writer.close()
          value
        }
      }
  }
}