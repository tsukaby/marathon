package mesosphere.marathon
package core.storage.backup.impl

import java.io.File
import java.util.zip.ZipFile

import stream.Implicits._
import akka.stream.scaladsl.Source
import akka.util.ByteString
import mesosphere.marathon.core.storage.backup.BackupItem
import mesosphere.{ AkkaTest, UnitTest }
import mesosphere.marathon.stream.Sink

class ZipFileStorageTest extends UnitTest with AkkaTest {

  "ZipFileStorage" should {

    "write entries and read again from a zipfile" in {
      Given("A zipfile with 100 backup items")
      val file = File.createTempFile("marathon-zipfile", ".zip")
      file.deleteOnExit()
      val zipSink = ZipFileStorage.sink(file)
      val backupItems = 0.until(100).map(num => BackupItem("foo", s"name-$num", None, ByteString(s"data-$num")))
      val source = Source.fromIterator(() => backupItems.iterator)

      When("we use the zip file sink")
      source
        .runWith(zipSink)
        .futureValue
      file.length() should be >= 0L

      Then("The zip file has all the content")
      val zipFile = new ZipFile(file)
      zipFile.entries().toSeq should have size 100

      When("we read from the zip source")
      val sink = Sink.seq[BackupItem]
      val zipSource = ZipFileStorage.source(file)
      val result = zipSource.runWith(sink).futureValue

      Then("The zip file has all the content")
      result shouldBe backupItems
      file.delete()
    }
  }
}
