package mesosphere.marathon.core.storage.backup.impl

import akka.NotUsed
import akka.stream.scaladsl._
import java.nio.charset.StandardCharsets.UTF_8
import akka.util.ByteString
import org.apache.commons.compress.archivers.tar.{ TarArchiveEntry, TarConstants }
import scala.annotation.tailrec

object TarFlow {
  private val recordSize = 512
  private val eofBlockSize = 1024
  private val padSize = 512

  case class TarEntry(header: TarArchiveEntry, data: ByteString)
  object TarEntry {
    def apply(
      name: String,
      data: ByteString,
      mode: Int = TarArchiveEntry.DEFAULT_FILE_MODE,
      user: String = System.getProperty("user.name"),
      modTime: Long = System.currentTimeMillis / 1000
    ): TarEntry = {
      val header = new TarArchiveEntry(name)
      header.setSize(data.size.toLong)
      header.setMode(mode)
      header.setUserName(user)
      header.setModTime(modTime)
      TarEntry(header, data)
    }
  }

  /**
    * Tar reader stages which keep track of what we're currently reading in a stream, and yields items as they are
    * available
    */
  private trait Stage {
    def apply(data: ByteString): (Stage, List[TarEntry], ByteString)
  }

  /**
    * Compute the amount of padding needed to make it fit a chunkSize boundary
    */
  private def padded(size: Int, chunkSize: Int) = {
    val leftover = size % chunkSize
    if (leftover == 0)
      size
    else
      size + (chunkSize - leftover)
  }

  /**
    * In this stage, we attempt to read some amount of data, after which point we call the andThen function and
    * transition to the returned stage.
    */
  private case class ReadingData(
      size: Int, andThen: ByteString => (Stage, List[TarEntry]), readSoFar: ByteString = ByteString.empty
  ) extends Stage {
    val paddedSize = padded(size, recordSize)
    def apply(data: ByteString): (Stage, List[TarEntry], ByteString) = {
      val accum = readSoFar ++ data
      if (accum.length > paddedSize) {
        val (entryData, rest) = accum.splitAt(paddedSize)
        val (nextStage, tarEntries) = andThen(entryData.take(size))
        (nextStage, tarEntries, rest)
      } else {
        (copy(readSoFar = accum), Nil, ByteString.empty)
      }
    }
  }

  /**
    * When we hit an EOF record (all 0's), we consume and drop the rest of the stream
    */
  private case object Terminal extends Stage {
    val response = (Terminal, Nil, ByteString.empty)
    def apply(data: ByteString): (Stage, List[TarEntry], ByteString) =
      response
  }

  /**
    * Stage used to represent that we are currently trying to read a tar header.
    *
    * May enclose state from a previous header (IE
- GNU Long name entry headers).
    */
  private case class ReadingHeader(readSoFar: ByteString = ByteString.empty, longName: Option[String] = None) extends Stage {
    def resumeWithLongName(data: ByteString): (Stage, List[TarEntry]) = {
      val withoutNullTerm = if (data.last == 0)
        data.dropRight(1)
      else
        data

      (ReadingHeader(longName = Some(withoutNullTerm.utf8String)), Nil)
    }

    def yieldEntry(header: TarArchiveEntry)(data: ByteString): (Stage, List[TarEntry]) = {
      (ReadingHeader(), List(TarEntry(header, data)))
    }

    def apply(data: ByteString): (Stage, List[TarEntry], ByteString) = {
      val accum = readSoFar ++ data
      if (accum.length >= recordSize) {
        val (headerBytes, rest) = accum.splitAt(recordSize)
        lazy val header = new TarArchiveEntry(headerBytes.toArray)
        if (headerBytes.forall(_ == 0)) {
          Terminal.response
        } else if (header.isGNULongNameEntry) {
          (ReadingData(header.getSize.toInt, andThen = resumeWithLongName), Nil, rest)
        } else if (header.isPaxHeader) {
          throw new RuntimeException("Pax headers not supported")
        } else if (header.isGNUSparse) {
          throw new RuntimeException("GNU Sparse headers not supported")
        } else if (header.getSize > Int.MaxValue) {
          throw new RuntimeException(s"Entries larger than ${Int.MaxValue} not supported")
        } else {
          longName.foreach(header.setName)
          (ReadingData(header.getSize.toInt, andThen = yieldEntry(header)), Nil, rest)
        }
      } else {
        (copy(readSoFar = accum), Nil, ByteString.empty)
      }
    }
  }

  /**
    * We recursively apply the stages until all of the current available data is consumed.
    */
  @tailrec private def process(stage: Stage, data: ByteString, entries: List[TarEntry] = Nil): (Stage, List[TarEntry]) =
    if (data.isEmpty) {
      (stage, entries)
    } else {
      val (nextStage, newEntries, remaining) = stage(data)
      process(nextStage, remaining, entries ++ newEntries)
    }

  val terminalChunk = ByteString.newBuilder.putBytes(Array.ofDim[Byte](eofBlockSize)).result

  /**
    * Flow which yields complete tar entry records (with corresponding data) as they are available.
    *
    * Entries are streamed but their contents are not.
    *
    * Does not throw an error or indicate if a partial tarball is provided. Combine with gzip compression to have the
    * http://doc.akka.io/api/akka/2.4/akka/stream/scaladsl/Compression$.html stream fail on error / early termination.
    *
    * Basic GNU tar features supported only (no PAX extensions, or sparse tarballs)
    */
  val reader: Flow[ByteString, TarEntry, NotUsed] = {
    Flow[ByteString].statefulMapConcat { () =>
      var stage: Stage = ReadingHeader()

      { data: ByteString =>
        val (nextStage, entries) = process(stage, data)
        stage = nextStage
        entries
      }
    }
  }

  /**
    * Flow which converts tar entries into a series of padded ByteStrings, (one ByteString for each Tar
    * entry). ByteStrings are concatenated together to make a tarball.
    */
  val writer: Flow[TarEntry, ByteString, NotUsed] = {
    Flow[TarEntry].map {
      case TarEntry(header, data) =>
        val buffer = Array.ofDim[Byte](recordSize)
        val builder = ByteString.newBuilder
        def appendHeader(header: TarArchiveEntry): Unit = {
          header.writeEntryHeader(buffer)
          builder ++= buffer
        }

        val nameAsBytes = header.getName.getBytes(UTF_8)
        if (nameAsBytes.length > TarConstants.NAMELEN) {
          val longNameHeader = new TarArchiveEntry(TarConstants.GNU_LONGLINK, TarConstants.LF_GNUTYPE_LONGNAME)
          longNameHeader.setSize(nameAsBytes.length.toLong + 1L) // +1 for null
          appendHeader(longNameHeader)
          builder ++= nameAsBytes
          builder += 0
          while (builder.length % recordSize != 0)
            builder += 0
        }

        header.setSize(data.length.toLong) // just in case
        appendHeader(header)
        builder ++= data

        while (builder.length % recordSize != 0)
          builder += 0

        builder.result()
    }.concat(Source.single(terminalChunk))
  }
}
