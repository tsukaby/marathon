package mesosphere.marathon
package core.storage.backup.impl

import java.io.File

import akka.Done
import akka.stream.Materializer
import com.typesafe.scalalogging.StrictLogging
import mesosphere.marathon.core.storage.backup.PersistentStoreBackup
import mesosphere.marathon.core.storage.store.PersistenceStore

import scala.concurrent.Future

class PersistentStoreBackupImpl(store: PersistenceStore[_, _, _], location: String)(implicit materializer: Materializer)
    extends PersistentStoreBackup with StrictLogging {

  override def backup(): Future[Done] = {
    logger.info(s"Create backup at $location")
    val sink = ZipFileStorage.sink(new File(location))
    store.backup().runWith(sink)
  }

  override def restore(): Future[Done] = {
    logger.info(s"Restore backup from $location")
    val source = ZipFileStorage.source(new File(location))
    store.restore(source)
  }
}
