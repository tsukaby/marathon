package mesosphere.marathon
package core.storage.backup

import akka.Done
import akka.stream.Materializer
import com.typesafe.scalalogging.StrictLogging
import mesosphere.marathon.core.storage.backup.impl.PersistentStoreBackupImpl
import mesosphere.marathon.core.storage.store.PersistenceStore

import scala.concurrent.Future

/**
  * Backup & Restore functionality for configured persistent store and backup location.
  */
trait PersistentStoreBackup {

  /**
    * Backup the state of the configured persistent store.
    * @return A future which succeeds, if the backup is written completely.
    */
  def backup(): Future[Done]

  /**
    * Restore the state from a given backup.
    * @return a future which succeeds, if the backup is restored completely.
    */
  def restore(): Future[Done]

}

object PersistentStoreBackup extends StrictLogging {

  def apply(location: Option[String], store: PersistenceStore[_, _, _])(implicit materializer: Materializer): PersistentStoreBackup = {
    location.fold(NoBackup) { loc =>
      new PersistentStoreBackupImpl(store, loc)
    }
  }

  lazy val NoBackup: PersistentStoreBackup = new PersistentStoreBackup {
    override def backup() = {
      logger.warn("No backup location defined. Please use --backup_location.")
      Future.successful(Done)
    }
    override def restore() = {
      logger.warn("No backup location defined. Please use --backup_location.")
      Future.successful(Done)
    }
  }
}
