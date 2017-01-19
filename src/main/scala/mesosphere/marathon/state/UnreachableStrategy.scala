package mesosphere.marathon
package state

import com.wix.accord._
import com.wix.accord.dsl._

import scala.concurrent.duration._
import mesosphere.marathon.Protos

sealed trait UnreachableStrategy {
  def toProto: Protos.UnreachableStrategy
}

case object UnreachableDisabled extends UnreachableStrategy {
  val toProto: Protos.UnreachableStrategy =
    Protos.UnreachableStrategy.newBuilder.
      setMode(Protos.UnreachableStrategy.Mode.DISABLED).
      build
}

/**
  * Defines the time outs for unreachable tasks.
  */
case class UnreachableEnabled(
    inactiveAfter: FiniteDuration = UnreachableEnabled.DefaultInactiveAfter,
    expungeAfter: FiniteDuration = UnreachableEnabled.DefaultExpungeAfter) extends UnreachableStrategy {

  def toProto: Protos.UnreachableStrategy =
    Protos.UnreachableStrategy.newBuilder.
      setExpungeAfterSeconds(expungeAfter.toSeconds).
      setInactiveAfterSeconds(inactiveAfter.toSeconds).
      setMode(Protos.UnreachableStrategy.Mode.INACTIVE_AND_EXPUNGE).
      build
}
object UnreachableEnabled {
  val DefaultInactiveAfter: FiniteDuration = 5.minutes
  val DefaultExpungeAfter: FiniteDuration = 10.minutes
  val default = UnreachableEnabled()

  implicit val unreachableEnabledValidator = validator[UnreachableEnabled] { strategy =>
    strategy.inactiveAfter should be >= 1.second
    strategy.inactiveAfter should be < strategy.expungeAfter
  }
}

object UnreachableStrategy {

  def default(resident: Boolean = false): UnreachableStrategy = {
    if (resident) UnreachableDisabled else UnreachableEnabled.default
  }

  def fromProto(proto: Protos.UnreachableStrategy): UnreachableStrategy = {
    import Protos.UnreachableStrategy.Mode.{ INACTIVE_AND_EXPUNGE, DISABLED }

    proto.getMode match {
      case DISABLED =>
        UnreachableDisabled
      case INACTIVE_AND_EXPUNGE =>
        UnreachableEnabled(
          inactiveAfter = proto.getInactiveAfterSeconds.seconds,
          expungeAfter = proto.getExpungeAfterSeconds.seconds)
    }
  }

  implicit val unreachableStrategyValidator = new Validator[UnreachableStrategy] {
    def apply(strategy: UnreachableStrategy): Result = strategy match {
      case UnreachableDisabled =>
        Success
      case unreachableEnabled: UnreachableEnabled =>
        validate(unreachableEnabled)
    }
  }
}
