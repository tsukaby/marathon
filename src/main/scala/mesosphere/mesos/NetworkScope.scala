package mesosphere.mesos

sealed trait NetworkScope {
  val name: String
  override def toString(): String = name
}

object NetworkScope {

  implicit class NetworkScopeDiscoveryInfo(scope: NetworkScope) {
    /** @return a port discovery label in the form of a string tuple */
    def discovery: Tuple2[String, String] = "network-scope" -> scope.toString
  }

  case object Container extends NetworkScope {
    val name = "container"
  }
  case object Host extends NetworkScope {
    val name = "host"
  }
}

