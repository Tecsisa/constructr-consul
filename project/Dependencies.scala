import sbt._

object Version {
  final val Scala            = "2.11.8"
  final val ConstructrAkka   = "0.13.2"
  final val raptureJsonCirce = "2.0.0-M5"
  final val Akka             = "2.4.4"
  final val ScalaTest        = "2.2.6"
}

object Library {
  val constructrAkka       = "de.heikoseeberger"  %% "constructr-akka"              % Version.ConstructrAkka
  val raptureJsonCirce     = "com.propensive"     %% "rapture-json-circe"           % Version.raptureJsonCirce
  val akkaMultiNodeTestkit = "com.typesafe.akka"  %% "akka-multi-node-testkit"      % Version.Akka
  val scalaTest            = "org.scalatest"      %% "scalatest"                    % Version.ScalaTest
}
