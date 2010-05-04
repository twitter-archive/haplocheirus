import sbt._
import com.twitter.sbt.StandardProject


class HaplocheirusProject(info: ProjectInfo) extends StandardProject(info) {
  val specs = "org.scala-tools.testing" % "specs" % "1.6.2.1"
  val vscaladoc = "org.scala-tools" % "vscaladoc" % "1.1-md-3"
  val configgy = "net.lag" % "configgy" % "1.5.2"
  val ostrich = "com.twitter" % "ostrich" % "1.1.15"
  val libthrift = "thrift" % "libthrift" % "0.2.0"
  val slf4j_jdk14 = "org.slf4j" % "slf4j-jdk14" % "1.5.2"
  val slf4j_api = "org.slf4j" % "slf4j-api" % "1.5.2"
  val jmock = "org.jmock" % "jmock" % "2.4.0" % "test"
  val hamcrest_all = "org.hamcrest" % "hamcrest-all" % "1.1" % "test"
  val cglib = "cglib" % "cglib" % "2.1_3" % "test"
  val asm = "asm" % "asm" % "1.5.3" % "test"
  val objenesis = "org.objenesis" % "objenesis" % "1.1" % "test"
  val xrayspecs = "com.twitter" % "xrayspecs" % "1.0.7"
}
