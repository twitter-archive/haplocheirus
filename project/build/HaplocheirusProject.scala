import sbt._
import com.twitter.sbt._

class HaplocheirusProject(info: ProjectInfo) extends StandardProject(info) with GithubPublisher {
  val gizzard = "com.twitter" % "gizzard" % "1.5.2-a"
  val jredis = "jredis" % "jredis" % "1.0-tw1"
  val codec = "commons-codec" % "commons-codec" % "1.4"
  val xrayspecs = "com.twitter" % "xrayspecs" % "1.0.7"  //--auto--

  val ostrich = "com.twitter" % "ostrich" % "1.2.15"

  val specs = "org.scala-tools.testing" % "specs" % "1.6.2.1" % "test"
  val jmock = "org.jmock" % "jmock" % "2.4.0" % "test"  //--auto--
  val hamcrest_all = "org.hamcrest" % "hamcrest-all" % "1.1" % "test"  //--auto--
  val cglib = "cglib" % "cglib" % "2.1_3" % "test"  //--auto--
  val asm = "asm" % "asm" % "1.5.3" % "test"  //--auto--
  val objenesis = "org.objenesis" % "objenesis" % "1.1" % "test"  //--auto--
}
