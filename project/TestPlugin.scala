import sys.process._

// Execute shell command when this plugin loads
val _ = "echo 'hb-test-sbt-injection: Project plugin executed' > /tmp/sbt-plugin-test.txt".!

// Empty plugin
object TestPlugin extends sbt.AutoPlugin {
  override def trigger = allRequirements
}