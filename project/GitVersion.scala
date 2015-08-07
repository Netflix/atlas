import sbt._
import sbt.Keys._
import sbtrelease.Version
import com.typesafe.sbt.SbtGit._

object GitVersion {
  // 0.1.x
  private val versionBranch = """v?([0-9\.]+?)(\.x)?""".r

  // v0.1.47-31-g230560c
  // v0.1.47-20150807.161518-9
  private val snapshotVersion = """v?([0-9\.]+)-(\d+)-([0-9a-z]+)""".r

  // v0.1.47
  private val releaseVersion = """v?([0-9\.]+)""".r

  /**
   * Needs to check for "false", don't assume it will ever be set to "true".
   * http://docs.travis-ci.com/user/environment-variables/#Default-Environment-Variables
   */
  private def isPullRequest: Boolean = sys.env.getOrElse("TRAVIS_PULL_REQUEST", "false") != "false"

  /**
   * Bump the last git described version to use for the current snapshot. If it is a version branch
   * and the prefix doesn't match, then it is the first snapshot for the branch so use the branch
   * version to start with. 
   */
  private def toSnapshotVersion(branch: String, v: String): String = {
    val v2 = Version(v).map(_.bump.string).getOrElse(v)
    val suffix = "-SNAPSHOT"
    branch match {
      case versionBranch(b) if !v2.startsWith(b) =>
        s"${Version(b).map(_.string).getOrElse(v2)}$suffix"
      case _ =>
        s"$v2$suffix"
    }
  }

  lazy val settings: Seq[Def.Setting[_]] = Seq(
    version in ThisBuild := {
      val branch = GitKeys.gitReader.value.withGit(_.branches).headOption.getOrElse("unknown")
      git.gitDescribedVersion.value getOrElse "0.1-SNAPSHOT" match {
        case _ if isPullRequest       => s"0.0.0-PULLREQUEST"
        case snapshotVersion(v, _, _) => toSnapshotVersion(branch, v)
        case releaseVersion(v)        => v
        case v                        => v
      }
    }
  )
}
