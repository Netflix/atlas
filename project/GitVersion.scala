import sbt.*
import sbt.Keys.*
import sbtrelease.Version
import com.github.sbt.git.SbtGit.*

object GitVersion {

  // Base version for master branch
  private val baseVersion = "v1.8.x"

  // 0.1.x
  private val versionBranch = """v?([0-9.]+)(?:\.x)?""".r

  // v0.1.47-31-g230560c
  // v0.1.47-20150807.161518-9
  private val snapshotVersion = """v?([0-9.]+)-\d+-[0-9a-z]+""".r

  // 1.5.0-rc.1-123-gcbfe51a
  private val candidateVersion = """v?([0-9.]+)(?:-rc\.\d+)?-\d+-[0-9a-z]+""".r

  // v0.1.47
  // 1.5.0-rc.1
  private val releaseVersion = """v?([0-9.]+(?:-rc\.\d+)?)""".r

  /**
    * Needs to check for "false", don't assume it will ever be set to "true".
    * http://docs.travis-ci.com/user/environment-variables/#Default-Environment-Variables
    */
  private def isPullRequest: Boolean = sys.env.getOrElse("GITHUB_EVENT_NAME", "unknown") == "pull_request"

  /**
    * Bump the last git described version to use for the current snapshot. If it is a version branch
    * and the prefix doesn't match, then it is the first snapshot for the branch so use the branch
    * version to start with.
    */
  private def toSnapshotVersion(branch: String, v: String): String = {
    val v2 = Version(v).map(_.bumpNext.unapply).getOrElse(v)
    val suffix = "-SNAPSHOT"
    branch match {
      case versionBranch(b) if !v2.startsWith(b) =>
        s"${Version(s"$b.0").map(_.unapply).getOrElse(v2)}$suffix"
      case _ =>
        s"$v2$suffix"
    }
  }

  private def extractBranchName(dflt: String): String = {
    val ref = sys.env.getOrElse("GITHUB_REF", dflt)
    // Return last part if there is a '/', e.g. refs/heads/feature-branch-1. For
    // this use-case we only care about master and version branches so it is ok
    // if something like 'feature/branch/1' exacts just the '1'.
    val parts = ref.split("/")
    parts(parts.length - 1)
  }

  lazy val settings: Seq[Def.Setting[?]] = Seq(
    ThisBuild / version := {
      val branch = extractBranchName(git.gitCurrentBranch.value)
      val branchVersion = if (branch == "main" || branch == "master") baseVersion else branch
      git.gitDescribedVersion.value.getOrElse("0.1-SNAPSHOT") match {
        case _ if isPullRequest  => s"0.0.0-PULLREQUEST"
        case snapshotVersion(v)  => toSnapshotVersion(branchVersion, v)
        case candidateVersion(v) => s"$v-SNAPSHOT"
        case releaseVersion(v)   => v
        case v                   => v
      }
    },
    ThisBuild / versionScheme := Some("semver-spec")
  )
}
