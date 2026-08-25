# Map stdin to /dev/null to avoid interactive prompts if there is some failure related to the
# build script.
ifeq (${TRAVIS_SCALA_VERSION},)
	SBT := cat /dev/null | project/sbt
else
	SBT := cat /dev/null | project/sbt ++${TRAVIS_SCALA_VERSION}
endif

LAUNCHER_JAR_URL := https://repo1.maven.org/maven2/com/netflix/iep/iep-launcher/6.0.6/iep-launcher-6.0.6.jar

.PHONY: build snapshot release clean format

build:
	$(SBT) clean testFull checkLicenseHeaders scalafmtCheckAll

snapshot:
	# Travis uses a depth when fetching git data so the tags needed for versioning may not
	# be available unless we explicitly fetch them
	git fetch --unshallow --tags
	$(SBT) storeBintrayCredentials
	$(SBT) clean testFull checkLicenseHeaders publish

release:
	# Travis uses a depth when fetching git data so the tags needed for versioning may not
	# be available unless we explicitly fetch them
	git fetch --unshallow --tags

	# Storing the bintray credentials needs to be done as a separate command so they will
	# be available early enough for the publish task.
	#
	# The storeBintrayCredentials still needs to be on the subsequent command or we get:
	# [error] (iep-service/*:bintrayEnsureCredentials) java.util.NoSuchElementException: None.get
	$(SBT) storeBintrayCredentials
	$(SBT) clean testFull checkLicenseHeaders storeBintrayCredentials publish bintrayRelease

clean:
	$(SBT) clean

format:
	$(SBT) formatLicenseHeaders scalafmtAll

# Build a single runnable jar. The classpath is extracted from sbt by keeping only
# .jar entries, which relies on exportJars being set (see project/BuildSettings.scala)
# so every runtime classpath entry is a packaged jar rather than a classes directory.
one-jar:
	mkdir -p target
	curl -fL $(LAUNCHER_JAR_URL) -o target/iep-launcher.jar
	classpath=`$(SBT) --error "atlas-standalone/printRuntimeClasspath" | tr -d '\r' | grep '\.jar$$'`; \
	test -n "$$classpath" || { echo "error: no jars in classpath from sbt" >&2; exit 1; }; \
	java -classpath target/iep-launcher.jar com.netflix.iep.launcher.JarBuilder \
		target/standalone.jar com.netflix.atlas.standalone.Main $$classpath
