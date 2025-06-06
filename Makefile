# Map stdin to /dev/null to avoid interactive prompts if there is some failure related to the
# build script.
ifeq (${TRAVIS_SCALA_VERSION},)
	SBT := cat /dev/null | project/sbt
else
	SBT := cat /dev/null | project/sbt ++${TRAVIS_SCALA_VERSION}
endif

WIKI_PRG        := atlas-wiki/runMain com.netflix.atlas.wiki.Main
WIKI_INPUT_DIR  := $(shell pwd)/atlas-wiki/src/main/resources
WIKI_OUTPUT_DIR := $(shell pwd)/target/atlas.wiki

LAUNCHER_JAR_URL := https://repo1.maven.org/maven2/com/netflix/iep/iep-launcher/5.1.1/iep-launcher-5.1.1.jar

.PHONY: build snapshot release clean format update-wiki publish-wiki

build:
	$(SBT) clean test checkLicenseHeaders scalafmtCheckAll

snapshot:
	# Travis uses a depth when fetching git data so the tags needed for versioning may not
	# be available unless we explicitly fetch them
	git fetch --unshallow --tags
	$(SBT) storeBintrayCredentials
	$(SBT) clean test checkLicenseHeaders publish

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
	$(SBT) clean test checkLicenseHeaders storeBintrayCredentials publish bintrayRelease

clean:
	$(SBT) clean

format:
	$(SBT) formatLicenseHeaders scalafmtAll

$(WIKI_OUTPUT_DIR):
	mkdir -p target
	git clone git@github.com:Netflix/atlas.wiki.git $(WIKI_OUTPUT_DIR)

update-wiki: $(WIKI_OUTPUT_DIR)
	cd $(WIKI_OUTPUT_DIR) && git rm -rf *
	$(SBT) "$(WIKI_PRG) $(WIKI_INPUT_DIR) $(WIKI_OUTPUT_DIR)"

publish-wiki: update-wiki
	cd $(WIKI_OUTPUT_DIR) && git add * && git status
	cd $(WIKI_OUTPUT_DIR) && git commit -a -m "update wiki"
	cd $(WIKI_OUTPUT_DIR) && git push origin master

one-jar:
	mkdir -p target
	curl -L $(LAUNCHER_JAR_URL) -o target/iep-launcher.jar
	java -classpath target/iep-launcher.jar com.netflix.iep.launcher.JarBuilder \
		target/standalone.jar com.netflix.atlas.standalone.Main \
		`$(SBT) "export atlas-standalone/runtime:fullClasspath" | tail -n1 | sed 's/:/ /g'`
