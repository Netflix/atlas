# Map stdin to /dev/null to avoid interactive prompts if there is some failure related to the
# build script.
SBT := cat /dev/null | project/sbt

WIKI_PRG := atlas-wiki/runMain com.netflix.atlas.wiki.Main
WIKI_DIR := target/atlas.wiki

IVY_CACHE_URL := https://www.dropbox.com/s/zx5yq86nk6q19w1/ivy2.tar.gz?dl=0
LAUNCHER_JAR_URL := https://repo1.maven.org/maven2/com/netflix/iep/iep-launcher/0.1.7/iep-launcher-0.1.7.jar

.PHONY: build clean coverage license update-wiki publish-wiki

build:
	$(SBT) clean test checkLicenseHeaders

clean:
	$(SBT) clean

coverage:
	$(SBT) clean coverage test coverageReport
	$(SBT) coverageAggregate

license:
	$(SBT) formatLicenseHeaders

$(WIKI_DIR):
	mkdir -p target
	git clone https://github.com/Netflix/atlas.wiki.git $(WIKI_DIR)

update-wiki: $(WIKI_DIR)
	cd $(WIKI_DIR) && git rm -rf *
	$(SBT) "$(WIKI_PRG) atlas-wiki/src/main/resources $(WIKI_DIR)"

publish-wiki: update-wiki
	cd $(WIKI_DIR) && git add * && git status
	cd $(WIKI_DIR) && git commit -a -m "update wiki"
	cd $(WIKI_DIR) && git push origin master

get-ivy-cache:
	curl -L $(IVY_CACHE_URL) -o $(HOME)/ivy.tar.gz
	tar -C $(HOME) -xzf $(HOME)/ivy.tar.gz

one-jar:
	mkdir -p target
	curl -L $(LAUNCHER_JAR_URL) -o target/iep-launcher.jar
	java -classpath target/iep-launcher.jar com.netflix.iep.launcher.JarBuilder \
		target/standalone.jar com.netflix.atlas.standalone.Main \
		`$(SBT) "export atlas-standalone/runtime:fullClasspath" | tail -n1 | sed 's/:/ /g'`
