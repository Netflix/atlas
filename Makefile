# Map stdin to /dev/null to avoid interactive prompts if there is some failure related to the
# build script.
SBT := cat /dev/null | project/sbt

WIKI_PRG := atlas-wiki/runMain com.netflix.atlas.wiki.Main
WIKI_DIR := target/atlas.wiki

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
