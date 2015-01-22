# Map stdin to /dev/null to avoid interactive prompts if there is some failure related to the
# build script.
SBT := cat /dev/null | project/sbt

WIKI_PRG := atlas-wiki/runMain com.netflix.atlas.wiki.Main

.PHONY: build coverage license update-wiki publish-wiki

build:
	$(SBT) clean test checkLicenseHeaders

coverage:
	$(SBT) clean coverage test coverageReport
	$(SBT) coverageAggregate

license:
	$(SBT) formatLicenseHeaders

update-wiki:
	mkdir -p target
	rm -rf target/atlas.wiki
	git clone https://github.com/Netflix/atlas.wiki.git target/atlas.wiki
	$(SBT) "$(WIKI_PRG) atlas-wiki/src/main/resources target/atlas.wiki"

publish-wiki: update-wiki
	cd target/atlas.wiki
	git commit -a -m "update wiki"
	git push origin master
