SBT := project/sbt

.PHONY: build coverage license

build:
	$(SBT) clean test checkLicenseHeaders

coverage:
	$(SBT) clean coverage test coverageReport
	$(SBT) coverageAggregate

license:
	$(SBT) formatLicenseHeaders
