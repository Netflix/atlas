#!/bin/bash 

# https://github.com/aws/aws-sdk-java/blob/master/aws-java-sdk-core/src/main/java/com/amazonaws/auth/profile/ProfilesConfigFile.java

outputFile="${HOME}/.aws/credentials"
mkdir -p $(dirname $outputFile)

#
# Helper to create a profiles config file from the output of the instance metadata service.
# Typically used to run locally using temporary credentials from an instance with an IAM role.
# Usage:
#
# $ ssh HOST "curl -s $(meta-iam-url.sh RoleName)" | write-aws-credentials.sh
#
# Sample JSON output for role:
#
#{
#  "Code" : "Success",
#  "LastUpdated" : "2014-12-17T20:12:45Z",
#  "Type" : "AWS-HMAC",
#  "AccessKeyId" : "access-key",
#  "SecretAccessKey" : "secret-key",
#  "Token" : "token",
#  "Expiration" : "2014-12-18T02:31:31Z"
#}
#

jq -r '
  @text "[default]",
  @text "# last-updated: \(.LastUpdated)",
  @text "#      expires: \(.Expiration)",
  @text "aws_access_key_id=\(.AccessKeyId)",
  @text "aws_secret_access_key=\(.SecretAccessKey)",
  @text "aws_session_token=\(.Token)"
' > $outputFile

