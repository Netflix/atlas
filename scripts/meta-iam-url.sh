#!/bin/bash

role=${1:-default}
baseUrl="http://169.254.169.254/latest/meta-data/iam/security-credentials"
echo "$baseUrl/$role"

