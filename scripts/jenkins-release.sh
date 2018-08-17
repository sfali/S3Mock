#!/bin/bash -l
set -e

#You need to setup LIGHTBEND Credentials and Post-Build Commit Status Hook and AWS Credentials for Deployment to DEV with ecs-service

export AWS_DEFAULT_REGION=us-east-1

# Use the cross account IAM role to access ECR
eval $(assume-role --role-arn="$ECR_ROLE")

# Login to private container repository
eval $(aws ecr get-login --no-include-email)

git branch --set-upstream-to=origin/master master

# Build and push image to ECR
sbt "release with-defaults"
