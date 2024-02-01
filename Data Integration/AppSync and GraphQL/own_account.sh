#!/bin/bash

#title           own_account.sh
#summary         This script is running the AppSync core Workshop in your own account 
#description     This script will setup an account identically to an account that has been provisioned in an AWS event. This will run the cloud formation tempates, initialize git, and upload the template to the provisioned repo
#version         1.0
#usage           sh own_account.sh
#==============================================================================

echo 'Starting CodeCommit CloudFormation stack'
aws cloudformation create-stack \
  --stack-name CodeCommitsk \
  --template-body file://codecommit.yaml \
  --parameters \
      ParameterKey=FrontendRepoNamesk,ParameterValue=AnyCompanyReads-frontendsk \
      ParameterKey=BackendRepoName,ParameterValue=AnyCompanyReads-backendsk \
      ParameterKey=S3CodeBucket,ParameterValue='' \
      ParameterKey=S3FrontendCodeKey,ParameterValue='' \
      ParameterKey=S3BackendCodeKey,ParameterValue=''

echo 'Initializing git'
git config --global user.name "shubha262001"
git config --global user.email shubha2001shub@gmail.com
git config --global credential.helper '!aws codecommit credential-helper $@'
git config --global credential.UseHttpPath true

echo 'Waiting for CodeCommit CloudFormation stack to complete'
aws cloudformation wait stack-create-complete --stack-name CodeCommitsk
echo 'CodeCommit stack complete'

echo 'Cloning Repository'
git clone https://git-codecommit.$AWS_REGION.amazonaws.com/v1/repos/AnyCompanyReads-frontendsk

echo 'Extracting AppSync Frontend code from zip file, overwriting any existing files'
unzip -o appsync-frontend-starter.zip -d AnyCompanyReads-frontendsk

echo 'Uploading code to repository'
cd AnyCompanyReads-frontendsk
git checkout -b main
git add .
git commit -m "Initial Commit"
git push --set-upstream origin main
cd ..

echo 'Cloning Repository'
git clone https://git-codecommit.$AWS_REGION.amazonaws.com/v1/repos/AnyCompanyReads-backendsk

echo 'Extracting AppSync Backend code from zip file, overwriting any existing files'
unzip -o appsync-backend-starter.zip -d AnyCompanyReads-backendsk

echo 'Uploading code to repository'
cd AnyCompanyReads-backendsk
git checkout -b main
git add .
git commit -m "Initial Commit"
git push --set-upstream origin main
cd ..

echo 'Starting Cloud9 CloudFormation stack'
aws cloudformation create-stack \
  --stack-name Cloud9sk \
  --template-body file://cloud9.yaml \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameters \
      ParameterKey=InstanceOwner,ParameterValue='' \
      ParameterKey=InstanceName,ParameterValue=appsync-workshop-sk 

echo 'Waiting for Cloud9 stack to complete. Can take around 5 minutes, polling every 30 seconds'
aws cloudformation wait stack-create-complete --stack-name Cloud9sk
echo 'Cloud9sk stack complete'

echo -e '\n\nSetup is complete\n\n'
