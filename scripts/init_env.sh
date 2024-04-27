# initialize required env variables for compiling and testing
# START this script with "source", so that the variables are set in the active shell !!!
# aws cli needs to be installed for the script to work


## odbc driver manager location for cargo to be able to compile odbc crate
#export LIBRARY_PATH=/opt/homebrew/lib:$LIBRARY_PATH

# azureite vars for object_store
export AZURE_STORAGE_USE_EMULATOR=true
export AZURE_CONTAINER_NAME=test-bucket
export AZURE_STORAGE_ACCOUNT_NAME=devstoreaccount1
export AZURE_STORAGE_ACCESS_KEY=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==

# localstack vars for object_store
#export AWS_DEFAULT_REGION=us-east-1
#export AWS_ACCESS_KEY_ID=test
#export AWS_SECRET_ACCESS_KEY=test
#export AWS_ENDPOINT=http://localhost:4566
#export AWS_ALLOW_HTTP=true
#export AWS_BUCKET_NAME=test-bucket

# check and run kafka containers
if ! docker ps --filter "name=kafka-community-small" --filter "status=running" | grep -q 'kafka-community-small'; then
  docker compose -f kafka/docker-compose-community-small.yml up -d
fi

# Check and run azure containers and create test bucket
if ! docker ps --filter "name=azurite" --filter "status=running" | grep -q 'azurite'; then
  docker run --name azurite -d -p 10000:10000 -p 10001:10001 -p 10002:10002 mcr.microsoft.com/azure-storage/azurite
fi

if ! docker ps --filter "name=azure_cli" | grep -q 'azure_cli'; then
  docker rm azure_cli
  docker run --name azure_cli --net=host mcr.microsoft.com/azure-cli az storage container create -n test-bucket --connection-string 'DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;'
fi

## Check and run aws containers and create test bucket
#if ! docker ps --filter "name=localstack" --filter "status=running" | grep -q 'localstack'; then
#  docker run --name localstack -d -p 4566:4566 localstack/localstack
#fi
#
#if ! docker ps --filter "name=aws_metadata" --filter "status=running" | grep -q 'aws_metadata'; then
#  docker run --name aws_metadata -d -p 1338:1338 amazon/amazon-ec2-metadata-mock:v1.9.2 --imdsv2
#fi
#
## Create AWS bucket only if it doesn't exist
#awslocal s3 ls s3://test-bucket || awslocal s3 mb s3://test-bucket

