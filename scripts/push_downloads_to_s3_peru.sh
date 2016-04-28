set -e

export BUCKETNAME=s3://datlas-peru-downloads
export PROFILENAME=datlas-peru-downloads-prod
export SOURCE=downloads/

./scripts/versioned_push_to_s3.sh
