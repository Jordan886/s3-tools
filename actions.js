const { S3Client, ListBucketsCommand, GetBucketVersioningCommand, ListObjectVersionsCommand } = require('@aws-sdk/client-s3')

const S3Action = {
  client: async function (common_options, command) {
    const client = new S3Client({ 
      credentials: {
        accessKeyId: common_options.accessKey,
        secretAccessKey: common_options.secretKey,
      },
      forcePathStyle: true, // for better compatibility
      endpoint: 'https://' + common_options.url, // TODO: allow http
      region: common_options.region
    })
    const response = await client.send(command)
    return response
  },
  bucketList: async function (common_options) {
    command_result = await this.client(common_options,  new ListBucketsCommand({}))
    result = command_result.Buckets.map(function(bucket){ return bucket.Name })
    return result
  },
  bucketVersioning: async function (common_options, command_options) {
    console.log('gettings stats for bucket ', command_options.bucket)
    command_result = await this.client(common_options, new GetBucketVersioningCommand({ Bucket: command_options.bucket }))
    return { 'Versioning': command_result.Status }
  },
  deletedObjects: async function (common_options, command_options, save = false) {
    console.log('gettings objects marked for deletion on bucket', command_options.bucket)
    command_result = await this.client(common_options, new ListObjectVersionsCommand({ Bucket: command_options.bucket }))
    // for every key found increment and count objects
    const deleted_count = command_result.DeleteMarkers.reduce((val, itm) => 
      itm.Key.length > 0 ? val + 1 : val + 0, 0)
    const versioned_count = command_result.Versions.reduce((val, itm) => 
    itm.Key.length > 0 ? val + 1 : val + 0, 0)
    let DeleteMarkers
    let Versions
    if (save) {
      DeleteMarkers =  command_result.DeleteMarkers
      Versions = command_result.Versions
    }
    return { 
      'Summary': {
        'Marked Deletion': deleted_count, 
        'Verioned Objects': versioned_count 
      },
      'Details': {
        'DeleteMarkers': DeleteMarkers,
        'Versions': Versions
      }
    }
  }
}

module.exports = { S3Action }