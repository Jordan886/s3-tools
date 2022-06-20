const {
  S3Client,
  ListBucketsCommand,
  GetBucketVersioningCommand,
} = require('@aws-sdk/client-s3')

const S3Action = {
  init: async function init(common_options) {
    const client = new S3Client({ 
      credentials: {
        accessKeyId: common_options.accessKey,
        secretAccessKey: common_options.secretKey,
      },
      forcePathStyle: true, // for better compatibility
      endpoint: `https://${common_options.url}`, // TODO: allow http
      region: common_options.region
    })
    return client
  },
  bucketList: async function bucketList(common_options) {
    const client = await this.init(common_options)
    const command_result = await client.send(new ListBucketsCommand({}))
    const result = command_result.Buckets.map((bucket) => bucket.Name)
    return result
  },
  bucketVersioning: async function bucketVersioning(common_options, command_options) {
    const client = await this.init(common_options)
    console.log('getting info for bucket ', command_options.bucket)
    const command_result = await client.send(
      new GetBucketVersioningCommand({ Bucket: command_options.bucket })
    )
    return { Versioning: command_result.Status }
  },
}

module.exports = { S3Action }
