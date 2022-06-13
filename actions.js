const fs  = require('fs')
const path = require('path')
const { parse } = require('json2csv');
const { S3Client, ListBucketsCommand, GetBucketVersioningCommand, ListObjectVersionsCommand, PutObjectRetentionCommand } = require('@aws-sdk/client-s3');
const { version } = require('os');

const S3Action = {
  init: async function (common_options) {
    const client = new S3Client({ 
      credentials: {
        accessKeyId: common_options.accessKey,
        secretAccessKey: common_options.secretKey,
      },
      forcePathStyle: true, // for better compatibility
      endpoint: 'https://' + common_options.url, // TODO: allow http
      region: common_options.region
    })
    return client
  },
  bucketList: async function (common_options) {
    const client = await this.init(common_options)
    command_result = await client.send(new ListBucketsCommand({}))
    result = command_result.Buckets.map(function(bucket){ return bucket.Name })
    return result
  },
  bucketVersioning: async function (common_options, command_options) {
    const client = await this.init(common_options)
    console.log('gettings stats for bucket ', command_options.bucket)
    command_result = await client.send(new GetBucketVersioningCommand({ Bucket: command_options.bucket }))
    return { 'Versioning': command_result.Status }
  },
  updateRetention: async function (common_options, command_options) {
    const client = await this.init(common_options)
    const version = await client.send(new ListObjectVersionsCommand({ Bucket: command_options.bucket, Prefix: command_options.file }))
    const date_to_add = new Date()
    date_to_add.setMinutes(date_to_add.getMinutes() + Number(command_options.minutes))
    const options = {
      Bucket: command_options.bucket,
      Key: command_options.file,
      VersionId: version.Versions[0].VersionId,
      Retention: {
        RetainUntilDate: date_to_add // new Date(command_options.date)
      }
    }
    console.log('using params:', options)
    command_result = await client.send(new PutObjectRetentionCommand( options ))
    return { command_result }
  },
  deletedObjects: async function (common_options, command_options, save = false) {
    const client = await this.init(common_options)
    console.log('gettings objects marked for deletion on bucket', command_options.bucket)
    let command_result = null
    let deleted_count = 0
    let versioned_count = 0
    let iterations = 0
    let NextVersionIdMarker
    let NextKeyMarker
    let writeStreamDeleted
    let writeStreamVerisoned
    // open write stream if save option is enabled
    if (save)
    {
      const save_path_deleted = save ? path.join(command_options.saveDetails, 'bucket_deleted.csv') : null
      const save_path_versioned = save ? path.join(command_options.saveDetails, 'bucket_versioned.csv') :null
      writeStreamDeleted = fs.createWriteStream(save_path_deleted)
      writeStreamVerisoned = fs.createWriteStream(save_path_versioned)
    }
    do {
      iterations ++
      let options = { Bucket: command_options.bucket,
                      VersionIdMarker: NextVersionIdMarker,
                      KeyMarker: NextKeyMarker
                    }
      command_result = await client.send(new ListObjectVersionsCommand(options))
      // for every key found increment and count objects
      if (command_result.DeleteMarkers) {
        deleted_count += command_result.DeleteMarkers.reduce((val, itm) => 
        itm.Key.length > 0 ? val + 1 : val + 0, 0)
        if (save) { 
          const csv_deleted = parse(command_result.DeleteMarkers)
          writeStreamDeleted.write(csv_deleted) 
        }
      }
      if (command_result.Versions) {
        versioned_count += command_result.Versions.reduce((val, itm) => 
        itm.Key.length > 0 ? val + 1 : val + 0, 0)
        if (save) { 
          const csv_versioned = parse(command_result.Versions)
          writeStreamVerisoned.write(csv_versioned) 
        }
      }
      NextKeyMarker = command_result.NextKeyMarker
      NextVersionIdMarker = command_result.NextVersionIdMarker
      // print progress every 100K files
      if ((iterations / 10) % 1 === 0 ) {
        console.log(`Delete_C=${deleted_count} Version_C=${versioned_count}, VersionIdMarker=${NextVersionIdMarker}`)
      }
    } while (command_result.IsTruncated)

    if (save) {
      writeStreamDeleted.end()
      writeStreamVerisoned.end()
    }
    return { 
      'Summary': {
        'Marked Deletion': deleted_count, 
        'Verioned Objects': versioned_count 
      }
    }
  }
}

module.exports = { S3Action }