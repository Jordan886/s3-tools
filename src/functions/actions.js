const fs = require('fs')
const path = require('path')
const { parse } = require('json2csv');
const {
  S3Client,
  ListBucketsCommand,
  GetBucketVersioningCommand,
  ListObjectVersionsCommand,
  ListObjectsV2Command
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
  deletedObjects: async function deletedObjects(common_options, command_options, save = false) {
    const client = await this.init(common_options)
    console.log('getting objects marked for deletion on bucket', command_options.bucket)
    let command_result = null
    let deleted_count = 0
    let versioned_count = 0
    let iterations = 0
    let NextVersionIdMarker
    let NextKeyMarker
    let writeStreamDeleted
    let writeStreamVerisoned
    // open write stream if save option is enabled
    if (save) {
      const save_path_deleted = save ? path.join(command_options.saveDetails, 'bucket_deleted.csv') : null
      const save_path_versioned = save ? path.join(command_options.saveDetails, 'bucket_versioned.csv') :null
      writeStreamDeleted = fs.createWriteStream(save_path_deleted)
      writeStreamVerisoned = fs.createWriteStream(save_path_versioned)
    }
    do {
      iterations += 1
      const options = {
        Bucket: command_options.bucket,
        VersionIdMarker: NextVersionIdMarker,
        KeyMarker: NextKeyMarker
      }
      command_result = await client.send(new ListObjectVersionsCommand(options))
      // for every key found increment and count objects
      if (command_result.DeleteMarkers) {
        deleted_count += command_result.DeleteMarkers.reduce((val, itm) => (
          itm.Key.length > 0 ? val + 1 : val + 0), 0)
        if (save) {
          const csv_deleted = parse(command_result.DeleteMarkers)
          writeStreamDeleted.write(csv_deleted) 
        }
      }
      if (command_result.Versions) {
        versioned_count += command_result.Versions.reduce((val, itm) => (
          itm.Key.length > 0 ? val + 1 : val + 0), 0)
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
      Summary: {
        'Marked Deletion': deleted_count,
        'Verioned Objects': versioned_count
      }
    }
  },
  count_versions: async function count_versions(object, options) {
    const client = await this.init(options)
    const version_result = await client.send( 
      new ListObjectVersionsCommand({Bucket: options.Bucket, Prefix: object.Key})
    )
    let number_of_versions
    if (version_result.Versions) {
      number_of_versions = await version_result.Versions.reduce((total, object) => (
        object.Key ? total + 1 : total + 0), 0)
      if (number_of_versions > 1) {
        number_of_versions += 1
      }
    }
    console.log('result', number_of_versions)
    return number_of_versions
  },
  // find all "folders" on a given path
  find_prefix: async function find_prefix(common_options, client_options) {
    let all_prefixes = []
    const init = await this.init(client_options)
    async function list_all_prefixes(client, bucket, prefix, token) {
      console.log('processing prefix', prefix)
      const options = {
        Bucket: bucket,
        Prefix: prefix,
        Delimiter: '/',
        ContinuationToken: token || '' // NextContinuationToken,
      }
      const prefixes = await client.send(new ListObjectsV2Command(options))
      // console.log(...prefixes.CommonPrefixes)
      // recursively go down one level until all prefixes are returned
      if (prefixes.CommonPrefixes && prefixes.CommonPrefixes.length) {
        all_prefixes = all_prefixes.concat(...prefixes.CommonPrefixes)
        const to_process = prefixes.CommonPrefixes
        to_process.forEach((item) => (
          list_all_prefixes(client, bucket, item.Prefix, prefixes.NextContinuationToken)
        ))
        if (prefixes.IsTruncated) {
          console.log('found truncated result')
          await list_all_prefixes(client, bucket, prefix, prefixes.NextContinuationToken)
        }
      }
      return all_prefixes
    }
    await list_all_prefixes(init, common_options.bucket, common_options.path)
    return all_prefixes
  },
  // list file in the current level
  list_files: async function list_files(prefix, token, client_options) {
    const client = await this.init(client_options)
    const options = {
      Bucket: client_options.bucket,
      Prefix: prefix,
      Delimiter: '/',
      ContinuationToken: token || '' // NextContinuationToken,
    }
    const file_list = await client.send(new ListObjectsV2Command(options))
    if (file_list.IsTruncated) {
      this.find_prefix(prefix, token, client_options)
    }
    return file_list
  },
  statistics: async function statistics(common_options, command_options, versioned = false) {
    console.log('gettings stats for bucket ', command_options.bucket)
    let stats = {
      file_count: 0,
      total_size: 0,
      file_with_versions: 0,
      avg_versions_per_file: 0
    }
    let num_versions_per_file = []
    const prefixes = await this.find_prefix(command_options, common_options)
    // const prefix_result = await Promise.all(prefixes)
    console.log(prefixes)
    // do {
    //   iterations ++
    //   command_result = await client.send(new ListObjectsV2Command(options))
    //   console.log(command_result)
    //   if ( command_result.Contents) {
    //     stats.file_count += command_result.KeyCount
    //     for (content of command_result.Contents){
    //       content.Size ? stats.total_size += content.Size : content.total_size += 0
    //       if( versioned === true) { 
    //         num_versions_per_file.push(this.count_versions(content)) 
    //       }
    //     }
    //   }
    //   if ( command_result.IsTruncated) {
    //     [options.ContinuationToken] = command_result.NextContinuationToken
    //   }
    //   console.log(stats)
    //   break
    // } while (command_result.IsTruncated)
    // const cose = await Promise.all(num_versions_per_file)
    // console.log(cose)
    // let sum_versions_in_files = 0
    // for (version of num_versions_per_file) {
    //   if (version > 0) {
    //     stats.file_with_versions += 1
    //     sum_versions_in_files += version
    //   }
    // }
    // stats.avg_versions_per_file = sum_versions_in_files / stats.file_with_versions
    // console.log(stats)
    return stats
  }
}

module.exports = { S3Action }