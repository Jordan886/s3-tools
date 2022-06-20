const {
  S3Client,
  ListObjectsV2Command,
  ListObjectVersionsCommand,
} = require('@aws-sdk/client-s3')

const { elasticSearch } = require('./elasticsearch')

const S3Inventory = {
  init: async function init(client_options) {
    const client = new S3Client({
      credentials: {
        accessKeyId: client_options.accessKey,
        secretAccessKey: client_options.secretKey,
      },
      forcePathStyle: true, // for better compatibility
      endpoint: `https://${client_options.url}`, // TODO: allow http
      region: client_options.region
    })
    return client
  },
  s3PrefixDebugger: async function s3PrefixDebugger(client_options, command_options) {
    const command = command_options
    const client = await this.init(client_options)
    const options = {
      Bucket: command.bucket,
      Prefix: command.path,
      Delimiter: command.path ? '/' : '',
      ContinuationToken: command.NextContinuationToken || ''
    }
    const prefix_list = await client.send(new ListObjectsV2Command(options))
    return prefix_list
  },
  s3FindAllPrefixes: async function s3FindAllPrefixes(client_options, command_options) {
    const init = await this.init(client_options)
    const prefixes = []
    let results_found = false
    const processed_prefixes = []
    const resultFinder = async function resultFinder(client, command) {
      const options = {
        Bucket: command.bucket,
        Prefix: command.path,
        Delimiter: command.path ? '/' : '',
        ContinuationToken: command.NextContinuationToken || ''
      }
      // console.log('listing with options', options)
      const prefix_list = await client.send(new ListObjectsV2Command(options))
      processed_prefixes.push(prefix_list.Prefix)
      if (prefix_list.CommonPrefixes) {
        results_found = true
        prefix_list.CommonPrefixes.map((item) => prefixes.push(item.Prefix))
        if (prefix_list.IsTruncated) {
          const new_options = command
          new_options.ContinuationToken = prefix_list.NextContinuationToken
          await resultFinder(client, new_options)
        }
      } else {
        // if the path doesnt have subfolers list itself
        prefixes.push(command.path)
        results_found = false
      }
      return results_found
    }
    const recursiveFinder = async function recursiveFinder(client, command) {
      const result = await resultFinder(client, command)
      if (result) {
        // console.log('found result')
        // find prefixes that are not yet processed
        // console.log(`found items ${prefixes}`)
        // console.log(`processed items ${processed_prefixes}`)
        const prefixes_to_process = prefixes.filter((item) => !processed_prefixes.includes(item))
        // console.log(`processing following prefixes: ${prefixes_to_process}`)
        await Promise.all(prefixes_to_process.map(async (item) => {
          const new_options = command
          new_options.path = item
          await recursiveFinder(client, new_options)
        }))
      }
    }
    await recursiveFinder(init, command_options)
    return prefixes
  },
  s3ListObjectVersion: async function s3ListObjectVersion(
    client_options, command_options, save_action, save_action_options
  ) {
    const init = await this.init(client_options)
    const file_count = {
      versioned: 0,
      deleted: 0
    }
    const PaginatedResult = async function PaginatedResult(client, command, save, save_options) {
      const options = {
        Bucket: command.bucket,
        Prefix: command.path,
        Delimiter: command.parallelTree ? '/' : '',
        KeyMarker: command.KeyMarker || '',
        VersionIdMarker: command.VersionIdMarker || ''
      }
      console.log('listing with options', options)
      const file_list = await client.send(new ListObjectVersionsCommand(options))
      // console.log(file_list)
      if (file_list.Versions || file_list.DeleteMarkers) {
        if (file_list.Versions) {
          console.log(`found ${file_list.Versions.length} file versions`)
          file_count.versioned += file_list.Versions.length
          save(save_options, file_list.Versions)
        }
        if (file_list.DeleteMarkers) {
          console.log(`found ${file_list.DeleteMarkers.length} file deleted`)
          // add deleted marker to distinguish deleted files during analisys
          const enriched_result = file_list.DeleteMarkers.map(
            (item) => ({ ...item, Deleted: true })
          )
          file_count.deleted += file_list.DeleteMarkers.length
          save(save_options, enriched_result)
        }
        if (file_list.IsTruncated) {
          console.log('found truncated result')
          const new_options = command
          new_options.KeyMarker = file_list.NextKeyMarker
          new_options.VersionIdMarker = file_list.NextVersionIdMarker
          await PaginatedResult(client, new_options, save, save_options)
        }
      }
      // return file_list
    }
    await PaginatedResult(init, command_options, save_action, save_action_options)
    return file_count
  },
  inventory: async function inventory(client_options, command_options) {
    // initialize elasticsearch
    console.log(command_options)
    const save_options = {
      elasticsearch_enabled: false
    }
    if (command_options.elasticsearchAddress) {
      save_options.elasticsearch_enabled = true
      save_options.address = command_options.elasticsearchAddress
      const now = new Date()
      const date = now.toISOString().split('T', 1)
      save_options.indexName = `${command_options.bucket}-${date}`
      if (command_options.elasticsearchApikey) {
        save_options.apiKey = command_options.elasticsearchApikey
      }
      await elasticSearch.indexBootstrap(save_options)
    }
    // const result = await this.s3ListObjectVersion(client_options, command_options)
    // const result = await this.s3PrefixDebugger(client_options, command_options)
    if (command_options.parallelTree) {
      const folders = await this.s3FindAllPrefixes(client_options, command_options)
      console.log('found following folders to process:', folders)
      await Promise.all(folders.map(async (item) => {
        console.log(`listing files in ${item}`)
        const new_options = command_options
        new_options.path = item
        await this.s3ListObjectVersion(
          client_options, new_options, this.save, save_options
        )
      }))
    }
    const result = await this.s3ListObjectVersion(
      client_options, command_options, this.save, save_options
    )
    console.log(`done, file count is ${JSON.stringify(result, null, 2)}`)
    // console.log(`the found the following files ${file_list}`)
  },
  // save result in various ways (callback to pass to listobject)
  save: async function save(save_options, body) {
    if (save_options.elasticsearch_enabled) {
      await elasticSearch.saveResults(save_options, body)
    }
  }
}

module.exports = { S3Inventory }
