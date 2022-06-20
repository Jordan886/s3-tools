const { Client } = require('@elastic/elasticsearch')

/**
 * class used to manage Elasticsearch interface
 * @param {object} options object containing all options
 * @param {string} options.address the address of elasticsearch cluster/node (eg localhost:9200)
 * @param {string} option.apiKey if auth is enable specify the authkey
 * @param {string} options.retention specify the retention of the index
 * @param {string} options.indexName the prefix name of the index (default to bucket name)
 * @param {integer} options.numShards the number of shards (for performance tuning)
 * @param {integer} options.Replicas the number of index replicas
*/
const elasticSearch = {
  init: async function init(options) {
    const client_options = {
      node: options.address,
      tls: {
        rejectUnauthorized: false
      }
    }
    if (options.apiKey) {
      client_options.auth = {
        apiKey: options.apiKey
      }
    }
    // console.log(client_options)
    const client = new Client(client_options)
    return client
  },
  indexBootstrap: async function indexBootstrap(options) {
    const elasticClient = await this.init(options)
    const bootstrap_index_name = `${options.indexName}`
    const bootstrap_index_exist = await elasticClient.indices.exists({
      index: bootstrap_index_name
    })
    if (!bootstrap_index_exist) {
      try {
        await elasticClient.indices.create({
          index: bootstrap_index_name,
          settings: {
            number_of_shards: options.numShards || 1,
            number_of_replicas: options.Replicas || 0
          }
        })
      } catch (err) {
        console.log(err)
        console.log(JSON.stringify(err.meta.body))
        throw new Error('Error in bootstrap: create first index')
      }
    }
    console.log('bootstrap terminated successfully')
    return bootstrap_index_name
  },
  saveResults: async function saveResults(options, body) {
    const elasticClient = await this.init(options)
    // const bulk = body.flatMap((doc) => [{ index: { _index: options.indexName } }, doc])
    // add new key "prefix" for better analysis
    const enriched_body = body.map((item) => {
      const new_item = item
      if (new_item.Key) {
        // build prefix by taking first part of the path without filename
        const folder = item.Key.slice(0, new_item.Key.lastIndexOf('/'))
        new_item.Prefix = folder
      }
      return new_item
    })
    await elasticClient.helpers.bulk({
      datasource: enriched_body,
      refreshOnCompletion: true,
      onDocument() {
        return {
          create: { _index: options.indexName }
        }
      }
    })
  }
}

module.exports = { elasticSearch }
