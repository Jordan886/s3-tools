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
    try {
      await elasticClient.ilm.putLifecycle({
        name: `${options.indexName}_retention`,
        policy: {
          phases: {
            hot: {
              min_age: '0ms',
              actions: {
                rollover: {
                  max_age: '1d'
                },
                set_priority: {
                  priority: 100
                }
              }
            },
            delete: {
              min_age: options.retention || '30d',
              actions: {
                delete: { }
              }
            }
          }
        }
      })
    } catch (err) {
      console.log(err)
      throw new Error('Error in boostrap: create/update lifecycle policy')
    }
    try {
      await elasticClient.indices.putTemplate({
        name: `${options.indexName}_template`,
        create: false,
        index_patterns: [`${options.indexName}-*`],
        settings: {
          number_of_shards: options.numShards || 1,
          number_of_replicas: options.Replicas || 1,
          'index.lifecycle.name': `${options.indexName}_retention`,
          'index.lifecycle.rollover_alias': options.indexName
        }
      })
    } catch (err) {
      console.log(err)
      throw new Error('Error in boostrap: create/update index template')
    }
    const bootstrap_index_name = `${options.indexName}-000001`
    const bootstrap_index_exist = await elasticClient.indices.exists({
      index: bootstrap_index_name
    })
    if (!bootstrap_index_exist) {
      try {
        await elasticClient.indices.create({
          index: bootstrap_index_name,
          aliases: {
            [options.indexName]: {
              is_write_index: true
            }
          }
        })
      } catch (err) {
        console.log(err)
        console.log(JSON.stringify(err.meta.body))
        throw new Error('Error in bootstrap: create first index')
      }
    }
    console.log('bootstrap terminated successfully')
  },
  saveResults: async function saveResults(options, body) {
    const elasticClient = await this.init(options)
    // const bulk = body.flatMap((doc) => [{ index: { _index: options.indexName } }, doc])
    await elasticClient.helpers.bulk({
      datasource: body,
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
