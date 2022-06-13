const { ListObjectVersionsCommand } = require('@aws-sdk/client-s3')

const count_versions = async function (object, client, options) {
  const version_result = await client.send( new ListObjectVersionsCommand({Bucket: options.Bucket, Prefix: object.Key}))
  let number_of_versions
  if (version_result.Versions) {
    number_of_versions = await version_result.Versions.reduce((total, object) =>
      object.Key ? total + 1 : total + 0, 0)
      number_of_versions > 1 ? number_of_versions += 1 : number_of_versions = 0
  }
    return number_of_versions
}

module.exports = { count_versions }
