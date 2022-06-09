const fs  = require('fs')
const path = require('path')
const { parse } = require('json2csv');
const { program } = require('commander')
const { S3Action } = require('./actions')

async function start () {
  program
    .name('s3-tools')
    .description('various s3 command tools')
    .version('0.1.0')
    .option('--url <url>', 'url of the s3 storage (default s3.scalablestorage.it)', 's3.scalablestorage.it')
    .option('--region <region>', 'the s3 region (default it-mi1)', 'it-mi1')
    .requiredOption('--access-key <access-key>', 'the access key')
    .requiredOption('--secret-key <secret-key>', 'the secret key')
  program
    .command('bucket-list')
    .action(async function () {
      res = await S3Action.bucketList(program.opts())
      console.log(res)
    })
  program
    .command('bucket-versioning-status')
    .requiredOption('--bucket <bucket>', 'the name of the bucket')
    .action(async function (option) {
      res = await S3Action.bucketVersioning(program.opts(), option)
      console.log(res)
    })
  program
    .command('count-deleted-objects')
    .requiredOption('--bucket <bucket>', 'the name of the bucket')
    .option('--save-details <saveDetails>', 'path where to save details in csv format')
    .action(async function (option) {
      res = await S3Action.deletedObjects(program.opts(), option, option.saveDetails ? true : false )
      console.log(res.Summary)
      if (option.saveDetails) {
        const save_path_deleted = path.join(option.saveDetails, 'bucket_deleted.csv')
        const save_path_versioned = path.join(option.saveDetails, 'bucket_versioned.csv')
        try {
          const csv_deleted = parse(res.Details.DeleteMarkers)
          fs.writeFileSync(save_path_deleted, csv_deleted )
          console.log('saved file in', save_path_deleted)
          const csv_versioned = parse(res.Details.Versions)
          fs.writeFileSync(save_path_versioned, csv_versioned )
          console.log('saved file in', save_path_versioned)
        } catch (error) {
          console.error(error.message)
        }
      }
    })
  program.parse()
  
}

start()