const { program } = require('commander')
const { S3Action } = require('./functions/actions')
const { S3Inventory } = require('./functions/inventory')

async function start() {
  program
    .name('s3-tools')
    .description('various s3 command tools')
    .version('0.2.0')
    .option('--url <url>', 'url of the s3 storage (default s3.scalablestorage.it)', 's3.scalablestorage.it')
    .option('--region <region>', 'the s3 region (default it-mi1)', 'it-mi1')
    .requiredOption('--access-key <access-key>', 'the access key')
    .requiredOption('--secret-key <secret-key>', 'the secret key')
  program
    .command('bucket-list')
    .action(() => {
      const res = S3Action.bucketList(program.opts())
      console.log(res)
    })
  program
    .command('bucket-versioning-status')
    .requiredOption('--bucket <bucket>', 'the name of the bucket')
    .action((option) => {
      const res = S3Action.bucketVersioning(program.opts(), option)
      console.log(res)
    })
  program
    .command('inventory')
    .requiredOption('--bucket <bucket>', 'the name of the bucket')
    .option('--path <path>', 'the starting path (careful not use this option with large buckets)')
    .option('--parallel-tree', '*EXPERIMENTAL* run recursive search of all subtrees and run list in parallel mode')
    .option('--elasticsearch-address <address>', 'save result into elasticsearch for analysis')
    .option('--elasticsearch-apikey <apikey>', 'if auth is enable specify an apikey')
    .action(async (option) => {
      await S3Inventory.inventory(program.opts(), option)
    })
  program.parse()
}

start()
