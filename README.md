# s3-tools

Command line utility for managing S3 Buckets
AWS offers it's inventory feature, for S3 compatible storages there is not such a thing.
That's why I started to write my own implementation.

### Prerequisites

This script is written and tested for NodeJS 16.x
If you have Node installed on your system simply run
```
npm install
node src/index.js
```
Otherwise i'm building binaries and executables at every release.

### Usage

The script accept command-line arguments all the configuration can be passed at runtime.
Could be extended in the future with config files.

Example of a bucket list:
```
s3-tools --access-key myaccesskey --secret-key mysecretkey bucket-list
```

Example of a inventory run on a specific path:
```
s3-tools --access-key my_ak --secret-key my_sk inventory --bucket abucketname --path /pictures
```

Example of an inventory run with results pushed on Elasticsearch:
```
s3-tools --access-key my_ak --secret-key my_sk inventory --bucket my_huge_bucket --elasticsearch-address https://myelastic:9200 --elasticsearch-apikey the_apy_key
```

### Tip

For huge buckets I reccomend using tmux

### Common Options ###

| Parameter                     | Example                | Description  |	
| :-----------------------------|:----------------------:|:-------------|
| --url **(required)** 	        |	s3.scalablestorage.it  | url where your s3 storage reside |
| --region **(required)** 	    |	it-mi1                 | the region |
| --access-key **(required)**   | user                   | the s3 user access key |
| --secret-key **(required)** 	|	secret                 | the user secret key |

### command `bucket-list` ###
show a list of buckets that can be accessed

### command `bucket-versioning` ###

check if on the specified bucket versioning is enabled or not

| Parameter                     | Example                | Description  |	
| :-----------------------------|:----------------------:|:-------------|
| --bucket **(required)**  		  | mybucket	             | the bucket to check |

### command `inventory` ###

take an inventory of the bucket or a specified prefix (the S3 "folders") and save the result
 **at the moment of writing the only destination supported is Elasticsearch**
 **tested with ES 8.2**
 the parallel-tree can boost significatively the execution time but on very large bucket can eat up your RAM pretty fast (i'm working on a solution)

| Parameter                         | Example                | Description                                             |	
| :---------------------------------|:----------------------:|:--------------------------------------------------------|
| --bucket **(required)**  		      | mybucket	             | the bucket you want to scan                             |
| --path                            | /folder1/folder2       | if you want to limit the scan on a specified folder     |
| --parallel-tree' **EXPERIMENTAL** |                        | find all folders and run parallel scan on all of them   |
| --elasticsearch-address           | https://myes:9200      | push results to the specified elasticsearch cluster     |
| --elasticsearch-apikey            | apikey==               | if auth is enabled use api-key(no other auth supported) |



## Considerations

The script was written for infrastrucutre admins or devops.
If used against public service providers could result in a very expensive bill, use it at your own risk.

## Contributing

Anyone is free to contribute, any suggestion, improvement or bugfixing is much welcome

## Authors

* Giordano Corradini - *Initial work*


## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details