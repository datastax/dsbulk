# DataStax Bulk Loader URL Utilities

This module contains URL utilities for DSBulk, among which:

1. DSBulk's `BulkLoaderURLStreamHandlerFactory`, which is DSBulk's default factory for URL handlers;
2. A URL stream handler for reading / writing to standard input / output.
3. A URL stream handler for reading from AWS S3 URLs.
   1. Every S3 URL must contain the proper query parameters from which an `S3Client` can be built. These parameters are:
      1. `region` (required): The AWS region, such as `us-west-1`.
      2. `profile` (optional, preferred): The profile to use to provide credentials. See [the AWS SDK credentials documentation](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials.html) for more information.
      3. `accessKeyId` and `secretKeyId` (optional, discouraged): In case you don't have a profile set up, you can use this less-secure method. Both parameters are required if you choose this.
   2. If only the `region` is provided, DSBulk will fall back to the default AWS credentials provider, which handles role-based credentials.
   3. To prevent unnecessary client re-creation when using many URLs from a `urlfile`, `S3Client`s are cached by the query parameters. The size of the cache is controlled by the `dsbulk.s3.clientCacheSize` option (default: 20).
