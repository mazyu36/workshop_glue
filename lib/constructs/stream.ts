import { Construct } from 'constructs';
import * as glue from '@aws-cdk/aws-glue-alpha';
import { Duration, RemovalPolicy, aws_kinesis as kinesis } from 'aws-cdk-lib';
import { aws_s3 as s3 } from 'aws-cdk-lib';
import { aws_s3_deployment as s3deploy } from 'aws-cdk-lib';
import * as cdk from 'aws-cdk-lib';
import { CfnTable } from 'aws-cdk-lib/aws-glue';

export interface StreamProps {

}

export class Stream extends Construct {
  constructor(scope: Construct, id: string, props: StreamProps) {
    super(scope, id);

    const stream = new kinesis.Stream(this, 'KinesisStream', {
      retentionPeriod: Duration.hours(24),
      shardCount: 2,
      streamName: 'glueworkshop'
    })

    const streamJobBucket = new s3.Bucket(this, 'StreamJobBucket', {
      autoDeleteObjects: true,
      removalPolicy: RemovalPolicy.DESTROY
    })

    new s3deploy.BucketDeployment(this, 'StreamingDeployment', {
      sources: [s3deploy.Source.asset('./data/streaming')],
      destinationBucket: streamJobBucket,
      destinationKeyPrefix: 'input/streaming'
    })


    const sparkUiBucket = new s3.Bucket(this, 'StreamSparkUiBucket', {
      autoDeleteObjects: true,
      removalPolicy: RemovalPolicy.DESTROY
    })

    const database = new glue.Database(this, 'StreamingDatabase', {
      databaseName: 'streaming-database'
    })


    new CfnTable(scope, "JobStreamingTable", {
      databaseName: database.databaseName,
      catalogId: cdk.Stack.of(scope).account,
      tableInput: {
        name: 'json-streaming-table',
        tableType: "EXTERNAL_TABLE",
        parameters: {
          "classification": 'json',
        },
        storageDescriptor: {
          columns: [
            {
              'name': 'uuid',
              'type': 'string'
            },
            {
              'name': 'country',
              'type': 'string'
            },
            {
              'name': 'itemtype',
              'type': 'string'
            },
            {
              'name': 'saleschannel',
              'type': 'string'
            },
            {
              'name': 'orderpriority',
              'type': 'string'
            },
            {
              'name': 'orderdate',
              'type': 'string'
            },
            {
              'name': 'region',
              'type': 'string'
            },
            {
              'name': 'shipdate',
              'type': 'string'
            },
            {
              'name': 'unitssold',
              'type': 'string'
            },
            {
              'name': 'unitprice',
              'type': 'string'
            },
            {
              'name': 'unitcost',
              'type': 'string'
            },
            {
              'name': 'totalrevenue',
              'type': 'string'
            },
            {
              'name': 'totalcost',
              'type': 'string'
            },
            {
              'name': 'totalprofit',
              'type': 'string'
            },
          ],
          inputFormat: "org.apache.hadoop.mapred.TextInputFormat",
          outputFormat: "rg.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
          parameters: {
            "endpointUrl": `https://kinesis.${cdk.Stack.of(scope).region}.amazonaws.com`,
            "streamName": stream.streamName,
            "typeOfData": "kinesis"
          },
          serdeInfo: {
            serializationLibrary: "org.openx.data.jsonserde.JsonSerDe",
            parameters: {
              "paths": "Country,ItemType,OrderDate,OrderPriority,Region,SalesChannel,ShipDate,TotalCost,TotalProfit,TotalRevenue,UnitCost,UnitPrice,UnitsSold,uuid"
            }
          },
        },
      }
    })

    const streamingJob = new glue.Job(this, 'PythonStreamingJob', {
      executable: glue.JobExecutable.pythonStreaming({
        glueVersion: glue.GlueVersion.V4_0,
        pythonVersion: glue.PythonVersion.THREE,
        script: glue.Code.fromAsset('src/streaming.py')
      }),
      // SparkUI有効化
      sparkUI: {
        enabled: true,
        bucket: sparkUiBucket
      },
      enableProfilingMetrics: true,
      workerType: glue.WorkerType.G_1X,
      workerCount: 10,
      // role: nycTaxiCrawlerRole,
      defaultArguments: {
        '--s3_bucket': `s3://${streamJobBucket.bucketName}/`,
        // '--enable-auto-scaling': 'true', // AutoScaling,
        '--TempDir': `s3://${streamJobBucket.bucketName}/`,
        '--job-bookmark-option': 'job-bookmark-enable' // Job Bookmark
      },
    })

    // 権限付与
    streamJobBucket.grantReadWrite(streamingJob)
    stream.grantRead(streamingJob)

  }
}