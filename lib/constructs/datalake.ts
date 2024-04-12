import { Construct } from 'constructs';
import { CfnOutput, RemovalPolicy, aws_s3 as s3 } from 'aws-cdk-lib';
import * as glue from '@aws-cdk/aws-glue-alpha';
import { aws_iam as iam } from 'aws-cdk-lib';
import { aws_logs as logs } from 'aws-cdk-lib';
import { CfnCrawler } from 'aws-cdk-lib/aws-glue';

export interface DataLakeProps {

}

export class DataLake extends Construct {
  constructor(scope: Construct, id: string, props: DataLakeProps) {
    super(scope, id);

    const dataLakeBucket = new s3.Bucket(this, 'DataLakeBucket', {
      autoDeleteObjects: true,
      removalPolicy: RemovalPolicy.DESTROY
    })

    const sparkUiBucket = new s3.Bucket(this, 'SparkUiBucket', {
      autoDeleteObjects: true,
      removalPolicy: RemovalPolicy.DESTROY
    })


    const transformedBucket = new s3.Bucket(this, 'TransformedBucket', {
      autoDeleteObjects: true,
      removalPolicy: RemovalPolicy.DESTROY
    })

    const nycTaxiDatabase = new glue.Database(this, 'NycTaxiDatabase', {
      databaseName: 'nyctaxi_db'
    })


    const nycTaxiCrawlerRole = new iam.Role(this, 'NycTaxiCrawlerRole', {
      assumedBy: new iam.ServicePrincipal('glue.amazonaws.com')
    });
    nycTaxiCrawlerRole.addManagedPolicy(
      iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSGlueServiceRole'));
    nycTaxiCrawlerRole.addToPolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        resources: [
          `${dataLakeBucket.bucketArn}/*`,
          `${dataLakeBucket.bucketArn}`,
          `${transformedBucket.bucketArn}/*`,
          `${transformedBucket.bucketArn}`
        ],
        actions: ["s3:GetObject", "s3:PutObject", "s3:ListBucket"]
      })
    );


    new CfnCrawler(this, 'NycTaxiCrawler', {
      role: nycTaxiCrawlerRole.roleArn,
      targets: {
        s3Targets: [{
          path: `s3://${dataLakeBucket.bucketName}/nyc-taxi/yellow-tripdata`,
        }],
      },
      databaseName: nycTaxiDatabase.databaseName,
      tablePrefix: 'raw_'
    })

    new CfnCrawler(this, 'TaxiZoneCrawler', {
      role: nycTaxiCrawlerRole.roleArn,
      targets: {
        s3Targets: [{
          path: `s3://${dataLakeBucket.bucketName}/nyc-taxi/taxi_zone_lookup`,
        }],
      },
      databaseName: nycTaxiDatabase.databaseName,
    })

    new glue.Job(this, 'NycTaxiTripJob', {
      executable: glue.JobExecutable.pythonEtl({
        glueVersion: glue.GlueVersion.V4_0,
        pythonVersion: glue.PythonVersion.THREE,
        script: glue.Code.fromAsset('src/etl_script.py')
      }),
      // SparkUI有効化
      sparkUI: {
        enabled: true,
        bucket: sparkUiBucket
      },
      enableProfilingMetrics: true,
      // ロギング有効化
      continuousLogging: {
        enabled: true,
        logGroup: new logs.LogGroup(this, 'GlueJobLogGroup', {
          removalPolicy: RemovalPolicy.DESTROY
        }),
        quiet: true
      },
      executionClass: glue.ExecutionClass.FLEX,
      workerType: glue.WorkerType.G_2X,
      workerCount: 10,
      role: nycTaxiCrawlerRole,
      defaultArguments: {
        '--BUCKET_NAME': transformedBucket.bucketName,
        '--enable-auto-scaling': 'true', // AutoScaling
        '--job-bookmark-option': 'job-bookmark-enable' // Job Bookmark
      },
    })


    new CfnCrawler(this, 'TransformedDataCrawler', {
      role: nycTaxiCrawlerRole.roleArn,
      targets: {
        s3Targets: [{
          path: `s3://${transformedBucket.bucketName}/nyc-taxi/yellow-tripdata/`,
        }],
      },
      databaseName: nycTaxiDatabase.databaseName,
    })





    new CfnOutput(this, 'DataLakeBucketName', { value: dataLakeBucket.bucketName, key: 'DataLakeBucketName' })


  }
}