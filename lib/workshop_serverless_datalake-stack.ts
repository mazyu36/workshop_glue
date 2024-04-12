import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import { DataLake } from './constructs/datalake';
import { Stream } from './constructs/stream';
import { JdbcRds } from './constructs/jdbc-rds';
// import * as sqs from 'aws-cdk-lib/aws-sqs';

export class WorkshopServerlessDatalakeStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // new DataLake(this, 'DataLake', {})

    // new Stream(this, 'Stream', {})

    new JdbcRds(this, 'JdbcRds', {})
  }
}
