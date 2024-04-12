import { Construct } from 'constructs'
import { aws_ec2 as ec2 } from 'aws-cdk-lib'
import { aws_rds as rds } from 'aws-cdk-lib'
import * as glue from '@aws-cdk/aws-glue-alpha'
import * as cdk from 'aws-cdk-lib'
import { CfnCrawler } from 'aws-cdk-lib/aws-glue'
import { RemovalPolicy } from 'aws-cdk-lib'
import { aws_s3 as s3 } from 'aws-cdk-lib'
import { aws_iam as iam } from 'aws-cdk-lib'

export interface JdbcRdsProps {

}

export class JdbcRds extends Construct {
  constructor(scope: Construct, id: string, props: JdbcRdsProps) {
    super(scope, id)

    const rdsJobBucket = new s3.Bucket(this, 'RdsJobBucket', {
      autoDeleteObjects: true,
      removalPolicy: RemovalPolicy.DESTROY
    })

    const sparkUiBucket = new s3.Bucket(this, 'RdsSparkUiBucket', {
      autoDeleteObjects: true,
      removalPolicy: RemovalPolicy.DESTROY
    })


    // vpc
    const vpc = new ec2.Vpc(this, 'Vpc', {
      natGateways: 0,
      maxAzs: 2,
      subnetConfiguration: [
        {
          name: 'private-subnet-',
          subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS,
          cidrMask: 24
        },
        {
          name: 'public-subnet-',
          subnetType: ec2.SubnetType.PUBLIC,
          cidrMask: 24
        }
      ]
    })

    vpc.addInterfaceEndpoint('SecretsManagerVpcEndpoint', {
      service: ec2.InterfaceVpcEndpointAwsService.SECRETS_MANAGER
    })

    vpc.addGatewayEndpoint('GlueGatewayEndpoint', {
      service: ec2.GatewayVpcEndpointAwsService.S3
    })


    // Aurora
    const auroraCluster = new rds.DatabaseCluster(this, 'DatabaseInstance', {
      engine: rds.DatabaseClusterEngine.auroraPostgres({ version: rds.AuroraPostgresEngineVersion.VER_13_13 }),
      vpc: vpc,
      vpcSubnets: vpc.selectSubnets({ subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS }),
      writer: rds.ClusterInstance.serverlessV2('Writer', {}),
      serverlessV2MinCapacity: 0.5,
      serverlessV2MaxCapacity: 1,
      credentials: { username: 'glueworkshop' },
      backup: {
        retention: cdk.Duration.days(1)
      },
      defaultDatabaseName: 'glueworkshop',
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      enableDataApi: true
    })


    // Glue
    const glueSecurityGroup = new ec2.SecurityGroup(this, 'GlueSG', {
      vpc: vpc,
    })
    glueSecurityGroup.connections.allowInternally(ec2.Port.allTcp())
    glueSecurityGroup.connections.allowTo(auroraCluster, ec2.Port.POSTGRES)


    const glueConnections = vpc.privateSubnets.map((subnet, index) =>
      new glue.Connection(this, `GlueConnection-${index}`, {
        type: glue.ConnectionType.JDBC,
        securityGroups: [glueSecurityGroup],
        subnet: subnet,
        properties: {
          JDBC_CONNECTION_URL: `jdbc:postgresql://${auroraCluster.clusterEndpoint.socketAddress}/glueworkshop`,
          JDBC_ENFORCE_SSL: 'false',
          SECRET_ID: auroraCluster.secret!.secretName
        }
      })
    )


    const glueRole = new iam.Role(this, 'RdsGlueRole', {
      assumedBy: new iam.ServicePrincipal('glue.amazonaws.com')
    })
    glueRole.addManagedPolicy(
      iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSGlueServiceRole'))

    auroraCluster.secret!.grantRead(glueRole)
    const database = new glue.Database(this, 'RdsDatabase', {
      databaseName: 'rds-database'
    })


    // Crawler
    const crawler = new CfnCrawler(this, 'RdsCraweler', {
      role: glueRole.roleArn,
      name: 'rds_crawler',
      targets: {
        jdbcTargets:
          [
            {
              connectionName: glueConnections[0].connectionName,
              path: 'glueworkshop/%'
            }
          ]
      },
      databaseName: database.databaseName,
      tablePrefix: 'rds_'
    })


    // Job
    const rdsJob = new glue.Job(this, 'RdsJob', {
      executable: glue.JobExecutable.pythonEtl({
        glueVersion: glue.GlueVersion.V4_0,
        pythonVersion: glue.PythonVersion.THREE,
        script: glue.Code.fromAsset('src/rds.py')
      }),
      sparkUI: {
        enabled: true,
        bucket: sparkUiBucket
      },
      enableProfilingMetrics: true,
      role: glueRole,
      workerType: glue.WorkerType.G_1X,
      workerCount: 4,
      executionClass: glue.ExecutionClass.FLEX,
      defaultArguments: {
        '--s3_bucket': `s3://${rdsJobBucket.bucketName}/`,
        '--region_name': cdk.Stack.of(scope).region,
        '--TempDir': `s3://${rdsJobBucket.bucketName}/output/temp`,
        '--TargetDir': `s3://${rdsJobBucket.bucketName}/output/orders`,
        '--job-bookmark-option': 'job-bookmark-enable' // Job Bookmark
      },
      connections: glueConnections
    })

    // 権限付与
    rdsJobBucket.grantReadWrite(rdsJob)




    // Output
    new cdk.CfnOutput(this, 'GlueCrawlerName', { value: crawler.name!, key: 'GlueCrawlerName' })

    new cdk.CfnOutput(this, 'GlueJobName', { value: rdsJob.jobName, key: 'GlueJobName' })

    new cdk.CfnOutput(this, 'AuroraArn', { value: auroraCluster.clusterArn, key: 'AuroraArn' })

    new cdk.CfnOutput(this, 'SecretArn', { value: auroraCluster.secret!.secretArn, key: 'SecretArn' })


  }
}