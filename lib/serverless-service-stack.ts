import * as cdk from '@aws-cdk/core';
import * as iam from '@aws-cdk/aws-iam';
import { CfnOutput, Construct, DockerImage, RemovalPolicy, StackProps, Stage, Tag } from '@aws-cdk/core';
import s3 = require('@aws-cdk/aws-s3');
import * as lambda from '@aws-cdk/aws-lambda';
import { LambdaFunction } from '@aws-cdk/aws-events-targets';
import * as s3n from '@aws-cdk/aws-s3-notifications';
import { Rule, Schedule } from '@aws-cdk/aws-events';
import * as secretsmanager from '@aws-cdk/aws-secretsmanager';
import { countReset } from 'console';

export class ServerlessServiceStack extends cdk.Stack {
  constructor(scope: cdk.Construct, id: string, props?: StackProps) {
    super(scope, id, props);
    const lambdaRole = iam.Role.fromRoleArn(
      this,
      'imported-role',
      `arn:aws:iam::${cdk.Stack.of(this).account}:role/servicenowlambda`,
      { mutable: true },
    );

    const secretmanagerForSecrets = new secretsmanager.Secret(this, "APISecrets" + this.stackName,
      { //DO NOT change this object, it will create new blank secretmanager 
        generateSecretString: {
          secretStringTemplate: '{"templatekey": "templatevalue"}',
          generateStringKey: 'password',
        },
      },
    );

    secretmanagerForSecrets.applyRemovalPolicy(RemovalPolicy.RETAIN)
    //remember to add url,username,password hints to secretmanager so lambda can fetch them
    datapipeServiceNowTable(this, "now/table/task", this.stackName, secretmanagerForSecrets, this.region,
      lambdaRole, "Servicenow-Table-Task-URL", "Servicenow-Table-Task-Username", "Servicenow-Table-Task-password", "com.cgi.lambda.apifetch.LambdaFunctionHandler",
      lambda.Code.fromAsset("./lambda/servicenow/ServiceNowDataToS3/",
        {
          bundling:
          {
            command:
              ["/bin/sh", "-c", "mvn clean install" +
                "&& cp ./target/servicenow-to-s3-lambda-1.0.0.jar /asset-output/"], 
            image: lambda.Runtime.JAVA_8.bundlingImage, 
            user: "root", 
            outputType: cdk.BundlingOutput.ARCHIVED
          }
        }
      )
    )
    
  }
}

function datapipeServiceNowTable(construct: cdk.Construct, rajapintaName: string, appnameAndEnv: string, secretmanager: secretsmanager.Secret, region: string, lambdaRole: iam.IRole, urlsecretHint: string, secretusernameHint: string, secretpasswordHint: string, handler: string, code: lambda.Code) {
  const resourcenaming = "-" + rajapintaName + "-" + appnameAndEnv

  const landingBucket = new s3.Bucket(construct, 'LandingBucket' + resourcenaming, {
    removalPolicy: cdk.RemovalPolicy.DESTROY,
  });
  const databucket = new s3.Bucket(construct, 'DataBucket' + resourcenaming, {
    removalPolicy: cdk.RemovalPolicy.DESTROY,
  });

  const apiLambda = new lambda.Function(construct, 'APIFetch' + resourcenaming, {
    code: code,
    handler: handler,
    runtime: lambda.Runtime.JAVA_8,
    environment: {
      "secrets": secretmanager.secretArn,
      "region": region,
      "alert_string": "## VAYLA AWS-HALYTYS: SERVICENOW:",
      "charset": "UTF-8",
      "password": secretpasswordHint,
      "username": secretusernameHint,
      "service_url": urlsecretHint,
      "splitlimit": "1500",
      "s3_bucket_name": landingBucket.bucketName
    },
    role: lambdaRole
  });
  secretmanager.grantRead(apiLambda)
  landingBucket.grantPut(apiLambda)


  /* Add these if two phased prosessing
  const converterLambda=new lambda.Function(construct, 'ADEConverter'+resourcenaming, {
    code: lambda.Code.fromAsset("./lambdas/converterlambda/path/", ),
    handler: "com.cgi.lambda.apifetch.LambdaFunctionHandler",
    environment: { 
      "DevARN": construct.node.tryGetContext('ADEDevACL'),"PRODARN": construct.node.tryGetContext('ADEPRODACL'),"QAARN": construct.node.tryGetContext('ADEQAACL'),
     "is_dst": "", "not_dst": "" },
    runtime: lambda.Runtime.JAVA_8,
  });  
  landingBucket.addEventNotification(s3.EventType.OBJECT_CREATED, new s3n.LambdaDestination(converterLambda))
 
  databucket.grantPut(converterLambda)
  landingBucket.grantRead(converterLambda)
  secretmanager.grantRead(apiLambda)
*/

  const rule = new Rule(construct, "dailyRun" + resourcenaming, {
    schedule: Schedule.expression("cron(15 3 * * ? *)"),
    //  targets: [new LambdaFunction(apiLambda)],
  });
  cdk.Tags.of(landingBucket).add("APIFetch", rajapintaName)
  cdk.Tags.of(databucket).add("APIFetch", rajapintaName)
  //cdk.Tags.of(apiLambda).add("APIFetch",rajapintaName)
  cdk.Tags.of(rule).add("APIFetch", rajapintaName)
}
//stepfunction (optional, nice to have no time atm)



