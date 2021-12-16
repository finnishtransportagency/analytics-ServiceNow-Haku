import * as cdk from '@aws-cdk/core';
import * as iam from '@aws-cdk/aws-iam';
import { CfnOutput, Construct, DockerImage, RemovalPolicy, Resource, StackProps, Stage, Tag } from '@aws-cdk/core';
import s3 = require('@aws-cdk/aws-s3');
import * as lambda from '@aws-cdk/aws-lambda';
import { LambdaFunction } from '@aws-cdk/aws-events-targets';
import * as s3n from '@aws-cdk/aws-s3-notifications';
import { Rule, Schedule } from '@aws-cdk/aws-events';
import * as secretsmanager from '@aws-cdk/aws-secretsmanager';

export class ServerlessServiceStack extends cdk.Stack {
  constructor(scope: cdk.Construct, id: string, props?: StackProps) {
    super(scope, id, props);
    const lambdaRole = iam.Role.fromRoleArn(
      this,
      'imported-role',
      `arn:aws:iam::${cdk.Stack.of(this).account}:role/servicenowlambda`,
      { mutable: true },
    );

    // Vain nimi, ensimmäinen osa
    var appname = this.stackName.split("-").slice(0)[0]
    // Vain env, viimeinen osa
    var env = this.stackName.split("-").slice(-1)[0]

    // HUOM: riittää 1 secret/env ==>> ServiceNow-APIFetch-dev
    var secretName = appname + "-APIFetch-" + env
    // "ServiceNowAPIdev" => ServiceNowAPIdev98FDFBFE-YECe7Myy38jY
    const secret = new secretsmanager.Secret(this, secretName,
      { //DO NOT change this object, it will create new blank secretmanager 
        generateSecretString: {
          secretStringTemplate: '{"username": "api username", "url": "api url"}',
		      generateStringKey: "password",
        },
        secretName: secretName
      },
    );

    secret.applyRemovalPolicy(RemovalPolicy.RETAIN)
    //remember to add username,password,url hints to secretmanager so lambda can fetch them
    

    // HUOM: riittää 1 bucket/env ==>> servicenow-apifetch-dev-data
    var dataBucketName = appname.toLowerCase() + "-apifetch-" + env.toLowerCase() + "-data"
    // alkuperäinen: 'data' + this.stackName
    // "data" => servicenow-dev-servicenow-service-de-data7e2128ca-v9oj09v2vhpu
    const dataBucket = new s3.Bucket(this, dataBucketName, {
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      bucketName: dataBucketName
    });

    var acl = this.node.tryGetContext('ADE'+env+'ACL')
    /*
    Replicate this for more lambda+bucket+crontrigger tasks sets and set secrets to secrets manager
    */
	

    // Incident lambda
    datapipeServiceNowTable(
      this,						// construct
      "incident",		// source name
      appname,			// appname == "ServiceNow"
      env,          // env == dev|test|qa|prod
      secret,	            // secret 
      this.region,				// region that is beign used
      lambdaRole,				  // role that allows cross region bucket put
      "com.cgi.lambda.apifetch.LambdaFunctionHandler", //handler used in code
      "mvn clean install && cp ./target/servicenow-to-s3-lambda-1.0.0.jar /asset-output/", //buildcommand
      "",		// Fill in query_string_default query string used to get data from API
      "",		// Fill in query_string_date date modifier if we want exact date
      dataBucket,  // Fill in databucket
      "servicenow2_u_case",		// Fill in s3 output_path
      "servicenow2_u_case",		// Fill in output_filename
      dataBucket.bucketName,  //"file-load-ade-runtime-" + env,		// Fill in manifestbucket_name
      "manifest/servicenow2_u_case",		// Fill in manifest_path,
      acl,		// ACL value for xaccount bucket write
      "true"	// coordinatetransformtoWgs84
    )


/*
    // services lambda
    datapipeServiceNowTable(
      this,						// construct
      "services",		// source name
      appname,			// appname == "ServiceNow"
      env,          // env == dev|test|qa|prod
      secret,	            // secret 
      this.region,				// region that is beign used
      lambdaRole,				  // role that allows cross region bucket put
      "com.cgi.lambda.apifetch.LambdaFunctionHandler", //handler used in code
      "mvn clean install && cp ./target/servicenow-to-s3-lambda-1.0.0.jar /asset-output/", //buildcommand
      "",		// Fill in query_string_default query string used to get data from API
      "",		// Fill in query_string_date date modifier if we want exact date
      dataBucket,  // Fill in databucket
      "servicenow2_services",		// Fill in s3 output_path
      "servicenow2_services",		// Fill in output_filename
      dataBucket.bucketName,  //"file-load-ade-runtime-" + env,		// Fill in manifestbucket_name
      "manifest/servicenow2_services",		// Fill in manifest_path,
      acl,		// ACL value for xaccount bucket write
      "true"	// coordinatetransformtoWgs84
    )
*/

  }
}

function datapipeServiceNowTable(
  construct: cdk.Construct,
  sourcename: string,
  appname: string,
  env: string,
  secret: secretsmanager.Secret,
  region: string,
  lambdaRole: iam.IRole,
  handler: string,
  buildcommand: string,
  query_string_default: string,
  query_string_date: string,
  output_bucket: s3.Bucket,
  output_path: string,
  output_filename: string,
  manifest_bucket: string,
  manifest_path: string,
  manifest_acl: string,
  ctransform: string) {

/*
  const resourcenaming = "-" + APIName + "-" + appnameAndEnv
  const databucket = new s3.Bucket(construct, 'DataBucket' + resourcenaming, {
    removalPolicy: cdk.RemovalPolicy.DESTROY,
  });
*/

  // ServiceNow-ApiFetch-dev-incident
  var functionName = appname + "-ApiFetch-" + env + "-" + sourcename

  // "incident" => ServiceNow-dev-ServiceNow-service-incident9FEF7035-rwPSYYliuOvt
  const lambdaFunc = new lambda.Function(construct, sourcename, {
    code: lambda.Code.fromAsset
      ("./lambda/servicenow/ServiceNowDataToS3/",
        {
        bundling:
        {
          command:
          ["/bin/sh", "-c", 
          buildcommand], 
          image: lambda.Runtime.JAVA_8.bundlingImage, 
          user: "root", 
          outputType: cdk.BundlingOutput.ARCHIVED
        }
      }
    ),
    functionName: functionName,
    handler: handler,
    runtime: lambda.Runtime.JAVA_8,
    environment: {
      "secret_arn": secret.secretArn,
      "region": region,
      "query_string_default": query_string_default,
      "query_string_date": query_string_date,
      "output_split_limit": "1500",
      "api_limit": "1000",
      "output_bucket": output_bucket.bucketName,
      "output_path": output_path,
      "output_filename": output_filename,
      "manifest_bucket": manifest_bucket,
      "manifest_path": manifest_path,
      "manifest_arn": manifest_acl,
      "coordinate_transform": ctransform,
      "fullscans":"" 
    },
    role: lambdaRole
  });
  
  secret.grantRead(lambdaFunc)
  output_bucket.grantPut(lambdaFunc)

  var ruleName = "dailyRun-" + functionName
  const rule = new Rule(construct, ruleName, {
    schedule: Schedule.expression("cron(15 3 * * ? *)"),
      targets: [new LambdaFunction(lambdaFunc)],
      ruleName: ruleName
  });

  //cdk.Tags.of(output_bucket).add("APIFetch", sourcename)
  cdk.Tags.of(lambdaFunc).add("APIFetch", sourcename)
  cdk.Tags.of(rule).add("APIFetch", sourcename)


}
//stepfunction (optional, nice to have to loop through dates)



