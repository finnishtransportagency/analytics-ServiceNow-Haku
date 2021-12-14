import * as cdk from '@aws-cdk/core';
import * as iam from '@aws-cdk/aws-iam';
import { CfnOutput, Construct, DockerImage, RemovalPolicy, StackProps, Stage, Tag } from '@aws-cdk/core';
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

	// stackName == "ServiceNowdevdepl-hgquSPWmczRQ"
    const secretmanagerForSecrets = new secretsmanager.Secret(this, "APISecrets" + this.stackName,
      { //DO NOT change this object, it will create new blank secretmanager 
        generateSecretString: {
          secretStringTemplate: '{"username": "api username", "url": "api url"}',
		      generateStringKey: "password",
        },
      },
    );

    secretmanagerForSecrets.applyRemovalPolicy(RemovalPolicy.RETAIN)
    //remember to add username,password,url hints to secretmanager so lambda can fetch them
    
    const landingBucket = new s3.Bucket(this, 'data' + this.stackName, {
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });
    var env = this.stackName.split("-").slice(-1)[0]
    var acl = this.node.tryGetContext('ADE'+env+'ACL')
    /*
    Replicate this for more lambda+bucket+crontrigger tasks sets and set secrets to secrets manager
    */
	
	  // Lambdan nimeksi tulee "ServiceNowdev-deploy-Serv-APIFetchnowtabletaskServ-s4WUIIwyHRxU"
	  // => Selkeä taulun/l�hteen nimi puuttuu.
	  // Nimeksi pitäisi riittää:
	  // "ServiceNow-" + <dev|prod> + "-APIFetch-" + <sourceName> + "-" + <generated id>
	  // Sourcename pitää olla lähdettä kuvaava ja pitäisi olla vain yksi / lähde.
	  // Ei tosin haittaa vaikka perään laittaa generoidun tunnisteen
    datapipeServiceNowTable(
      this,						// construct
      "u_case",		// APIName ==>> /_ merkit näyttää häviävän nimestä
      this.stackName,			// stackname = appName-environmentName
      secretmanagerForSecrets,	// secretmanager for storing secrets
      this.region,				// region that is beign used      
      lambdaRole,				// role that allows cross region bucket put
      "com.cgi.lambda.apifetch.LambdaFunctionHandler", //handler used in code 
      "mvn clean install && cp ./target/servicenow-to-s3-lambda-1.0.0.jar /asset-output/", //buildcommand
      "",		// Fill in query_string_default query string used to get data from API
      "",		// Fill in query_string_date date modifier if we want exact date
      "servicenow2_u_case",		// Fill in output_path
      "servicenow2_u_case",		// Fill in output_filename 
      "file-load-ade-runtime-" + env,		// Fill in manifestbucket_name
      acl,		// ACL value for xaccount bucket write
      "manifest/servicenow2_u_case",		// Fill in manifest_path,
      "true",	// coordinatetransformtoWgs84      
    )
/*
    datapipeServiceNowTable(
      this,						// construct
      "services",		// APIName ==>> / merkit näyttää häviävän nimestä
      this.stackName,			// stackname = appName-environmentName
      secretmanagerForSecrets,	// secretmanager for storing secrets
      this.region,				// region that is beign used      
      lambdaRole,				// role that allows cross region bucket put
      "com.cgi.lambda.apifetch.LambdaFunctionHandler", //handler used in code 
      "mvn clean install && cp ./target/servicenow-to-s3-lambda-1.0.0.jar /asset-output/", //buildcommand
      "",		// Fill in query_string_default query string used to get data from API
      "",		// Fill in query_string_date date modifier if we want exact date
      "servicenow2_services",		// Fill in output_path
      "servicenow2_services",		// Fill in output_filename 
      "",		// Fill in manifestbucket_name
      acl,		// ACL value for xaccount bucket write
      "",		// Fill in manifest_path,
      "true",	// coordinatetransformtoWgs84      
    )
*/

  }
}

function datapipeServiceNowTable(
  construct: cdk.Construct,
  APIName: string,
  appnameAndEnv: string,
  secretmanager: secretsmanager.Secret,
  region: string,
  lambdaRole: iam.IRole,
  handler: string,
  buildcommand: string,
  query_string_default:string,
  query_string_date:string,
  output_path:string,
  output_filename:string,
  manifestbucket_name:string,
  aclValue:string,
  manifest_path:string,
  ctransform:string) {

/*
  const resourcenaming = "-" + APIName + "-" + appnameAndEnv
  const databucket = new s3.Bucket(construct, 'DataBucket' + resourcenaming, {
    removalPolicy: cdk.RemovalPolicy.DESTROY,
  });
*/
/*  
  const apiLambda = new lambda.Function(construct, 'APIFetch' + resourcenaming, {
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
    handler: handler,
    runtime: lambda.Runtime.JAVA_8,
    environment: {
      "secret_arn": secretmanager.secretArn,
      "region": region,
      "query_string_default": query_string_default,
      "query_string_date": query_string_date,
      "output_split_limit": "1500",
      "api_limit": "1000",
      "output_bucket": databucket.bucketName,
      "output_path": output_path,
      "output_filename": output_filename,
      "manifest_bucket": manifestbucket_name,
      "manifest_path":manifest_path,
      "manifest_arn":aclValue,
      "coordinate_transform": ctransform,
      "fullscans":"" 
    },
    role: lambdaRole
  });
  
  // Kaiva jostain toimiiko
  // console.log("lambda name" + apiLambda.getName());

  console.log("function name = '" + apiLambda.functionName + "'")
  
  secretmanager.grantRead(apiLambda)
  databucket.grantPut(apiLambda)

  const rule = new Rule(construct, "dailyRun" + resourcenaming, {
    schedule: Schedule.expression("cron(15 3 * * ? *)"),
      targets: [new LambdaFunction(apiLambda)], 
  });
  cdk.Tags.of(databucket).add("APIFetch", APIName)
  cdk.Tags.of(apiLambda).add("APIFetch",APIName)
  cdk.Tags.of(rule).add("APIFetch", APIName)
*/

}
//stepfunction (optional, nice to have to loop through dates)



