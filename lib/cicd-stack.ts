import * as cdk from '@aws-cdk/core';
import * as ec2 from '@aws-cdk/aws-ec2';

import { Construct, Stage, Stack, StageProps, SecretValue } from '@aws-cdk/core';
import { CodePipeline, CodePipelineSource, ShellStep,ManualApprovalStep } from "@aws-cdk/pipelines";
import * as ca from '@aws-cdk/aws-codepipeline-actions'
import * as codepipeline from '@aws-cdk/aws-codepipeline';
import s3deploy = require('@aws-cdk/aws-s3-deployment');
import * as ssm from '@aws-cdk/aws-ssm';
import * as codecommit from '@aws-cdk/aws-codecommit';
import { GitHubSourceAction, ManualApprovalAction } from '@aws-cdk/aws-codepipeline-actions';
import s3 = require('@aws-cdk/aws-s3');
import * as secretsmanager from '@aws-cdk/aws-secretsmanager';
import { ServerlessServiceStack } from './serverless-service-stack';
import { countReset, timeStamp } from 'console';

const sourceOutput = new codepipeline.Artifact();

interface stageprops extends StageProps {
  appname:string
}
class ApplicationStageDev extends Stage {
  constructor(scope: Construct, id: string, props: stageprops) {
    super(scope, id, props);
    new ServerlessServiceStack(this, props.appname+'-service-'+"dev",{ env:props.env
    });
  }
}

class ApplicationStageProd extends Stage {
  constructor(scope: Construct, id: string, props: stageprops) {
    super(scope, id, props);
    new ServerlessServiceStack(this, props.appname+'-service-'+"prod",{ env:props.env
    });
  }
}

interface ServiceStackProps extends cdk.StackProps {

}
export class CICDStack extends Stack {
  constructor(scope: Construct, id: string, props: ServiceStackProps) {
    super(scope, id, props);
    const appname = this.node.tryGetContext('appname')  
    const sourceArtifact = new codepipeline.Artifact();
    const cloudAssemblyArtifact = new codepipeline.Artifact();
    
    new ssm.StringParameter(this, 'ServiceNow-GitConnectionParameter', {
      description: 'ARN of the github connection that connects pipelineToRepository after manually setting it up',
      parameterName: '/servicenow/gitconnectionARN',
      stringValue: 'InsertMeAfterInitPhase',
      tier: ssm.ParameterTier.STANDARD,
    });

/*
Old method of getting access to github, should be replaced with ServiceNow-GitConnectionParameter... read todo from code for reason
*/

    const secretToken = new secretsmanager.Secret(this, appname + '-pipelineSecrets',
    {
      generateSecretString: {
        secretStringTemplate: '{"gittoken": "token"}', 
        generateStringKey: 'password',
      },
    },
  );

  if (this.node.tryGetContext('phase') != "init") {

    var branch = "master"

    //TODO input in pipeline should be replaced with this. Current method is based on personal token which means that when person looses access to Väylä Git, Pipeline stops working
    // New method requires connection be crated with organization admin priviliges
    /* 
          input: CodePipelineSource.connection(this.node.tryGetContext('ownerandapp'), 'master', {
            connectionArn: ssm.StringParameter.fromStringParameterAttributes(this, appname+'-RetrivedGitConnectionParameter', {
              parameterName: '/'+appname+'/gitARN',
            }).stringValue, // Created using the AWS console in init phase * });',

    */
   

        const pipeline = new CodePipeline(this, appname+'-Pipeline', {
          dockerEnabledForSelfMutation:true,
          crossAccountKeys:true,
          pipelineName: appname+'-Pipeline',
          synth:  new ShellStep('Synth', {
          input: CodePipelineSource.gitHub("oappicgi/testing", 'master', {
            authentication: SecretValue.secretsManager(secretToken.secretArn,{jsonField:"gittoken2"})
            }),          
          commands: ['npm ci', 'npm run build', 'npx cdk synth'],            
          }), 
        });




    /*
repo - to read the repository
admin:repo_hook - if you plan to use webhooks (true by default)
*/
    


   const deployStage = pipeline.addStage( new ApplicationStageDev(this, appname+"dev-deploy", {
      env: { account: this.account, region: this.region }, 
      appname:appname
    },
    )); 
    const proddeployStage = pipeline.addStage(new ApplicationStageProd(this, appname+"prod-deploy", {
      env: { account: this.account, region: this.region },
      appname:appname    
    }));
    proddeployStage.addPre(new ManualApprovalStep('Pre-production check'))    
  }  
  }
  }

