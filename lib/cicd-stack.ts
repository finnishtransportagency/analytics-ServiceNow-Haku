import * as cdk from '@aws-cdk/core';
import { Construct, Stage, Stack, StageProps} from '@aws-cdk/core';
import { CodePipeline, CodePipelineSource, ShellStep,ManualApprovalStep } from "@aws-cdk/pipelines";
import * as codepipeline from '@aws-cdk/aws-codepipeline';
import * as ssm from '@aws-cdk/aws-ssm';
import { ServerlessServiceStack } from './serverless-service-stack';

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

 if (this.node.tryGetContext('phase') != "init") {

    var branch = "master"

        const pipeline = new CodePipeline(this, appname+'-Pipeline', {
          dockerEnabledForSelfMutation:true,
          crossAccountKeys:true,
          pipelineName: appname+'-Pipeline',
          synth:  new ShellStep('Synth', {
          input: CodePipelineSource.connection(this.node.tryGetContext('ownerandapp'), branch, {
            connectionArn:  `arn:aws:codestar-connections:${cdk.Stack.of(this).region}:${cdk.Stack.of(this).account}:connection/${this.node.tryGetContext('connectionID')}`}),          
          commands: ['npm ci', 'npm run build', 'npx cdk synth'],            
          }), 
        });
  
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

