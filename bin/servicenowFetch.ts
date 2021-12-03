#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from '@aws-cdk/core';
import { CICDStack } from '../lib/cicd-stack';




const app = new cdk.App();
new CICDStack(app, 'ranalytics-cicd', {  
     env: { 
     account: app.node.tryGetContext('devaccountid'), 
     region: app.node.tryGetContext('region')
     }}
);
app.synth();