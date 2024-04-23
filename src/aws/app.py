#!/usr/bin/env python3

import aws_cdk as cdk

from aws.stacks.ETL.etl_stack import ETLStack

app = cdk.App()
ETLStack(app, "ETLStack", 
         env=cdk.Environment(account='905418180182', region='us-east-1'))

app.synth()
