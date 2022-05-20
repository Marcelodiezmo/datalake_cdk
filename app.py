from importlib.resources import path
from msilib.schema import Environment
from aws_cdk import (
    aws_events as events,
    aws_lambda as lambda_,
    aws_ec2 as ec2,
    aws_iam as iam_,
    aws_events_targets as targets,
    App, Duration, Stack
)
from os import path
import os
import json


class DatalakeIntegrationsStack(Stack):
    def __init__(self, app: App, id: str, **kwargs) -> None:
        super().__init__(app, id)

        env = str(os.environ['CDK_DEFAULT_ACCOUNT'])
        if env == '824404647578':  # DEV
            # MODIFICAR ARCHIVO PARAMETRICO

            # Layers
            basic_emr_layer = lambda_.LayerVersion.from_layer_version_arn(
                self,
                'basic_emr_layer',
                layer_version_arn='arn:aws:lambda:us-east-1:824404647578:layer:shared-resources-lib:24')

            # Rol
            basic_emr_role = iam_.Role.from_role_arn(
                self,
                'basic_emr_role',
                role_arn='arn:aws:iam::824404647578:role/service-role/emrshoot-role-ta32ua2r')


            # Variable
            env_variable = {
                "branch": "develop",
                "bucket_log": "analitycs-s3-dev",
                "bucket_name": "analytics-ubits-production-dev",
                "prefix": "dev",
                "keys":"ubits-dev-key_v2",
                "subnet":"subnet-0c8143c08ed2a0184"}


            lambdaFn = lambda_.Function(
                self, "DatalakeIntegrationsLambda",
                environment=env_variable,
                code=lambda_.Code.from_asset("./files/"),
                handler="lambda-handler.main",
                role=basic_emr_role,
                timeout=Duration.seconds(300),
                runtime=lambda_.Runtime.PYTHON_3_8,
                layers=[basic_emr_layer],
            )

            rule = events.Rule(
                self, "Rule",
                schedule=events.Schedule.cron(
                    minute='00',
                    hour='13',
                    month='*',
                    week_day='MON-FRI',
                    year='*'),
            )

            #rule.add_target(targets.LambdaFunction(lambdaFn))


        elif env == '986361039434':  # PROD


            env_variable = {
                "branch": "develop",
                "bucket_log": "analitycs-s3",
                "bucket_name": "analytics-ubits-production",
                "prefix": "prod",
                "keys":"emr-pair",
                "subnet":"subnet-0db407dc07055d853"}

            # Layers
            basic_emr_layer = lambda_.LayerVersion.from_layer_version_arn(
                self,
                'basic_emr_layer',
                layer_version_arn='arn:aws:lambda:us-east-1:986361039434:layer:csb-sam-app-dependencies:24')


            # Rol
            basic_emr_role = iam_.Role.from_role_arn(
                self,
                'basic_emr_role',
                role_arn='arn:aws:iam::986361039434:role/emrlambda0')

            # # Create lambda
            lambdaFn = lambda_.Function(
                self, "DatalakeIntegrationsLambda",
                environment=env_variable,
                code=lambda_.Code.from_asset("./files/"),
                handler="lambda-handler.main",
                role=basic_emr_role,
                timeout=Duration.seconds(300),
                runtime=lambda_.Runtime.PYTHON_3_8,
                layers=[basic_emr_layer],
            )

            rule = events.Rule(
                self, "Rule",
                schedule=events.Schedule.cron(
                    minute='00',
                    hour='13',
                    month='*',
                    week_day='MON-FRI',
                    year='*'),
            )

            rule.add_target(targets.LambdaFunction(lambdaFn))


app = App()
DatalakeIntegrationsStack(app, "DatalakeIntegrations", env={
    'account': os.environ['CDK_DEFAULT_ACCOUNT'],
    'region': os.environ['CDK_DEFAULT_REGION']
})


app.synth()

