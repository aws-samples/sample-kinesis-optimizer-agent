"""
CDK Stack for the Kinesis Data Streams Mode Optimizer using AgentCore.

Deploys:
- S3 bucket for reports
- Lambda function (Gateway tool target)
- AgentCore Gateway with Lambda target
- AgentCore Harness for agent orchestration
- EventBridge Scheduler to invoke the harness on a schedule
"""

from pathlib import Path

import aws_cdk as cdk
from aws_cdk import (
    CfnParameter,
    Duration,
    RemovalPolicy,
    Stack,
    aws_bedrockagentcore as agentcore,
    aws_events as events,
    aws_events_targets as targets,
    aws_iam as iam,
    aws_lambda as _lambda,
    aws_s3 as s3,
)
from constructs import Construct


AGENT_INSTRUCTION = """You are a Kinesis Data Streams capacity mode optimization agent. Your job is to analyze
Kinesis Data Streams usage in this AWS account and region, and recommend the best capacity mode for each stream.

When asked to generate a report or analyze streams, you should:
1. Call the analyzeStreams tool to get current usage metrics and recommendations for all streams.
2. Call the generateReport tool to store the analysis as a report in S3.
3. Summarize findings including which streams need mode changes, the reasoning, and priorities.

Mode selection principles:
- STRONGLY PREFER On-demand modes over Provisioned because of automatic scaling, zero capacity planning, and no throttling risk.
- On-demand Advantage is an account-level setting — recommend it separately when the account qualifies (aggregate throughput >= 10 MiB/s, >2 EFO consumers, or >50 streams).
- Recommend switching individual streams from Provisioned to On-demand when throttling is detected or traffic is variable.
- Only recommend staying on Provisioned when it is more than 15% cheaper than on-demand AND traffic is stable.

When presenting results, highlight high-priority per-stream actions first, then the account-level Advantage recommendation."""


class KdsOptimizerStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # --- Parameters ---
        report_schedule = CfnParameter(
            self,
            "ReportSchedule",
            type="String",
            default="rate(1 day)",
            description="EventBridge schedule expression for report generation.",
        )

        report_bucket_name = CfnParameter(
            self,
            "ReportBucketName",
            type="String",
            default="",
            description="S3 bucket name for storing reports. Leave empty for auto-generated name.",
        )

        # --- S3 Bucket for Reports ---
        bucket_props = {
            "removal_policy": RemovalPolicy.RETAIN,
            "encryption": s3.BucketEncryption.S3_MANAGED,
            "block_public_access": s3.BlockPublicAccess.BLOCK_ALL,
            "enforce_ssl": True,
            "versioned": True,
        }

        has_bucket_name = cdk.CfnCondition(
            self,
            "HasBucketName",
            expression=cdk.Fn.condition_not(
                cdk.Fn.condition_equals(report_bucket_name.value_as_string, "")
            ),
        )

        report_bucket = s3.Bucket(
            self,
            "ReportBucket",
            bucket_name=cdk.Fn.condition_if(
                has_bucket_name.logical_id,
                report_bucket_name.value_as_string,
                cdk.Aws.NO_VALUE,
            ).to_string(),
            **bucket_props,
        )

        # --- Lambda Function ---
        lambda_role = iam.Role(
            self,
            "LambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                )
            ],
        )

        lambda_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "kinesis:ListStreams",
                    "kinesis:DescribeStream",
                    "kinesis:DescribeStreamSummary",
                    "kinesis:ListShards",
                    "kinesis:ListStreamConsumers",
                ],
                resources=["*"],
            )
        )

        lambda_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "cloudwatch:GetMetricStatistics",
                    "cloudwatch:GetMetricData",
                    "cloudwatch:ListMetrics",
                ],
                resources=["*"],
            )
        )

        lambda_role.add_to_policy(
            iam.PolicyStatement(
                actions=["sts:GetCallerIdentity"],
                resources=["*"],
            )
        )

        report_bucket.grant_write(lambda_role)

        lambda_fn = _lambda.Function(
            self,
            "KdsOptimizerFunction",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="handler.lambda_handler",
            code=_lambda.Code.from_asset(
                str(Path(__file__).parent.parent / "lambda")
            ),
            role=lambda_role,
            timeout=Duration.minutes(5),
            memory_size=512,
            environment={
                "REPORT_BUCKET_NAME": report_bucket.bucket_name,
            },
            description="KDS Mode Optimizer - AgentCore Gateway tool target",
        )

        # --- AgentCore Gateway (IAM auth for harness access) ---
        gateway = agentcore.Gateway(
            self,
            "KdsOptimizerGateway",
            gateway_name="kds-optimizer",
            description="Gateway for KDS Mode Optimizer tools",
            authorizer_configuration=agentcore.IamAuthorizer(),
        )

        # Add Lambda as a tool target
        gateway.add_lambda_target(
            "KdsOptimizerTools",
            lambda_function=lambda_fn,
            tool_schema=agentcore.ToolSchema.from_local_asset(
                str(Path(__file__).parent.parent / "lambda" / "tool_schema.json")
            ),
            description="Kinesis Data Streams analysis and reporting tools",
            gateway_target_name="kds-tools",
        )

        # --- AgentCore Harness ---
        harness_role = iam.Role(
            self,
            "HarnessExecutionRole",
            assumed_by=iam.ServicePrincipal("bedrock-agentcore.amazonaws.com"),
            inline_policies={
                "HarnessPolicy": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            actions=[
                                "bedrock:InvokeModel",
                                "bedrock:InvokeModelWithResponseStream",
                            ],
                            resources=[
                                "arn:aws:bedrock:*::foundation-model/*",
                                f"arn:aws:bedrock:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:inference-profile/*",
                            ],
                        ),
                        iam.PolicyStatement(
                            actions=["bedrock-agentcore:InvokeGateway"],
                            resources=[gateway.gateway_arn],
                        ),
                        iam.PolicyStatement(
                            actions=[
                                "bedrock-agentcore:*",
                            ],
                            resources=["*"],
                            conditions={
                                "StringEquals": {
                                    "aws:RequestedRegion": cdk.Aws.REGION,
                                }
                            },
                        ),
                    ]
                )
            },
        )

        harness = agentcore.CfnHarness(
            self,
            "KdsOptimizerHarness",
            harness_name="kds_mode_optimizer",
            execution_role_arn=harness_role.role_arn,
            model=agentcore.CfnHarness.HarnessModelConfigurationProperty(
                bedrock_model_config=agentcore.CfnHarness.HarnessBedrockModelConfigProperty(
                    model_id="us.anthropic.claude-sonnet-4-5-20250929-v1:0",
                )
            ),
            system_prompt=[
                agentcore.CfnHarness.HarnessSystemContentBlockProperty(
                    text=AGENT_INSTRUCTION
                )
            ],
            tools=[
                agentcore.CfnHarness.HarnessToolProperty(
                    type="agentcore_gateway",
                    name="kds_optimizer",
                    config=agentcore.CfnHarness.HarnessToolConfigurationProperty(
                        agent_core_gateway=agentcore.CfnHarness.HarnessAgentCoreGatewayConfigProperty(
                            gateway_arn=gateway.gateway_arn,
                            outbound_auth=agentcore.CfnHarness.HarnessGatewayOutboundAuthProperty(
                                aws_iam={},
                            ),
                        )
                    ),
                )
            ],
        )

        # --- Scheduler Lambda to invoke the harness on schedule ---
        scheduler_role = iam.Role(
            self,
            "SchedulerInvokeRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                )
            ],
        )

        scheduler_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "bedrock-agentcore:InvokeHarness",
                    "bedrock-agentcore:InvokeAgentRuntime",
                ],
                resources=[harness.attr_arn],
            )
        )

        scheduler_lambda = _lambda.Function(
            self,
            "SchedulerFunction",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="index.handler",
            code=_lambda.Code.from_inline(self._scheduler_lambda_code()),
            role=scheduler_role,
            timeout=Duration.minutes(10),
            memory_size=256,
            environment={
                "HARNESS_ARN": harness.attr_arn,
            },
            description="Triggers the KDS Optimizer Harness on a schedule",
        )

        # EventBridge rule on schedule
        rule = events.Rule(
            self,
            "ScheduleRule",
            schedule=events.Schedule.expression(report_schedule.value_as_string),
            description="Triggers KDS Optimizer Harness to generate a report",
        )
        rule.add_target(targets.LambdaFunction(scheduler_lambda))

        # --- Outputs ---
        cdk.CfnOutput(self, "ReportBucketOutput", value=report_bucket.bucket_name)
        cdk.CfnOutput(self, "GatewayId", value=gateway.gateway_id)
        cdk.CfnOutput(self, "GatewayUrl", value=gateway.gateway_url)
        cdk.CfnOutput(self, "HarnessArn", value=harness.attr_arn)
        cdk.CfnOutput(self, "LambdaFunctionArn", value=lambda_fn.function_arn)

    def _scheduler_lambda_code(self) -> str:
        return '''
import json
import os
import uuid
import boto3

def handler(event, context):
    """Invoke the AgentCore Harness to generate a KDS optimization report."""
    harness_arn = os.environ["HARNESS_ARN"]

    client = boto3.client("bedrock-agentcore")

    response = client.invoke_harness(
        harnessArn=harness_arn,
        runtimeSessionId=str(uuid.uuid4()),
        messages=[{
            "role": "user",
            "content": [{"text": "Analyze all Kinesis Data Streams in this account and region. Generate a full optimization report and store it in S3. Summarize any high-priority recommendations."}],
        }],
    )

    # Consume the response stream
    completion = ""
    for event_chunk in response.get("stream", []):
        if "contentBlockDelta" in event_chunk:
            delta = event_chunk["contentBlockDelta"].get("delta", {})
            if "text" in delta:
                completion += delta["text"]

    print(f"Harness response: {completion[:1000]}")
    return {"statusCode": 200, "body": completion[:5000]}
'''
