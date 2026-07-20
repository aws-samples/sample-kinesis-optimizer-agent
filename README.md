# Kinesis Data Streams Mode Optimizer Agent

An AI-powered agent running on Amazon Bedrock AgentCore that analyzes your Kinesis Data Streams usage and recommends the optimal capacity mode: **On-demand Standard**, **On-demand Advantage**, or **Provisioned**.

The agent strongly prefers on-demand modes for their operational simplicity (automatic scaling, no capacity planning, no throttling risk) and only recommends provisioned when it's demonstrably cheaper (>15%).

## How It Works

See [HOW_IT_WORKS.md](HOW_IT_WORKS.md) for a detailed step-by-step explanation of the analysis and decision logic.

**Short version:** The agent collects 7 days of CloudWatch metrics for every Kinesis stream in the region, discovers EFO consumers, computes throughput patterns and cost comparisons across all three pricing modes, then recommends per-stream actions (provisioned vs on-demand) and an account-level On-demand Advantage assessment. Results are stored as both JSON and a visual HTML report in S3.

## Architecture

```
EventBridge Schedule (daily/weekly/custom)
    │
    ▼
Scheduler Lambda ──► AgentCore Harness (orchestration loop)
                          │
                          ▼
                     AgentCore Gateway (MCP tools)
                          │
                          ▼
                     Tool Lambda
                          │
                ┌─────────┼─────────┐
                ▼         ▼         ▼
           Kinesis    CloudWatch    S3
           (list)     (metrics)   (report)
```

**Key components:**
- **AgentCore Harness** — Managed orchestration loop that handles model inference, tool routing, and multi-turn conversations
- **AgentCore Gateway** — Exposes the Lambda as MCP-compatible tools with IAM auth
- **Tool Lambda** — Business logic for stream analysis, metrics collection, and report generation

## Region Behavior

**The agent is region-specific.** When deployed to a region, it automatically analyzes only the Kinesis Data Streams in that region. The Lambda picks up the region from its runtime environment (`AWS_REGION`) — no hardcoded region configuration needed.

**To cover multiple regions**, set the region environment variable and deploy separately for each:

**PowerShell:**
```powershell
$env:AWS_DEFAULT_REGION="us-east-1"
cdk bootstrap aws://<ACCOUNT_ID>/us-east-1
cdk deploy

$env:AWS_DEFAULT_REGION="eu-west-1"
cdk bootstrap aws://<ACCOUNT_ID>/eu-west-1
cdk deploy
```

Each deployment is independent — its own harness, its own S3 bucket, its own schedule.

## Prerequisites

- **AWS CDK v2 (2.1132+)** — `npm install -g aws-cdk`
- **Python 3.12+**
- **aws-cdk-lib >= 2.251.0** — for AgentCore L2 constructs
- **AWS CLI** configured with credentials that have admin access
- **Bedrock model access** — ensure you have access to Claude Sonnet 4.5 in the target region

## Step-by-Step Deployment

### 1. Clone the repository

```bash
git clone <repo-url>
cd sample-kds-optimizer-agent
```

### 2. Install CDK dependencies

```bash
cd infra
pip install -r requirements.txt
```

### 3. Set your target region

**PowerShell:**
```powershell
$env:AWS_DEFAULT_REGION="us-east-1"
```

**Linux/macOS:**
```bash
export AWS_DEFAULT_REGION=us-east-1
```

### 4. Bootstrap CDK (first time per account/region)

```bash
cdk bootstrap aws://<ACCOUNT_ID>/<REGION>
```

### 5. Deploy

```bash
cdk deploy
```

With custom schedule (default is daily):
```bash
cdk deploy --parameters ReportSchedule="rate(7 days)"
```

With a specific bucket name:
```bash
cdk deploy --parameters ReportBucketName=my-kinesis-reports
```

### 6. Verify deployment

The deploy outputs will show:
- **GatewayId** — AgentCore Gateway identifier
- **GatewayUrl** — MCP endpoint URL for the gateway
- **HarnessArn** — AgentCore Harness ARN for invocation
- **ReportBucketOutput** — S3 bucket where reports land
- **LambdaFunctionArn** — The tool Lambda

### 7. Test the agent

Via Python SDK:
```python
import boto3, uuid

client = boto3.client("bedrock-agentcore", region_name="us-east-1")

response = client.invoke_harness(
    harnessArn="<HARNESS_ARN>",
    runtimeSessionId=str(uuid.uuid4()),
    messages=[{
        "role": "user",
        "content": [{"text": "Analyze all Kinesis streams and generate a report."}],
    }],
)

for event in response.get("stream", []):
    if "contentBlockDelta" in event:
        delta = event["contentBlockDelta"].get("delta", {})
        if "text" in delta:
            print(delta["text"], end="", flush=True)
```

Or via AWS CLI:
```bash
aws bedrock-agentcore invoke-harness \
  --harness-arn <HARNESS_ARN> \
  --runtime-session-id $(uuidgen) \
  --messages '[{"role":"user","content":[{"text":"Generate an optimization report"}]}]' \
  --region us-east-1
```

### 8. View reports

Reports are stored in S3 at:
```
s3://<bucket>/kinesis-optimization-reports/YYYY/MM/DD/HHMMSS-<report-id>.html
s3://<bucket>/kinesis-optimization-reports/YYYY/MM/DD/HHMMSS-<report-id>.json
```

Generate a pre-signed URL to view the HTML report in a browser:
```bash
aws s3 presign s3://<bucket>/kinesis-optimization-reports/2026/01/15/080000-abc123.html --expires-in 3600
```

## Configuration

### Schedule expressions

| Expression | Frequency |
|------------|-----------|
| `rate(1 day)` | Daily |
| `rate(7 days)` | Weekly |
| `cron(0 8 ? * MON *)` | Every Monday at 8am UTC |
| `rate(12 hours)` | Twice daily |

### Changing the model

Edit `infra/kds_optimizer_stack.py` and update the `model_id` in the `HarnessBedrockModelConfigProperty` to a model available in your target region.

## Project Structure

```
├── README.md                   # This file
├── HOW_IT_WORKS.md             # Detailed explanation of the analysis logic
├── .gitignore
├── lambda/
│   ├── handler.py              # AgentCore Gateway tool Lambda handler
│   ├── kinesis_analyzer.py     # Core analysis engine (metrics + recommendations)
│   ├── report_generator.py     # HTML + JSON report generation and S3 storage
│   ├── tool_schema.json        # MCP tool definitions for AgentCore Gateway
│   └── requirements.txt
├── infra/
│   ├── app.py                  # CDK app entry point
│   ├── cdk.json                # CDK configuration
│   ├── kds_optimizer_stack.py  # Full infrastructure stack (AgentCore)
│   └── requirements.txt
└── tests/
    ├── test_kinesis_analyzer.py # Unit tests for recommendation logic
    └── test_scale.py           # Scale test (500 streams)
```

## Decision Logic Summary

The analysis produces two types of recommendations:

### Per-Stream Recommendations (Provisioned vs On-demand)

| Condition | Recommendation |
|-----------|---------------|
| Provisioned stream with throttling (>1%) | → **On-demand** (HIGH priority) |
| Highly variable traffic (peak > 3x avg) | → **On-demand** (MEDIUM priority) |
| Provisioned is >15% cheaper AND stable traffic | → Stay **Provisioned** (LOW priority) |
| Everything else | → **On-demand** (LOW priority) |

### Account-Level Recommendation (On-demand Advantage)

On-demand Advantage is an **account-level setting per region** — not a per-stream choice. The report assesses eligibility separately based on:

| Trigger | Threshold |
|---------|-----------|
| Aggregate sustained throughput (7-day avg) | ≥ 10 MiB/s |
| EFO consumers on any stream | > 2 |
| Total streams in account/region | > 50 |

If any trigger is met, the report recommends enabling Advantage at the account level after switching provisioned streams to on-demand.

### Pricing Model

The cost analysis uses full three-way comparison (us-east-1 rates):

| Dimension | On-demand Standard | On-demand Advantage | Provisioned |
|-----------|-------------------|--------------------:|------------:|
| Data ingest | $0.08/GB | $0.032/GB | $0.015/shard-hr + $0.014/1M PUT units |
| Data retrieval | $0.04/GB | $0.016/GB | Included in shard cost |
| EFO retrieval | $0.05/GB | $0.016/GB | $0.013/GB + $0.015/consumer-shard-hr |
| Per-stream charge | $0.04/hr | None | None |
| Retention (24h-7d) | $0.10/GB-month | $0.023/GB-month | $0.02/shard-hr |
| Retention (>7d) | $0.023/GB-month | $0.023/GB-month | $0.023/GB-month |

## Running Tests

```bash
# Unit tests
python -m pytest tests/test_kinesis_analyzer.py -v

# Scale test (simulates 500 streams)
python -m pytest tests/test_scale.py -v
```

## Cleanup

To remove the stack from a region:
```bash
cd infra
cdk destroy
```

Note: The S3 bucket has `RemovalPolicy.RETAIN` — it won't be deleted with the stack. Delete it manually if needed.

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.
