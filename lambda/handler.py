"""
AWS Lambda handler for the AgentCore Gateway target.

This Lambda is invoked by AgentCore Gateway when a tool is called.
The event contains the tool's input parameters as a flat dict.
The tool name is available in context.client_context.custom.
"""

import json
import logging
import os

from kinesis_analyzer import KinesisAnalyzer
from report_generator import ReportGenerator

logger = logging.getLogger()
logger.setLevel(logging.INFO)

REPORT_BUCKET = os.environ.get("REPORT_BUCKET_NAME", "")

# Delimiter used by AgentCore Gateway to prefix tool names with target name
TOOL_NAME_DELIMITER = "___"


def lambda_handler(event, context):
    """
    AgentCore Gateway Lambda handler.

    The Gateway passes tool input as the event (flat dict of properties).
    The tool name is in context.client_context.custom['bedrockAgentCoreToolName'].
    """
    logger.info(f"Received event: {json.dumps(event)}")

    # Extract tool name from context
    tool_name = _get_tool_name(context)
    logger.info(f"Tool name: {tool_name}")

    try:
        if tool_name == "analyzeStreams":
            result = handle_analyze_streams(event)
        elif tool_name == "generateReport":
            result = handle_generate_report(event)
        elif tool_name == "getStreamDetails":
            result = handle_get_stream_details(event)
        else:
            result = {"error": f"Unknown tool: {tool_name}"}

        return json.loads(json.dumps(result, default=str))

    except Exception as e:
        logger.error(f"Error handling request: {e}", exc_info=True)
        return {"error": str(e)}


def _get_tool_name(context) -> str:
    """Extract the tool name from the AgentCore Gateway context."""
    try:
        original_name = context.client_context.custom.get("bedrockAgentCoreToolName", "")
        # Strip the target name prefix (format: targetName___toolName)
        if TOOL_NAME_DELIMITER in original_name:
            return original_name[original_name.index(TOOL_NAME_DELIMITER) + len(TOOL_NAME_DELIMITER):]
        return original_name
    except (AttributeError, TypeError):
        # Fallback: check if tool_name is passed directly in event (for local testing)
        return ""


def handle_analyze_streams(params: dict) -> dict:
    """Analyze all streams and return recommendations without storing a report."""
    analyzer = KinesisAnalyzer()
    results = analyzer.analyze_all_streams()
    return results


def handle_generate_report(params: dict) -> dict:
    """Analyze all streams and store the report in S3."""
    bucket = params.get("bucket_name", REPORT_BUCKET)
    if not bucket:
        return {"error": "No S3 bucket specified. Set REPORT_BUCKET_NAME env var or pass bucket_name parameter."}

    analyzer = KinesisAnalyzer()
    analysis_results = analyzer.analyze_all_streams()

    generator = ReportGenerator(bucket_name=bucket)
    report_summary = generator.generate_and_store(analysis_results)
    return report_summary


def handle_get_stream_details(params: dict) -> dict:
    """Get detailed metrics and recommendation for a specific stream."""
    stream_name = params.get("stream_name")
    if not stream_name:
        return {"error": "stream_name parameter is required."}

    analyzer = KinesisAnalyzer()

    try:
        stream_desc = analyzer.describe_stream(stream_name)
        metrics = analyzer.get_stream_metrics(stream_name)

        # Get EFO consumers
        stream_arn = stream_desc.get("StreamARN", "")
        consumers = analyzer.list_stream_consumers(stream_arn) if stream_arn else []
        efo_consumer_count = len(consumers)

        usage_summary = analyzer.compute_usage_summary(
            stream_name, metrics, stream_desc, efo_consumer_count=efo_consumer_count
        )

        current_mode = stream_desc.get("StreamModeDetails", {}).get(
            "StreamMode", "PROVISIONED"
        )

        rec = analyzer.recommend_mode(usage_summary, current_mode)

        return {
            "stream_name": stream_name,
            "current_mode": current_mode,
            "current_shards": stream_desc.get("OpenShardCount", 0),
            "stream_arn": stream_arn,
            "retention_period_hours": stream_desc.get("RetentionPeriodHours", 24),
            "efo_consumers": consumers,
            "efo_consumer_count": efo_consumer_count,
            "metrics_summary": usage_summary,
            "recommendation": rec["recommendation"],
            "reasoning": rec["reasoning"],
            "priority": rec["priority"],
        }
    except Exception as e:
        return {"error": f"Failed to analyze stream '{stream_name}': {str(e)}"}
