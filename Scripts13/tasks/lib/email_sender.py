"""
Email Sender for Pipeline Status Reports

Sends HTML-formatted pipeline status emails via AWS SES.
The email format mirrors the existing Snowflake SEND_TASK_STATUS_EMAIL
procedure's HTML template (blue header, striped rows, red/orange highlights).
"""

import datetime
from typing import Dict, Any, List, Optional

from .utils import get_logger, format_duration

logger = get_logger(__name__)


def _build_html_email(
    pipeline_results: List[Dict[str, Any]],
    pre_counts: Dict[str, int],
    post_counts: Dict[str, int],
    total_duration: float,
    overall_status: str,
    environment: str = 'dev',
) -> str:
    """
    Build an HTML email body summarizing the pipeline run.

    Mirrors the Snowflake SEND_TASK_STATUS_EMAIL format:
    - Blue header (#0056b3)
    - Blue table headers (#0073e6, white text)
    - Striped rows (#f2f2f2 on even)
    - Red (#ff6666) for failed actions
    - Orange (#ffcc66) for warnings
    """
    now = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')
    status_color = '#28a745' if overall_status == 'completed' else '#dc3545'

    # Build action rows
    action_rows = ''
    for r in pipeline_results:
        action = r.get('action', 'unknown')
        status = r.get('status', 'unknown')
        duration_s = r.get('duration_seconds', 0)
        duration_str = format_duration(duration_s)

        row_class = ''
        if status in ('failed', 'error'):
            row_class = ' class="highlight-failure"'
        elif status == 'partial':
            row_class = ' class="highlight-abnormal"'

        action_rows += (
            f'        <tr{row_class}>\n'
            f'          <td>{action}</td>\n'
            f'          <td>{status}</td>\n'
            f'          <td>{duration_str}</td>\n'
            f'        </tr>\n'
        )

    # Build row count comparison rows
    count_rows = ''
    all_tables = sorted(set(list(pre_counts.keys()) + list(post_counts.keys())))
    for table in all_tables:
        pre = pre_counts.get(table, -1)
        post = post_counts.get(table, -1)
        pre_str = f'{pre:,}' if pre >= 0 else 'N/A'
        post_str = f'{post:,}' if post >= 0 else 'N/A'
        if pre >= 0 and post >= 0:
            delta = post - pre
            delta_str = f'+{delta:,}' if delta >= 0 else f'{delta:,}'
        else:
            delta_str = '-'
        count_rows += (
            f'        <tr>\n'
            f'          <td>{table}</td>\n'
            f'          <td style="text-align:right">{pre_str}</td>\n'
            f'          <td style="text-align:right">{post_str}</td>\n'
            f'          <td style="text-align:right">{delta_str}</td>\n'
            f'        </tr>\n'
        )

    html = f"""\
<html>
<head>
    <style>
        body {{
            font-family: Arial, sans-serif;
            color: #333;
            line-height: 1.6;
            background-color: #f4f4f9;
            margin: 0;
            padding: 20px;
        }}
        h1 {{
            color: #0056b3;
            text-align: center;
            margin-bottom: 20px;
        }}
        h2 {{
            color: #0056b3;
            margin-top: 30px;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin-top: 10px;
        }}
        th, td {{
            padding: 10px 12px;
            text-align: left;
            border: 1px solid #ddd;
        }}
        th {{
            background-color: #0073e6;
            color: white;
        }}
        tr:nth-child(even) {{
            background-color: #f2f2f2;
        }}
        .highlight-failure {{
            background-color: #ff6666;
        }}
        .highlight-abnormal {{
            background-color: #ffcc66;
        }}
        .status-badge {{
            display: inline-block;
            padding: 4px 12px;
            border-radius: 4px;
            color: white;
            font-weight: bold;
        }}
    </style>
</head>
<body>
    <h1>Conflict Management Pipeline Status Report</h1>
    <p>
        <strong>Environment:</strong> {environment}<br>
        <strong>Completed:</strong> {now}<br>
        <strong>Total Duration:</strong> {format_duration(total_duration)}<br>
        <strong>Overall Status:</strong>
        <span class="status-badge" style="background-color:{status_color}">
            {overall_status.upper()}
        </span>
    </p>

    <h2>Action Summary</h2>
    <table>
        <thead>
            <tr>
                <th>Action</th>
                <th>Status</th>
                <th>Duration</th>
            </tr>
        </thead>
        <tbody>
{action_rows}
        </tbody>
    </table>

    <h2>Row Count Changes</h2>
    <table>
        <thead>
            <tr>
                <th>Table</th>
                <th style="text-align:right">Before</th>
                <th style="text-align:right">After</th>
                <th style="text-align:right">Delta</th>
            </tr>
        </thead>
        <tbody>
{count_rows}
        </tbody>
    </table>

    <p style="margin-top:30px; color:#666; font-size:12px;">
        This is an automated report from the Conflict Management Pipeline (ECS).
    </p>
</body>
</html>"""
    return html


def send_pipeline_email(
    email_config: Dict[str, Any],
    pipeline_results: List[Dict[str, Any]],
    pre_counts: Dict[str, int],
    post_counts: Dict[str, int],
    total_duration: float,
    overall_status: str,
    environment: str = 'dev',
) -> bool:
    """
    Send a pipeline status email via AWS SES.

    Args:
        email_config: Email configuration from config.json
        pipeline_results: List of action result dicts (each has 'action', 'status', 'duration_seconds')
        pre_counts: Pre-run table row counts
        post_counts: Post-run table row counts
        total_duration: Total pipeline duration in seconds
        overall_status: Overall pipeline status string
        environment: Environment name for the subject line

    Returns:
        True if email was sent successfully, False otherwise
    """
    if not email_config.get('enabled', False):
        logger.info("  Email notifications disabled in config")
        return False

    sender = email_config.get('sender', '')
    recipients_str = email_config.get('recipients', '')
    region = email_config.get('region', 'us-east-1')
    subject_prefix = email_config.get('subject_prefix', 'Conflict Management Pipeline')

    if not sender or not recipients_str:
        logger.warning("  Email sender or recipients not configured -- skipping email")
        return False

    recipients = [r.strip() for r in recipients_str.split(',') if r.strip()]
    if not recipients:
        logger.warning("  No valid email recipients -- skipping email")
        return False

    subject = f"{subject_prefix} - {overall_status.upper()} - {environment}"

    html_body = _build_html_email(
        pipeline_results=pipeline_results,
        pre_counts=pre_counts,
        post_counts=post_counts,
        total_duration=total_duration,
        overall_status=overall_status,
        environment=environment,
    )

    try:
        import boto3
        ses_client = boto3.client('ses', region_name=region)

        response = ses_client.send_email(
            Source=sender,
            Destination={'ToAddresses': recipients},
            Message={
                'Subject': {'Data': subject, 'Charset': 'UTF-8'},
                'Body': {
                    'Html': {'Data': html_body, 'Charset': 'UTF-8'},
                },
            },
        )

        message_id = response.get('MessageId', 'unknown')
        logger.info(f"  Email sent successfully (MessageId: {message_id})")
        logger.info(f"    To: {', '.join(recipients)}")
        logger.info(f"    Subject: {subject}")
        return True

    except ImportError:
        logger.warning("  boto3 not installed -- cannot send email via SES")
        return False
    except Exception as e:
        logger.error(f"  Failed to send email via SES: {e}")
        return False
