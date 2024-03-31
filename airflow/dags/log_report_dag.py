from dataclasses import dataclass
from datetime import datetime, date
from itertools import chain
from json import loads, dumps

from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator


@dataclass
class LogMessage:
    timestamp: datetime
    level: str
    logger: str


@dataclass
class ProblematicServiceData:
    logger_name: str
    num_warnings: int = 0
    num_errors: int = 0


@dataclass
class HourReport:
    timestamp: datetime
    total_warnings: int
    total_errors: int
    problematic_services: list


@dataclass
class DailyReport:
    date: str
    daily_errors: int
    daily_warnings: int
    hour_reports: list


with DAG(
        dag_id="create_daily_report",
        start_date=datetime(2024, 3, 31),
        schedule="@daily",
):
    @task
    def read_file_content(bucket: str, file_name: str) -> list:
        s3_hook = S3Hook(aws_conn_id='minio')
        return list(map(extract_data, s3_hook.read_key(file_name, bucket).splitlines()))


    def extract_data(log_line: str) -> LogMessage:
        parsed = loads(log_line)
        timestamp = datetime.strptime(parsed["@timestamp"], '%Y-%m-%dT%H:%M:%S.%f%z')
        return LogMessage(timestamp, parsed["log.level"], parsed["log.logger"])


    @task
    def grouping_by_hour(messages: list) -> dict:
        result = {}
        for message in messages:
            hour_timestamp = message.timestamp.replace(minute=0, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
            if hour_timestamp in result:
                result[hour_timestamp].append(message)
            else:
                result[hour_timestamp] = [message]
        return result


    @task
    def make_hour_report(grouped: dict) -> list:
        num_errors = 0
        num_warnings = 0
        problematic_services = {}
        result = []
        for timestamp, messages in grouped.items():
            for message in messages:
                if message.level == 'WARN':
                    num_warnings += 1
                    if message.logger in problematic_services:
                        logger_data = problematic_services[message.logger]
                        logger_data.num_warnings += 1
                    else:
                        problematic_services[message.logger] = ProblematicServiceData(logger_name=message.logger,
                                                                                      num_warnings=1)
                if message.level == 'ERROR':
                    num_errors += 1
                    if message.logger in problematic_services:
                        logger_data = problematic_services[message.logger]
                        logger_data.num_errors += 1
                    else:
                        problematic_services[message.logger] = ProblematicServiceData(logger_name=message.logger,
                                                                                      num_errors=1)
            result.append(HourReport(timestamp=timestamp,
                                     total_warnings=num_warnings,
                                     total_errors=num_errors,
                                     problematic_services=list(problematic_services.values())))
        return result


    @task
    def make_daily_report(hour_reports_list: list) -> DailyReport:
        daily_errors = 0
        daily_warnings = 0

        for reports in hour_reports_list:
            for report in reports:
                daily_errors += report.total_errors
                daily_warnings += report.total_warnings

        return DailyReport(date=date.today().strftime("%Y-%m-%d"),
                           daily_errors=daily_errors,
                           daily_warnings=daily_warnings,
                           hour_reports=list(chain(hour_reports_list)))


    @task
    def to_kwargs(report: DailyReport):
        return {
            "s3_key": report.date + ".json",
            "data": dumps(report.__dict__, indent=4)
        }


    list_keys = S3ListOperator(
        task_id="list_log_files",
        bucket="logs",
        prefix="timestamp",
        aws_conn_id='minio'
    )
    file_content = read_file_content.partial(bucket="logs").expand(file_name=list_keys.output)
    grouped_by_hour = grouping_by_hour.expand(messages=file_content)
    reports_per_hour = make_hour_report.expand(grouped=grouped_by_hour)
    daily_report = make_daily_report(reports_per_hour)
    load_object_args = to_kwargs(daily_report)

    load_report = S3CreateObjectOperator.partial(
        aws_conn_id='minio',
        task_id="load_daily_report",
        s3_bucket="daily-reports",
        replace="true"
    ).expand_kwargs([load_object_args])
