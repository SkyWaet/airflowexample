from dataclasses import dataclass
from datetime import datetime, date
from itertools import chain
from itertools import groupby
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
        schedule="55 23 * * *",
):
    @task
    def read_file_content(bucket: str, file_name: str) -> list:
        s3_hook = S3Hook(aws_conn_id='minio')
        return list(map(extract_data, s3_hook.read_key(file_name, bucket).splitlines()))


    def extract_data(log_line: str) -> LogMessage:
        parsed = loads(log_line)
        timestamp = datetime.strptime(parsed["@timestamp"], '%Y-%m-%d--%H-%M')
        return LogMessage(timestamp, parsed["log.level"], parsed["log.logger"])


    @task
    def grouping_by_hour(messages: list) -> dict:
        result = {}
        for message in chain.from_iterable(messages):
            hour_timestamp = message.timestamp.replace(minute=0, second=0, microsecond=0).strftime("%Y-%m-%d--%H")
            if hour_timestamp in result:
                result[hour_timestamp].append(message)
            else:
                result[hour_timestamp] = [message]
        return result


    @task
    def make_hour_reports(grouped: dict) -> dict:
        result = {}

        for timestamp, messages in grouped.items():
            num_errors = 0
            num_warnings = 0
            problematic_services = []
            for logger, group in groupby(sorted(messages, key=lambda m: m.logger), lambda m: m.logger):
                logger_warnings = 0
                logger_errors = 0
                for message in group:
                    if message.level == 'WARN':
                        logger_warnings += 1
                    if message.level == 'ERROR':
                        logger_errors += 1
                if logger_warnings + logger_errors > 0:
                    problematic_services.append(ProblematicServiceData(logger_name=logger,
                                                                       num_warnings=logger_warnings,
                                                                       num_errors=logger_errors))
                num_warnings += logger_warnings
                num_errors += logger_errors

            result[timestamp] = HourReport(timestamp=timestamp,
                                           total_warnings=num_warnings,
                                           total_errors=num_errors,
                                           problematic_services=problematic_services)

        return result


    @task
    def make_daily_report(hour_reports: dict) -> DailyReport:
        daily_errors = 0
        daily_warnings = 0

        for report in hour_reports.values():
            daily_errors += report.total_errors
            daily_warnings += report.total_warnings

        return DailyReport(date=date.today().strftime("%Y-%m-%d"),
                           daily_errors=daily_errors,
                           daily_warnings=daily_warnings,
                           hour_reports=list(hour_reports.values()))


    @task
    def to_kwargs(report: DailyReport):
        return {
            "s3_key": report.date + ".json",
            "data": dumps(report.__dict__, indent=4)
        }


    list_keys = S3ListOperator(
        task_id="list_log_files",
        bucket="logs",
        prefix=date.today().strftime("%Y-%m-%d"),
        aws_conn_id='minio'
    )
    file_content = read_file_content.partial(bucket="logs").expand(file_name=list_keys.output)
    grouped_by_hour = grouping_by_hour(file_content)
    reports_per_hour = make_hour_reports(grouped_by_hour)
    daily_report = make_daily_report(reports_per_hour)
    load_object_args = to_kwargs(daily_report)

    load_report = S3CreateObjectOperator.partial(
        aws_conn_id='minio',
        task_id="load_daily_report",
        s3_bucket="daily-reports",
        replace="true"
    ).expand_kwargs([load_object_args])
