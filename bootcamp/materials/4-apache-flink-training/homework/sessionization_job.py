import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, DataTypes, StreamTableEnvironment
from pyflink.table.expressions import lit, col
from pyflink.table.window import Session


def create_processed_events_source_kafka(t_env):
    """Create Kafka source table for processed events"""
    kafka_key = os.environ.get("KAFKA_WEB_TRAFFIC_KEY", "")
    kafka_secret = os.environ.get("KAFKA_WEB_TRAFFIC_SECRET", "")
    table_name = "process_events_kafka"
    pattern = "yyyy-MM-dd''T''HH:mm:ss.SSS''Z''"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            ip VARCHAR,
            event_time VARCHAR,
            referrer VARCHAR,
            host VARCHAR,
            url VARCHAR,
            geodata VARCHAR,
            event_timestamp AS TO_TIMESTAMP(event_time, '{pattern}'),
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '15' SECOND
        ) WITH (
             'connector' = 'kafka',
            'properties.bootstrap.servers' = '{os.environ.get('KAFKA_URL')}',
            'topic' = '{os.environ.get('KAFKA_TOPIC')}',
            'properties.group.id' = '{os.environ.get('KAFKA_GROUP')}',
            'properties.security.protocol' = 'SASL_SSL',
            'properties.sasl.mechanism' = 'PLAIN',
            'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{kafka_key}\" password=\"{kafka_secret}\";',
            'scan.startup.mode' = 'latest-offset',
            'properties.auto.offset.reset' = 'latest',
            'format' = 'json'
        );
    """
    t_env.execute_sql(source_ddl)
    return table_name


def create_session_analysis_sink_postgres(t_env):
    """Create PostgreSQL sink table for session analysis results"""
    table_name = 'session_analysis'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            session_start TIMESTAMP(3),
            session_end TIMESTAMP(3),
            ip VARCHAR,
            host VARCHAR,
            event_count BIGINT,
            session_duration_minutes DOUBLE
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{os.environ.get("POSTGRES_URL")}',
            'table-name' = '{table_name}',
            'username' = '{os.environ.get("POSTGRES_USER", "postgres")}',
            'password' = '{os.environ.get("POSTGRES_PASSWORD", "postgres")}',
            'driver' = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name


def create_host_comparison_sink_postgres(t_env):
    """Create PostgreSQL sink table for host comparison results"""
    table_name = 'host_comparison_analysis'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            host VARCHAR,
            avg_events_per_session DOUBLE,
            total_sessions BIGINT,
            total_events BIGINT
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{os.environ.get("POSTGRES_URL")}',
            'table-name' = '{table_name}',
            'username' = '{os.environ.get("POSTGRES_USER", "postgres")}',
            'password' = '{os.environ.get("POSTGRES_PASSWORD", "postgres")}',
            'driver' = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name


def sessionization_analysis():
    """Main function to perform sessionization analysis"""
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10000)  # 10 seconds
    env.set_parallelism(3)

    # Set up the table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    try:
        # Create source and sink tables
        source_table = create_processed_events_source_kafka(t_env)
        session_sink = create_session_analysis_sink_postgres(t_env)
        host_comparison_sink = create_host_comparison_sink_postgres(t_env)

        # Sessionization with 5-minute gap by IP address and host
        print("Starting sessionization analysis...")
        
        # Create sessionized data
        sessionized_events = t_env.from_path(source_table) \
            .window(
                Session.with_gap(lit(5).minutes).on(col("event_timestamp")).alias("session_window")
            ) \
            .group_by(
                col("session_window"),
                col("ip"),
                col("host")
            ) \
            .select(
                col("session_window").start.alias("session_start"),
                col("session_window").end.alias("session_end"),
                col("ip"),
                col("host"),
                col("ip").count.alias("event_count"),
                ((col("session_window").end - col("session_window").start).cast(DataTypes.BIGINT()) / 60000.0).alias("session_duration_minutes")
            )

        # Insert sessionized data into PostgreSQL
        sessionized_events.execute_insert(session_sink)

        # Host comparison analysis - average events per session by host
        host_analysis = t_env.from_path(source_table) \
            .window(
                Session.with_gap(lit(5).minutes).on(col("event_timestamp")).alias("session_window")
            ) \
            .group_by(
                col("session_window"),
                col("ip"),
                col("host")
            ) \
            .select(
                col("host"),
                col("ip").count.alias("events_in_session")
            ) \
            .group_by(col("host")) \
            .select(
                col("host"),
                col("events_in_session").avg.alias("avg_events_per_session"),
                col("events_in_session").count.alias("total_sessions"),
                col("events_in_session").sum.alias("total_events")
            )

        # Insert host comparison analysis into PostgreSQL
        host_analysis.execute_insert(host_comparison_sink).wait()

        print("Sessionization analysis completed successfully!")

    except Exception as e:
        print("Sessionization analysis failed:", str(e))


if __name__ == '__main__':
    sessionization_analysis()