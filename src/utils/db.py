from pathlib import Path
import os

from dotenv import load_dotenv
from sqlalchemy import create_engine, text


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def load_env():
    env_path = PROJECT_ROOT / ".env"
    load_dotenv(env_path, override=False)


def get_env_value(name, default=None, required=False):
    load_env()
    value = os.getenv(name, default)
    if required and not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def get_mysql_url(include_database=True):
    host = get_env_value("MYSQL_HOST", required=True)
    port = get_env_value("MYSQL_PORT", "3306", required=True)
    user = get_env_value("MYSQL_USER", required=True)
    password = get_env_value("MYSQL_PASSWORD", required=True)
    database = get_env_value("MYSQL_DATABASE", required=include_database)

    if include_database:
        return f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
    return f"mysql+pymysql://{user}:{password}@{host}:{port}"


def get_engine(include_database=True):
    return create_engine(get_mysql_url(include_database=include_database))


def split_sql_statements(sql_text):
    statements = []
    current_lines = []

    for line in sql_text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("--"):
            continue
        current_lines.append(line)
        if stripped.endswith(";"):
            statements.append("\n".join(current_lines).strip().rstrip(";"))
            current_lines = []

    if current_lines:
        statements.append("\n".join(current_lines).strip().rstrip(";"))

    return [statement for statement in statements if statement]


def run_sql(sql_text, include_database=True):
    engine = get_engine(include_database=include_database)
    statements = split_sql_statements(sql_text)

    with engine.begin() as connection:
        for statement in statements:
            connection.execute(text(statement))


def run_sql_file(sql_file_path, include_database=True):
    sql_path = Path(sql_file_path)
    if not sql_path.is_absolute():
        sql_path = PROJECT_ROOT / sql_path

    if not sql_path.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_path}")

    sql_text = sql_path.read_text(encoding="utf-8")
    run_sql(sql_text, include_database=include_database)
