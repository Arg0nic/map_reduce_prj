import logging
import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

from libs.logging_config import configure_logging, format_log_fields


MIGRATIONS_DIR = Path(__file__).resolve().parents[1] / "migrations"
load_dotenv()
logger = logging.getLogger(__name__)


def iter_sql_statements(sql: str):
    for statement in sql.split(";"):
        statement = statement.strip()
        if statement:
            yield statement


def run_migrations(database_url: str, migrations_dir: Path = MIGRATIONS_DIR) -> None:
    engine = create_engine(database_url, future=True)

    with engine.begin() as connection:
        for path in sorted(migrations_dir.glob("*.sql")):
            sql = path.read_text(encoding="utf-8")
            for statement in iter_sql_statements(sql):
                connection.execute(text(statement))
            logger.info("applied migration %s", format_log_fields(migration=path.name))


def main() -> None:
    configure_logging("migrate")
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise SystemExit("DATABASE_URL is required to run migrations.")

    run_migrations(database_url)


if __name__ == "__main__":
    main()
