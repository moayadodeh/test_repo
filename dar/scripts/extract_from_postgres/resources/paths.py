from pathlib import Path
from dar.config.settings import get_settings


settings = get_settings()

base_path = Path(settings.get("main_dir"))

project_path = base_path / "scripts/extract_from_postgres"

project_path_to_queries = project_path / "queries"

select_info_tables_sql = project_path_to_queries / "select_from_info_tables.sql"
select_conditions_sql = project_path_to_queries / "select_conditions.sql"
get_first_scrape_date_sql = project_path_to_queries / "get_first_scrape_date.sql"