"""Service entrypoint utilities."""

from core.config import Settings


def run_service() -> None:
    """Start the application with validated configuration."""
    settings = Settings()
    print("Starting service with database URL:")
    print(settings.database_url)
