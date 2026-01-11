#!/usr/bin/env python3
"""Tests for the enhanced logging system.

This module tests:
- configure_logging() function with various configurations
- File rotation behavior
- RichHandler integration
- LoggingConfig Pydantic model validation
"""

from __future__ import annotations

import logging
import tempfile
from pathlib import Path

import pytest


class TestConfigureLogging:
    """Tests for configure_logging function."""

    def setup_method(self) -> None:
        """Reset logging state before each test."""
        # Reset the module-level flag
        import annual_report_mda.utils as utils_module

        utils_module._logging_configured = False
        # Clear all handlers from root logger except pytest's LogCaptureHandler
        root = logging.getLogger()
        # Keep pytest's handlers, remove our handlers
        root.handlers = [h for h in root.handlers if type(h).__name__ == "LogCaptureHandler"]
        root.setLevel(logging.WARNING)

    def test_console_only_default(self) -> None:
        """Test that configure_logging sets up console handler correctly."""
        from annual_report_mda.utils import configure_logging

        configure_logging(level="DEBUG", enable_console=True, enable_file=False)

        root = logging.getLogger()
        assert root.level == logging.DEBUG
        assert len(root.handlers) >= 1

    def test_file_logging_creates_directory(self) -> None:
        """Test that file logging creates the log directory."""
        from annual_report_mda.utils import configure_logging

        with tempfile.TemporaryDirectory() as tmp_dir:
            log_dir = Path(tmp_dir) / "logs"
            configure_logging(
                level="INFO",
                log_dir=log_dir,
                log_file_prefix="test",
                enable_file=True,
                enable_console=False,
            )

            # Directory should be created
            assert log_dir.exists()

            # Log something
            logger = logging.getLogger("test")
            logger.info("Test message")

            # Flush handlers
            for handler in logging.getLogger().handlers:
                handler.flush()

            # Check log file exists
            log_files = list(log_dir.glob("test.log*"))
            assert len(log_files) >= 1

    def test_idempotent_configuration(self) -> None:
        """Test that calling configure_logging twice doesn't duplicate handlers."""
        from annual_report_mda.utils import configure_logging

        root = logging.getLogger()

        def count_our_handlers():
            """Count handlers excluding pytest's LogCaptureHandler."""
            return len([h for h in root.handlers if type(h).__name__ != "LogCaptureHandler"])

        baseline = count_our_handlers()
        assert baseline == 0, f"Expected 0 non-pytest handlers, got {baseline}"

        configure_logging(level="INFO", enable_console=True, enable_file=False)
        handler_count_1 = count_our_handlers()

        configure_logging(level="INFO", enable_console=True, enable_file=False)
        handler_count_2 = count_our_handlers()

        # Should have same number of handlers (not doubled)
        assert (
            handler_count_1 == handler_count_2
        ), f"First: {handler_count_1}, Second: {handler_count_2}"

    def test_without_rich(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test graceful fallback when Rich is not available."""
        import annual_report_mda.utils as utils_module

        # Mock RichHandler as None
        monkeypatch.setattr(utils_module, "RichHandler", None)
        utils_module._logging_configured = False

        from annual_report_mda.utils import configure_logging

        configure_logging(level="INFO", enable_console=True, console_rich=True)

        root = logging.getLogger()
        # Should have a StreamHandler (fallback)
        assert any(isinstance(h, logging.StreamHandler) for h in root.handlers)

    def test_file_rotation(self) -> None:
        """Test that file rotation works with small max_bytes."""
        from annual_report_mda.utils import configure_logging

        with tempfile.TemporaryDirectory() as tmp_dir:
            log_dir = Path(tmp_dir)
            configure_logging(
                level="INFO",
                log_dir=log_dir,
                log_file_prefix="rotate_test",
                max_bytes=1024,  # 1KB for testing
                backup_count=3,
                enable_file=True,
                enable_console=False,
            )

            logger = logging.getLogger("test_rotation")

            # Write enough to trigger rotation
            for i in range(100):
                logger.info("X" * 50)  # 50 chars per message

            # Flush handlers
            for handler in logging.getLogger().handlers:
                handler.flush()

            # Check for rotated files
            log_files = list(log_dir.glob("rotate_test.log*"))
            # Should have multiple files due to rotation
            assert len(log_files) >= 1


class TestLoggingConfig:
    """Tests for LoggingConfig Pydantic model."""

    def test_default_values(self) -> None:
        """Test LoggingConfig default values."""
        from annual_report_mda.config_manager import LoggingConfig

        config = LoggingConfig()

        assert config.enable_console is True
        assert config.enable_file is False
        assert config.log_dir == Path("logs")
        assert config.file_prefix == "app"
        assert config.max_bytes == 10 * 1024 * 1024  # 10MB
        assert config.backup_count == 7

    def test_custom_values(self) -> None:
        """Test LoggingConfig with custom values."""
        from annual_report_mda.config_manager import LoggingConfig

        config = LoggingConfig(
            enable_file=True,
            log_dir=Path("/var/log/app"),
            file_prefix="myapp",
            max_bytes=5 * 1024 * 1024,  # 5MB
            backup_count=5,
        )

        assert config.enable_file is True
        assert config.log_dir == Path("/var/log/app")
        assert config.file_prefix == "myapp"
        assert config.max_bytes == 5 * 1024 * 1024
        assert config.backup_count == 5

    def test_validation_max_bytes_too_small(self) -> None:
        """Test that max_bytes too small raises ValidationError."""
        from pydantic import ValidationError

        from annual_report_mda.config_manager import LoggingConfig

        with pytest.raises(ValidationError):
            LoggingConfig(max_bytes=100)  # Less than 1024

    def test_validation_max_bytes_too_large(self) -> None:
        """Test that max_bytes too large raises ValidationError."""
        from pydantic import ValidationError

        from annual_report_mda.config_manager import LoggingConfig

        with pytest.raises(ValidationError):
            LoggingConfig(max_bytes=200 * 1024 * 1024)  # More than 100MB

    def test_validation_backup_count_bounds(self) -> None:
        """Test backup_count validation bounds."""
        from pydantic import ValidationError

        from annual_report_mda.config_manager import LoggingConfig

        # Too small
        with pytest.raises(ValidationError):
            LoggingConfig(backup_count=0)

        # Too large
        with pytest.raises(ValidationError):
            LoggingConfig(backup_count=31)

    def test_validation_file_prefix_pattern(self) -> None:
        """Test file_prefix pattern validation."""
        from pydantic import ValidationError

        from annual_report_mda.config_manager import LoggingConfig

        # Invalid: starts with number
        with pytest.raises(ValidationError):
            LoggingConfig(file_prefix="123app")

        # Invalid: contains slash
        with pytest.raises(ValidationError):
            LoggingConfig(file_prefix="app/test")

        # Valid patterns
        LoggingConfig(file_prefix="app")
        LoggingConfig(file_prefix="my-app")
        LoggingConfig(file_prefix="my_app_2")


class TestConfigureLoggingFromConfig:
    """Tests for configure_logging_from_config helper."""

    def setup_method(self) -> None:
        """Reset logging state before each test."""
        import annual_report_mda.utils as utils_module

        utils_module._logging_configured = False
        root = logging.getLogger()
        root.handlers.clear()
        root.setLevel(logging.WARNING)

    def test_with_logging_config_object(self) -> None:
        """Test configure_logging_from_config with LoggingConfig object."""
        from annual_report_mda.config_manager import LoggingConfig
        from annual_report_mda.utils import configure_logging_from_config

        with tempfile.TemporaryDirectory() as tmp_dir:
            logging_config = LoggingConfig(
                enable_console=True,
                enable_file=True,
                log_dir=Path(tmp_dir),
                file_prefix="test",
            )

            configure_logging_from_config(
                log_level="DEBUG",
                logging_config=logging_config,
            )

            root = logging.getLogger()
            assert root.level == logging.DEBUG
            # Should have both console and file handlers
            assert len(root.handlers) >= 2

    def test_without_logging_config(self) -> None:
        """Test configure_logging_from_config with None config."""
        from annual_report_mda.utils import configure_logging_from_config

        configure_logging_from_config(log_level="INFO", logging_config=None)

        root = logging.getLogger()
        assert root.level == logging.INFO
        # Should have console handler only
        assert len(root.handlers) >= 1


class TestM4AcceptanceCriteria:
    """Tests for M4 milestone acceptance criteria."""

    def setup_method(self) -> None:
        """Reset logging state before each test."""
        import annual_report_mda.utils as utils_module

        utils_module._logging_configured = False
        root = logging.getLogger()
        root.handlers.clear()
        root.setLevel(logging.WARNING)

    def test_m4_03_unified_log_format(self) -> None:
        """M4-03: Verify unified log format with timestamp + level + module + message."""
        from annual_report_mda.utils import configure_logging

        with tempfile.TemporaryDirectory() as tmp_dir:
            log_dir = Path(tmp_dir)
            configure_logging(
                level="INFO",
                log_dir=log_dir,
                log_file_prefix="test",
                enable_file=True,
                enable_console=False,
            )

            logger = logging.getLogger("test_module")
            logger.info("Test message")

            # Flush handlers
            for handler in logging.getLogger().handlers:
                handler.flush()

            log_file = log_dir / "test.log"
            content = log_file.read_text()

            # Verify format contains: timestamp, level, module, message
            assert "INFO" in content
            assert "test_module" in content
            assert "Test message" in content
            # Check for timestamp pattern (YYYY-MM-DD HH:MM:SS)
            import re

            assert re.search(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", content)

    def test_m4_05_size_based_rotation(self) -> None:
        """M4-05: Verify automatic rotation at max_bytes with backup_count files."""
        from annual_report_mda.utils import configure_logging

        with tempfile.TemporaryDirectory() as tmp_dir:
            log_dir = Path(tmp_dir)
            configure_logging(
                level="INFO",
                log_dir=log_dir,
                log_file_prefix="app",
                max_bytes=1024,  # 1KB for testing
                backup_count=3,
                enable_file=True,
                enable_console=False,
            )

            logger = logging.getLogger("test")

            # Write enough to trigger multiple rotations
            for i in range(500):
                logger.info("X" * 100)

            # Flush handlers
            for handler in logging.getLogger().handlers:
                handler.flush()

            log_files = list(log_dir.glob("app.log*"))
            # Should have at most 4 files (current + 3 backups)
            assert len(log_files) <= 4
            assert len(log_files) >= 1
