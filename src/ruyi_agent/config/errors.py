"""Stable exceptions raised at declarative configuration boundaries."""


class ConfigError(ValueError):
    """A declarative configuration value is invalid."""
