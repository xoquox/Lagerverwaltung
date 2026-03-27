"""Projektweite Versionsdefinition fuer Release- und Entwicklungsstaende."""

VERSION_MAJOR = 1
VERSION_MINOR = 21
VERSION_PATCH = 0

# Leer fuer produktive Releases auf `main`, sonst z. B. "dev".
VERSION_STAGE = "dev"
VERSION_BUILD = 1


def build_version() -> str:
    base = f"{VERSION_MAJOR}.{VERSION_MINOR}.{VERSION_PATCH}"
    if VERSION_STAGE:
        if VERSION_BUILD:
            return f"{base}-{VERSION_STAGE}.{VERSION_BUILD}"
        return f"{base}-{VERSION_STAGE}"
    return base


APP_VERSION = build_version()
