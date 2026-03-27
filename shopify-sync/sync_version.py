"""Versionsdefinition fuer den separaten Shopify-Sync."""

SYNC_VERSION_MAJOR = 0
SYNC_VERSION_MINOR = 1
SYNC_VERSION_PATCH = 0

# Leer fuer produktive Sync-Releases, sonst z. B. "dev".
SYNC_VERSION_STAGE = "dev"
SYNC_VERSION_BUILD = 1


def build_sync_version() -> str:
    base = f"{SYNC_VERSION_MAJOR}.{SYNC_VERSION_MINOR}.{SYNC_VERSION_PATCH}"
    if SYNC_VERSION_STAGE:
        if SYNC_VERSION_BUILD:
            return f"{base}-{SYNC_VERSION_STAGE}.{SYNC_VERSION_BUILD}"
        return f"{base}-{SYNC_VERSION_STAGE}"
    return base


SYNC_VERSION = build_sync_version()
