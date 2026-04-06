from importlib import import_module
from pkgutil import iter_modules


def _load_language_modules():
    translations = {}
    country_names = {}
    for module_info in iter_modules(__path__):
        language_code = module_info.name
        module = import_module(f"{__name__}.{language_code}")
        translation_map = getattr(module, "TRANSLATIONS", None)
        country_name_map = getattr(module, "COUNTRY_NAMES", None)
        if not isinstance(translation_map, dict):
            raise TypeError(f"languages/{language_code}.py missing TRANSLATIONS dict")
        if not isinstance(country_name_map, dict):
            raise TypeError(f"languages/{language_code}.py missing COUNTRY_NAMES dict")
        translations[language_code] = translation_map
        country_names[language_code] = country_name_map
    return translations, country_names


TRANSLATIONS, COUNTRY_NAMES = _load_language_modules()
SUPPORTED_LANGUAGES = set(TRANSLATIONS.keys())

if "en" in COUNTRY_NAMES:
    COUNTRY_ORDER = list(COUNTRY_NAMES["en"].keys())
else:
    first_language = next(iter(COUNTRY_NAMES.values()), {})
    COUNTRY_ORDER = list(first_language.keys())

__all__ = [
    "COUNTRY_NAMES",
    "COUNTRY_ORDER",
    "SUPPORTED_LANGUAGES",
    "TRANSLATIONS",
]
