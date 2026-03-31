#!/usr/bin/env python3
import json
import os
import shutil
import sys
import tempfile
import zipfile
from pathlib import Path


PORTABLE_KEYS = {
    "db_host", "db_name", "db_user", "db_pass",
    "language", "color_theme",
    "gls_api_url", "gls_user", "gls_password", "gls_contact_id",
    "post_api_url", "post_api_key", "post_api_secret", "post_user", "post_password", "post_partner_id",
    "shipping_active_carriers",
    "shipping_services", "shipping_packaging_weight_grams",
    "shopify_tracking_mode_gls", "shopify_tracking_mode_post",
    "shopify_tracking_url_gls", "shopify_tracking_url_post",
    "delivery_note_sender_name", "delivery_note_sender_street", "delivery_note_sender_city", "delivery_note_sender_email",
}

NON_OVERWRITTEN_LOCAL_KEYS = [
    "picklist_printer", "delivery_note_printer", "delivery_note_format",
    "shipping_label_printer", "shipping_label_printer_gls", "shipping_label_printer_free", "shipping_label_printer_post",
    "shipping_label_output_dir", "shipping_label_format", "shipping_label_format_gls", "shipping_label_format_free", "shipping_label_format_post",
    "pdf_output_dir", "printer_uri", "printer_model", "label_size",
]


def _load_merged_settings(root_dir):
    settings = {}
    for path in (root_dir / "settings.json", root_dir / "settings.local.json"):
        if path.exists():
            settings.update(json.loads(path.read_text(encoding="utf-8")))
    return settings


def _copy_if_exists(source, destination):
    if Path(source).is_file():
        Path(destination).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)


def _copy_gls_credentials(root_dir, stage_dir):
    for pdf in (root_dir / "gls").glob("*.pdf"):
        if pdf.name.startswith(("testlabel", "gls_", "post_", "test_")):
            continue
        shutil.copy2(pdf, stage_dir / "files" / "gls" / pdf.name)


def _copy_optional_setting_file(settings, bundle_settings, file_mappings, stage_dir, setting_key, relative_target):
    value = (settings.get(setting_key) or "").strip()
    if not value:
        return
    if value.startswith(("http://", "https://")):
        bundle_settings[setting_key] = value
        return
    source = Path(os.path.expanduser(value))
    if not source.is_file():
        return
    target = stage_dir / "files" / relative_target
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, target)
    bundle_settings[setting_key] = f"__BUNDLE__/{relative_target.replace(os.sep, '/')}"
    file_mappings.append({"setting": setting_key, "target": relative_target.replace(os.sep, "/")})


def create_bundle(root_dir):
    root_dir = Path(root_dir).resolve()
    export_dir = root_dir / "exports"
    export_dir.mkdir(parents=True, exist_ok=True)
    stamp = __import__("datetime").datetime.now().strftime("%Y%m%d_%H%M%S")
    stage_dir = export_dir / f"local_bundle_{stamp}"
    zip_path = export_dir / f"lager_mc_local_bundle_{stamp}.zip"
    if stage_dir.exists():
        shutil.rmtree(stage_dir)
    (stage_dir / "files" / "gls").mkdir(parents=True, exist_ok=True)
    (stage_dir / "files" / "shopify-sync").mkdir(parents=True, exist_ok=True)
    (stage_dir / "files" / "fonts").mkdir(parents=True, exist_ok=True)
    (stage_dir / "files" / "assets").mkdir(parents=True, exist_ok=True)

    _copy_if_exists(root_dir / "shopify-sync" / ".env", stage_dir / "files" / "shopify-sync" / ".env")
    _copy_if_exists(root_dir / "fonts" / "bahnschrift.ttf", stage_dir / "files" / "fonts" / "bahnschrift.ttf")
    _copy_if_exists(root_dir / "fonts" / "bahnschrift-condensed.ttf", stage_dir / "files" / "fonts" / "bahnschrift-condensed.ttf")
    _copy_if_exists(root_dir / "assets" / "lager-mc.svg", stage_dir / "files" / "assets" / "lager-mc.svg")
    _copy_gls_credentials(root_dir, stage_dir)

    settings = _load_merged_settings(root_dir)
    bundle_settings = {key: settings[key] for key in PORTABLE_KEYS if key in settings}
    file_mappings = []

    _copy_optional_setting_file(settings, bundle_settings, file_mappings, stage_dir, "delivery_note_template_path", "templates/delivery_note_template" + Path((settings.get("delivery_note_template_path") or "template")).suffix)
    _copy_optional_setting_file(settings, bundle_settings, file_mappings, stage_dir, "delivery_note_logo_source", "templates/delivery_note_logo" + Path((settings.get("delivery_note_logo_source") or "logo")).suffix)
    _copy_optional_setting_file(settings, bundle_settings, file_mappings, stage_dir, "free_label_template_path", "templates/free_label_template" + Path((settings.get("free_label_template_path") or "template")).suffix)
    _copy_optional_setting_file(settings, bundle_settings, file_mappings, stage_dir, "label_font_regular", "fonts/label_font_regular" + Path((settings.get("label_font_regular") or "font")).suffix)
    _copy_optional_setting_file(settings, bundle_settings, file_mappings, stage_dir, "label_font_condensed", "fonts/label_font_condensed" + Path((settings.get("label_font_condensed") or "font")).suffix)
    _copy_optional_setting_file(settings, bundle_settings, file_mappings, stage_dir, "color_theme_file", "themes/custom_theme" + Path((settings.get("color_theme_file") or "theme")).suffix)

    manifest = {
        "bundle_version": 2,
        "portable_settings": sorted(bundle_settings.keys()),
        "non_overwritten_local_keys": NON_OVERWRITTEN_LOCAL_KEYS,
        "setting_file_mappings": file_mappings,
    }
    (stage_dir / "settings.bundle.json").write_text(json.dumps(bundle_settings, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (stage_dir / "bundle_manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (stage_dir / "README.txt").write_text(
        "\n".join(
            [
                "Dieses Archiv enthaelt lokale/private Lagerverwaltungs-Dateien fuer einen neuen Arbeitsplatz.",
                "",
                "Installationsablauf:",
                "1. Git-Repo normal klonen.",
                "2. Dieses Archiv in das Projektverzeichnis kopieren.",
                "3. ./scripts/apply_local_bundle.sh <archiv.zip> ausfuehren.",
                "",
                "Enthalten sind portable/private Projektdateien wie:",
                "- API-Zugaenge und portable Settings",
                "- Shopify-Sync .env",
                "- GLS-Zugangspdf",
                "- Fonts, Logo und ausgewaehlte Vorlagen/Theme-Dateien",
                "",
                "Nicht ueberschrieben werden lokale Arbeitsplatz-Einstellungen wie:",
                "- Drucker",
                "- Druckformate",
                "- PDF-/Label-Zielordner",
                "- lokale Labeldrucker URI/Modell-Angaben",
                "",
            ]
        ),
        encoding="utf-8",
    )
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for path in stage_dir.rglob("*"):
            if path.is_file():
                archive.write(path, path.relative_to(stage_dir))
    shutil.rmtree(stage_dir)
    return zip_path


def apply_bundle(root_dir, bundle_path):
    root_dir = Path(root_dir).resolve()
    bundle_path = Path(bundle_path).resolve()
    with tempfile.TemporaryDirectory(prefix="lager-bundle-") as tmpdir:
        stage = Path(tmpdir)
        with zipfile.ZipFile(bundle_path, "r") as archive:
            archive.extractall(stage)

        manifest = json.loads((stage / "bundle_manifest.json").read_text(encoding="utf-8"))
        bundle_settings = json.loads((stage / "settings.bundle.json").read_text(encoding="utf-8"))
        protected_keys = set(manifest.get("non_overwritten_local_keys") or [])
        local_settings_path = root_dir / "settings.local.json"
        local_settings = {}
        if local_settings_path.exists():
            local_settings = json.loads(local_settings_path.read_text(encoding="utf-8"))

        for source in (stage / "files").rglob("*"):
            if not source.is_file():
                continue
            destination = root_dir / source.relative_to(stage / "files")
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, destination)

        for mapping in manifest.get("setting_file_mappings", []):
            key = mapping["setting"]
            target = mapping["target"]
            if bundle_settings.get(key, "").startswith("__BUNDLE__/"):
                bundle_settings[key] = str(root_dir / target)

        for key, value in bundle_settings.items():
            if key in protected_keys:
                continue
            local_settings[key] = value

        local_settings_path.write_text(json.dumps(local_settings, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        return local_settings_path


def main(argv=None):
    argv = list(argv or sys.argv[1:])
    if len(argv) < 2 or argv[0] not in {"create", "apply"}:
        print("Usage: local_bundle.py create <root_dir> | apply <root_dir> <bundle.zip>", file=sys.stderr)
        return 1
    command = argv[0]
    root_dir = argv[1]
    if command == "create":
        print(str(create_bundle(root_dir)))
        return 0
    if len(argv) < 3:
        print("Usage: local_bundle.py apply <root_dir> <bundle.zip>", file=sys.stderr)
        return 1
    print(str(apply_bundle(root_dir, argv[2])))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
