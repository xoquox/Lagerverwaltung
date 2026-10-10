import json
import tempfile
import subprocess
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts" / "install-standalone-de.sh"
RUNNER = ROOT / "scripts" / "run-installer-test.sh"


class InstallStandaloneDeScriptTests(unittest.TestCase):
    def test_script_exists(self):
        self.assertTrue(SCRIPT.is_file())

    def test_script_has_valid_bash_syntax(self):
        result = subprocess.run(
            ["bash", "-n", str(SCRIPT)],
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr)

    def test_runner_script_has_valid_bash_syntax(self):
        result = subprocess.run(
            ["bash", "-n", str(RUNNER)],
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr)

    def test_help_output_mentions_standalone_installer(self):
        result = subprocess.run(
            ["bash", str(SCRIPT), "--help"],
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertIn("Standalone-Installer", result.stdout)
        self.assertIn("Komplettinstallation", result.stdout)

    def run_installer_flow(self, provider):
        with tempfile.TemporaryDirectory() as directory:
            shell = r"""
installer="$1"
install_target="$2"
provider="$3"
shift 3
source "$installer"
MODE=complete
MODE_SET_BY_ARG=1
SHOP_SYSTEM_VALUE="$provider"
INSTALL_ROOT="$install_target"
LOG_FILE="$install_target/installer.log"
VENV_DIR="$install_target/venv"
DB_HOST_VALUE=localhost
DB_PORT_VALUE=5432
DB_NAME_VALUE=testdb
DB_USER_VALUE=testuser
DB_PASS_VALUE=''
PDF_DIR_VALUE=''
LABEL_DIR_VALUE=''
for function_name in ensure_linux ensure_guided_ui_backend detect_ui_backend setup_dialog_theme check_required_tools ui_progress ui_message consent_phase choose_target_dir download_release prepare_runtime_environment prompt_db_values prompt_optional_directories log_line; do
  eval "$function_name() { :; }"
done
configure_shopify_sync_env() { printf 'configure-shopify\n'; }
run_logged() {
  if [[ "$1" == "$VENV_DIR/bin/python" ]]; then
    printf 'configure-woo:%s\n' "${@: -1}"
  else
    "$@"
  fi
}
run_database_migrations() { printf 'migrate\n'; }
run_shopify_connect() { printf 'connect-shopify\n'; }
prompt_shopify_location_settings() { printf 'locations-shopify\n'; }
test_db_connection() { return 0; }
summarize_completion() { printf 'complete:%s\n' "$SHOP_SYSTEM_VALUE"; }
main
"""
            result = subprocess.run(['bash', '-c', shell, 'test', str(SCRIPT), directory, provider],
                                    capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            settings = json.loads((Path(directory) / 'settings.local.json').read_text())
            self.assertEqual(settings['shop_system'], provider)
            return result.stdout.splitlines()

    def test_woocommerce_installation_skips_shopify_setup(self):
        calls = self.run_installer_flow('woocommerce')
        self.assertTrue(calls[0].endswith('/woocommerce-sync/requirements.txt'))
        self.assertTrue(calls[1].endswith('/scripts/configure_woocommerce_sync.py'))
        self.assertEqual(calls[2:], ['migrate', 'complete:woocommerce'])

    def test_shopify_installation_verifies_database_before_connect(self):
        calls = self.run_installer_flow('shopify')
        self.assertEqual(calls, ['configure-shopify', 'migrate', 'connect-shopify',
                                'locations-shopify', 'complete:shopify'])

    def test_update_retains_existing_woocommerce_selection(self):
        with tempfile.TemporaryDirectory() as directory:
            (Path(directory) / 'settings.local.json').write_text(json.dumps({'shop_system': 'woocommerce'}))
            shell = r"""
installer="$1"
install_target="$2"
shift 2
source "$installer"
MODE=update
INSTALL_ROOT="$install_target"
ui_menu() { return 99; }
select_shop_system
printf '%s' "$SHOP_SYSTEM_VALUE"
"""
            result = subprocess.run(['bash', '-c', shell, 'test', str(SCRIPT), directory],
                                    capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stdout, 'woocommerce')


if __name__ == "__main__":
    unittest.main()
