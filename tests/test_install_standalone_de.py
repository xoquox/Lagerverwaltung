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


if __name__ == "__main__":
    unittest.main()
