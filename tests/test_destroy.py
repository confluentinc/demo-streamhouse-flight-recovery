"""A failed airline-demo destroy must not go on to destroy core underneath it."""

import sys

import pytest

from scripts import destroy


def test_failed_demo_destroy_keeps_core(tmp_path, monkeypatch):
    (tmp_path / "credentials.env").write_text("TF_VAR_resource_prefix=test\n")
    for env in ("airline-demo", "core"):
        (tmp_path / "terraform" / env).mkdir(parents=True)
        (tmp_path / "terraform" / env / "terraform.tfstate").write_text("{}")

    attempted = []

    def fake_destroy(env_path):
        attempted.append(env_path.name)
        return env_path.name != "airline-demo"

    monkeypatch.setattr(destroy, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(destroy, "run_terraform_destroy", fake_destroy)
    monkeypatch.setattr(sys, "argv", ["destroy", "--testing"])

    with pytest.raises(SystemExit) as exit_info:
        destroy.main()

    assert exit_info.value.code == 1
    assert attempted == ["airline-demo"]
    assert (tmp_path / "terraform" / "core" / "terraform.tfstate").exists()
