import hashlib
import io
import json
from pathlib import Path
import tarfile
import tempfile
import unittest

import release_registry_rehearsal as rehearsal


class RegistryRehearsalTests(unittest.TestCase):
    def test_index_paths(self):
        for name, expected in [("a", "1/a"), ("ab", "2/ab"), ("abc", "3/a/abc"), ("example", "ex/am/example")]:
            self.assertEqual(rehearsal.index_path(name), Path(expected))

    def test_alias_and_feature_identity(self):
        result = rehearsal.dependencies({"dependencies": {"matrix-sdk": {
            "package": "guigu-matrix-sdk", "version": "=0.13.0-guigu.1",
            "default-features": False, "features": ["sqlite"],
        }}})
        self.assertEqual(result[0]["name"], "matrix-sdk")
        self.assertEqual(result[0]["package"], "guigu-matrix-sdk")
        self.assertFalse(result[0]["default_features"])
        self.assertEqual(result[0]["features"], ["sqlite"])

    def test_non_registry_dependency_refuses(self):
        for forbidden in ["path", "git", "registry"]:
            with self.subTest(forbidden=forbidden), self.assertRaises(ValueError):
                rehearsal.dependencies({"dependencies": {"example": {"version": "1", forbidden: "unexpected"}}})

    def test_target_dependency_is_preserved(self):
        result = rehearsal.dependencies({"target": {"cfg(unix)": {"build-dependencies": {"example": "1"}}}})
        self.assertEqual(result[0]["target"], "cfg(unix)")
        self.assertEqual(result[0]["kind"], "build")

    def test_checksum_and_immutable_archive_refuse(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            registry = root / "registry"
            registry.mkdir()
            archive = root / "example-1.0.0.crate"
            archive.write_bytes(b"first")
            record = {"name": "example", "vers": "1.0.0", "cksum": "wrong"}
            with self.assertRaises(ValueError):
                rehearsal.add_archive(registry, archive, record)
            record["cksum"] = hashlib.sha256(b"first").hexdigest()
            rehearsal.add_archive(registry, archive, record)
            rehearsal.add_archive(registry, archive, record)
            archive.write_bytes(b"second")
            record["cksum"] = hashlib.sha256(b"second").hexdigest()
            with self.assertRaises(ValueError):
                rehearsal.add_archive(registry, archive, record)

    def test_archive_staging_uses_normalized_manifest(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            registry = root / "registry"
            registry.mkdir()
            archive = root / "example-1.0.0.crate"
            manifest = b'[package]\nname="example"\nversion="1.0.0"\n[features]\ndefault=[]\n'
            with tarfile.open(archive, "w:gz") as package:
                member = tarfile.TarInfo("example-1.0.0/Cargo.toml")
                member.size = len(manifest)
                package.addfile(member, io.BytesIO(manifest))
            rehearsal.stage(registry, archive)
            record = json.loads((registry / "index" / rehearsal.index_path("example")).read_text())
            self.assertEqual(record["name"], "example")
            self.assertEqual(record["features2"], {"default": []})
            self.assertEqual(record["cksum"], hashlib.sha256(archive.read_bytes()).hexdigest())


if __name__ == "__main__":
    unittest.main()
