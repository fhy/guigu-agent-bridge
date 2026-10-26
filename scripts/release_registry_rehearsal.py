"""Build an explicit offline rehearsal registry; never publish or read secrets."""

import argparse
import hashlib
import json
import re
from pathlib import Path
import shutil
import tarfile
import tomllib
import urllib.request


def index_path(name):
    if len(name) == 1:
        return Path("1", name)
    if len(name) == 2:
        return Path("2", name)
    if len(name) == 3:
        return Path("3", name[0], name)
    return Path(name[:2], name[2:4], name)


def add_archive(registry, archive, record):
    digest = hashlib.sha256(archive.read_bytes()).hexdigest()
    if digest != record["cksum"]:
        raise ValueError(f"checksum mismatch: {archive.name}")
    destination = registry / archive.name
    if destination.exists() and destination.read_bytes() != archive.read_bytes():
        raise ValueError(f"immutable archive conflict: {archive.name}")
    entry = registry / "index" / index_path(record["name"])
    entry.parent.mkdir(parents=True, exist_ok=True)
    records = [] if not entry.exists() else [json.loads(row) for row in entry.read_text().splitlines()]
    for previous in records:
        if previous["vers"] == record["vers"]:
            if previous != record:
                raise ValueError(f"immutable index conflict: {archive.name}")
            return
    shutil.copyfile(archive, destination)
    records.append(record)
    entry.write_text("".join(json.dumps(row, separators=(",", ":")) + "\n" for row in records))


def seed(registry, cache, index):
    for cached in sorted((index / ".cache").rglob("*")):
        if not cached.is_file():
            continue
        records = [part for part in cached.read_bytes().split(b"\0") if part.startswith(b'{"name":')]
        if records:
            destination = registry / "index" / cached.relative_to(index / ".cache")
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(b"\n".join(records) + b"\n")
    for archive in sorted(cache.glob("*.crate")):
        name, version = re.fullmatch(r"(.+?)-(\d.*)\.crate", archive.name).groups()
        candidates = index / ".cache" / index_path(name)
        if not candidates.exists():
            continue
        records = [json.loads(part) for part in candidates.read_bytes().split(b"\0") if part.startswith(b'{"name":')]
        selected = [record for record in records if record["vers"] == version]
        if len(selected) == 1:
            add_archive(registry, archive, selected[0])


def dependencies(manifest):
    sections = [(None, manifest)] + list(manifest.get("target", {}).items())
    result = []
    for target, tables in sections:
        for section, kind in [("dependencies", "normal"), ("dev-dependencies", "dev"), ("build-dependencies", "build")]:
            for alias, config in tables.get(section, {}).items():
                if isinstance(config, str):
                    config = {"version": config}
                if "git" in config or "registry" in config or "path" in config:
                    raise ValueError(f"non-registry normalized dependency: {alias}")
                result.append({
                    "name": alias, "req": config["version"], "features": config.get("features", []),
                    "optional": config.get("optional", False), "default_features": config.get("default-features", True),
                    "target": target, "kind": kind, "registry": None, "package": config.get("package"),
                })
    return result


def stage(registry, archive):
    with tarfile.open(archive, "r:gz") as package:
        manifests = [member for member in package.getmembers() if member.name.count("/") == 1 and member.name.endswith("/Cargo.toml")]
        if len(manifests) != 1:
            raise ValueError("expected one root normalized Cargo.toml")
        manifest = tomllib.loads(package.extractfile(manifests[0]).read().decode())
    if "patch" in manifest:
        raise ValueError("package contains root patch")
    identity = manifest["package"]
    record = {
        "name": identity["name"], "vers": identity["version"], "deps": dependencies(manifest),
        "cksum": hashlib.sha256(archive.read_bytes()).hexdigest(), "features": {},
        "features2": manifest.get("features", {}), "v": 2, "yanked": False,
        "rust_version": identity.get("rust-version"), "links": identity.get("links"),
    }
    add_archive(registry, archive, record)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("registry", type=Path)
    parser.add_argument("--seed-cache", type=Path)
    parser.add_argument("--seed-index", type=Path)
    parser.add_argument("--stage", type=Path)
    parser.add_argument("--cargo-home", type=Path)
    parser.add_argument("--fetch-index")
    args = parser.parse_args()
    args.registry.mkdir(parents=True, exist_ok=True)
    if args.seed_cache:
        if not args.seed_index:
            parser.error("--seed-index required")
        seed(args.registry, args.seed_cache, args.seed_index)
    if args.stage:
        stage(args.registry, args.stage)
    if args.fetch_index:
        name = args.fetch_index
        if not re.fullmatch(r"[a-z0-9_-]+", name):
            raise ValueError("invalid public index name")
        relative = index_path(name)
        with urllib.request.urlopen("https://index.crates.io/" + relative.as_posix(), timeout=30) as response:
            content = response.read()
        for row in content.splitlines():
            if json.loads(row)["name"] != name:
                raise ValueError("public index identity mismatch")
        destination = args.registry / "index" / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        if destination.exists():
            raise ValueError("refusing to overwrite rehearsal index")
        destination.write_bytes(content)
    if args.cargo_home:
        args.cargo_home.mkdir(parents=True, exist_ok=True)
        config = args.cargo_home / "config.toml"
        if config.exists():
            raise ValueError("refusing to overwrite existing Cargo configuration")
        config.write_text('[source.crates-io]\nreplace-with = "rehearsal"\n[source.rehearsal]\nlocal-registry = ' + json.dumps(str(args.registry.resolve())) + '\n[net]\noffline = true\n')
    print(json.dumps({"registry": str(args.registry), "archives": len(list(args.registry.glob("*.crate")))}))


if __name__ == "__main__":
    main()
