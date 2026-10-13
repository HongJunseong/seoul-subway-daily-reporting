"""Capture checksum contract; hashing here does not establish source authenticity."""
import hashlib
import hmac


def capture_version(manifest):
    version = manifest.get("schema_version")
    if type(version) is not int or version not in (1, 2):
        raise ValueError("unsupported source capture schema")
    return version


def validate_checksum(item, version):
    if version == 1:
        if "raw_sha256" in item:
            raise ValueError("legacy capture cannot declare a version 2 checksum")
    elif item.get("status") == "collected":
        value = item.get("raw_sha256")
        if (not isinstance(value, str) or len(value) != 64
                or any(c not in "0123456789abcdef" for c in value)):
            raise ValueError("collected source requires a lowercase SHA-256 checksum")
    elif "raw_sha256" in item:
        raise ValueError("unsuccessful source cannot declare a saved-payload checksum")


def verify_payload(content, item, version):
    validate_checksum(item, version)
    digest = hashlib.sha256(content).hexdigest()
    if version == 2 and not hmac.compare_digest(digest, item["raw_sha256"]):
        raise ValueError("saved source checksum differs from collection record")
    return digest


def integrity_summary(manifest, hashes):
    """Call after load_capture verified these exact bytes, not after metadata-only analysis."""
    version = capture_version(manifest)
    expected = {"usage.json": manifest["usage"], **{
        f"arrival-{i:02d}.json": item for i, item in enumerate(manifest["arrival_slots"])}}
    if set(hashes) != set(expected):
        raise ValueError("processing hashes differ from captured file set")
    for name, item in expected.items():
        validate_checksum(item, version)
        if version == 2 and hashes[name] != item["raw_sha256"]:
            raise ValueError("processing hashes differ from capture checksums")
    return {
        "status": "verified_against_capture" if version == 2 else "unverified_legacy",
        "algorithm": "sha256",
        "files_hashed_at_processing": len(hashes),
        "files_verified_against_capture": len(hashes) if version == 2 else 0,
        "limitations": [
            "Legacy checksums were not recorded at collection and cannot be reconstructed retrospectively",
            "Checksums detect changes relative to capture metadata, not authenticity if both files and metadata change",
        ],
    }
