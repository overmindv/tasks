#!/usr/bin/env python3
"""Проверяет wire fixtures и отсутствие дрейфа schemas между tasks/sandbox."""
import argparse
import copy
import json
from pathlib import Path
from jsonschema import Draft202012Validator, FormatChecker


def schema_dir(root):
    return root / ("api/schemas" if (root / "api/schemas").is_dir() else "api/events")


def check(root, peer=None):
    fixtures = root / "tests/fixtures/execution-contract"
    checked = 0
    for kind in ("request", "result"):
        name = f"sandbox.run.{kind}.v1.json"
        schema = json.loads((schema_dir(root) / name).read_text())
        Draft202012Validator.check_schema(schema)
        validator = Draft202012Validator(schema, format_checker=FormatChecker())
        if peer:
            assert schema == json.loads((schema_dir(peer) / name).read_text()), f"schema drift: {name}"
        for mode in ("execution", "pytest", "go-test"):
            path = fixtures / f"{mode}.{kind}.json"
            payload = json.loads(path.read_text())
            if peer:
                assert payload == json.loads((peer / "tests/fixtures/execution-contract" / path.name).read_text()), f"fixture drift: {path.name}"
            validator.validate(payload)
            for invalid in (dict(payload, unexpected=True), dict(payload, schema_version="unknown.v2")):
                assert not validator.is_valid(invalid), f"accepted incompatible {path.name}"
            invalid = copy.deepcopy(payload)
            del invalid["attempt_id"]
            assert not validator.is_valid(invalid), f"accepted missing attempt: {path.name}"
            if kind == "request":
                invalid = copy.deepcopy(payload)
                invalid.pop("tests", None)
                invalid.pop("execution", None)
                assert not validator.is_valid(invalid), "accepted request without execution mode"
                if mode == "execution":
                    # Go сериализует zero-value TestSpec даже при omitempty.
                    payload["tests"] = {"mode": "", "source": ""}
                    validator.validate(payload)
                    invalid = copy.deepcopy(payload)
                    invalid["execution"]["inputs"] = []
                    assert not validator.is_valid(invalid), "accepted empty stdin cases"
            checked += 1
    print(f"OK: {checked} fixtures, negative cases" + (", peer schemas/fixtures identical" if peer else ""))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--peer", type=Path)
    args = parser.parse_args()
    check(Path(__file__).resolve().parents[1], args.peer.resolve() if args.peer else None)
