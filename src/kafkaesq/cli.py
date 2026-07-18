"""Command-line interface: convert Kafka config files between libraries.

Input may be a librdkafka/Java-style ``key=value`` .properties file, JSON,
or YAML (auto-detected from the file extension and content, or forced with
``--input-format``). Output is JSON by default; ``--format yaml`` writes
YAML, and ``--format properties`` writes a .properties file (confluent
target only). YAML support requires PyYAML (``pip install kafkaesq[yaml]``).

Examples::

    # Confluent Cloud .properties file -> aiokafka kwargs as JSON
    kafkaesq client.properties --to aiokafka

    # YAML confluent config -> kafka-python kwargs as YAML
    kafkaesq client.yaml --to kafka-python --format yaml

    # JSON kafka-python kwargs -> librdkafka .properties, written to a file
    kafkaesq kwargs.json --to confluent --format properties -o client.properties

    # Read from stdin, write to stdout
    cat client.properties | kafkaesq --to kafka-python
"""

from __future__ import annotations

import argparse
import json
import sys
import warnings
from pathlib import Path
from typing import Any, TextIO

try:
    import yaml
except ModuleNotFoundError:
    yaml = None

from ._faust import BY_CONFLUENT_KEY_FOR_FAUST, BY_FAUST_KEY, looks_like_faust
from ._go import go_binding_guidance
from ._java import java_only_guidance
from ._mappings import (
    BY_CONFLUENT_KEY,
    BY_CONFLUENT_SSL_KEY,
    BY_SNAKE_KEY,
    BY_SNAKE_SSL_KEY,
    SSL_CONFLUENT_KEYS,
)
from .convert import (
    KafkaesqWarning,
    UnmappedConfigError,
    aiokafka_to_confluent,
    confluent_to_aiokafka,
    confluent_to_faust,
    confluent_to_kafka_python,
    faust_to_confluent,
    kafka_python_to_confluent,
)

LIBRARIES = ("confluent", "aiokafka", "kafka-python", "faust")


class CLIError(Exception):
    """Fatal CLI error; its message is printed to stderr."""


def _parse_properties(text: str) -> dict[str, str]:
    """Parse librdkafka/Java-style ``key=value`` properties."""
    config: dict[str, str] = {}
    for lineno, raw in enumerate(text.splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith(("#", "!")):
            continue
        key, sep, value = line.partition("=")
        if not sep or not key.strip():
            raise CLIError(f"line {lineno}: expected key=value, got {line!r}")
        config[key.strip()] = value.strip()
    return config


def _require_yaml() -> Any:
    if yaml is None:
        raise CLIError(
            "YAML support requires PyYAML; install it with "
            "'pip install kafkaesq[yaml]'"
        )
    return yaml


def _detect_input_format(path: str, text: str) -> str:
    suffix = Path(path).suffix.lower()
    if suffix == ".json":
        return "json"
    if suffix in (".yaml", ".yml"):
        return "yaml"
    if suffix in (".properties", ".conf", ".config", ".ini"):
        return "properties"
    if text.lstrip().startswith("{"):
        return "json"
    first_line = next(
        (
            line.strip()
            for line in text.splitlines()
            if line.strip() and not line.strip().startswith(("#", "!"))
        ),
        "",
    )
    # "bootstrap.servers=host:9092" is properties; "bootstrap.servers: host" is
    # YAML. When both separators appear, whichever comes first wins.
    if "=" in first_line and (
        ":" not in first_line or first_line.index("=") < first_line.index(":")
    ):
        return "properties"
    return "yaml"


def _load_config(text: str, input_format: str) -> dict[str, Any]:
    if input_format == "json":
        try:
            config = json.loads(text)
        except json.JSONDecodeError as exc:
            raise CLIError(f"invalid JSON input: {exc}") from exc
    elif input_format == "yaml":
        try:
            config = _require_yaml().safe_load(text)
        except Exception as exc:
            if isinstance(exc, CLIError):
                raise
            raise CLIError(f"invalid YAML input: {exc}") from exc
    else:
        return _parse_properties(text)
    if not isinstance(config, dict):
        raise CLIError(
            f"{input_format.upper()} input must be a mapping of config keys"
        )
    return config


def _detect_source(config: dict[str, Any]) -> str:
    if any("." in key for key in config):
        return "confluent"
    if looks_like_faust(config):
        return "faust"
    if any(key in BY_SNAKE_SSL_KEY for key in config):
        return "kafka-python"
    return "aiokafka"


def _to_confluent_view(
    config: dict[str, Any], source: str, on_unmapped: str
) -> dict[str, Any]:
    if source == "confluent":
        return dict(config)
    if source == "faust":
        return faust_to_confluent(config, on_unmapped=on_unmapped)
    if source == "kafka-python":
        return kafka_python_to_confluent(config, on_unmapped=on_unmapped)
    return aiokafka_to_confluent(config, on_unmapped=on_unmapped)


def _convert(
    config: dict[str, Any], source: str, target: str, on_unmapped: str
) -> dict[str, Any]:
    if source == target:
        raise CLIError(
            f"source and target are both {source!r}; nothing to convert "
            "(pass --from to override auto-detection)"
        )
    # Every conversion goes source -> confluent representation -> target;
    # the confluent step is the identity when the source is confluent, and
    # its keys are all mapped, so it adds no extra unmapped noise.
    conf = _to_confluent_view(config, source, on_unmapped)
    if target == "confluent":
        return conf
    if target == "faust":
        return confluent_to_faust(conf, on_unmapped=on_unmapped)
    if target == "aiokafka":
        # An ssl_context is a runtime object that cannot be written to a
        # config file, and building one would require the SSL files to
        # exist on this machine — skip it (a note is printed instead).
        return confluent_to_aiokafka(
            conf, on_unmapped=on_unmapped, build_ssl_context=False
        )
    return confluent_to_kafka_python(conf, on_unmapped=on_unmapped)


_SSL_CONTEXT_ARGS = (
    ("ssl.ca.location", "cafile"),
    ("ssl.certificate.location", "certfile"),
    ("ssl.key.location", "keyfile"),
    ("ssl.key.password", "password"),
)


_SECURITY_KEY_PREFIXES = ("sasl.", "ssl.")


def _security_keys(conf: dict[str, Any]) -> list[str]:
    return [
        key
        for key in conf
        if key == "security.protocol" or key.startswith(_SECURITY_KEY_PREFIXES)
    ]


def _needs_ssl_context(conf: dict[str, Any]) -> bool:
    if any(key in conf for key in SSL_CONFLUENT_KEYS):
        return True
    return str(conf.get("security.protocol", "")).upper() in ("SSL", "SASL_SSL")


def _ssl_context_note(conf: dict[str, Any]) -> str:
    """Explain how to build the omitted ssl_context at runtime."""
    args = ", ".join(
        f"{name}={conf[key]!r}"
        for key, name in _SSL_CONTEXT_ARGS
        if conf.get(key) is not None
    )
    return (
        "note: aiokafka needs an ssl_context, which is a runtime object and "
        "cannot be written to a config file. Build it when creating the "
        f"client, e.g. aiokafka.helpers.create_ssl_context({args})"
    )


def _to_properties(conf: dict[str, Any]) -> str:
    lines = []
    for key, value in sorted(conf.items()):
        if isinstance(value, bool):
            value = "true" if value else "false"
        lines.append(f"{key}={value}")
    return "\n".join(lines) + "\n"


def _read_input(path: str) -> str:
    if path == "-":
        return sys.stdin.read()
    try:
        with open(path, encoding="utf-8") as handle:
            return handle.read()
    except OSError as exc:
        raise CLIError(f"cannot read {path}: {exc}") from exc


def _write_output(path: str, text: str) -> None:
    if path == "-":
        sys.stdout.write(text)
        return
    try:
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(text)
    except OSError as exc:
        raise CLIError(f"cannot write {path}: {exc}") from exc


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="kafkaesq",
        description=(
            "Convert a Kafka client config file between confluent-kafka "
            "(librdkafka dotted keys), aiokafka, and kafka-python "
            "(snake_case kwargs). Input may be a key=value .properties file "
            "or a JSON object."
        ),
        epilog=(
            "See also: 'kafkaesq validate --help' to validate a config "
            "file without converting it."
        ),
    )
    parser.add_argument(
        "input",
        nargs="?",
        default="-",
        help="config file to read, or '-' for stdin (default)",
    )
    parser.add_argument(
        "--to",
        dest="target",
        required=True,
        choices=LIBRARIES,
        help="library to convert the config for",
    )
    parser.add_argument(
        "--from",
        dest="source",
        choices=LIBRARIES,
        help="source library (default: auto-detect from key style)",
    )
    parser.add_argument(
        "--format",
        "-f",
        dest="fmt",
        choices=("json", "yaml", "properties"),
        default="json",
        help=(
            "output format (default: json; yaml requires PyYAML; "
            "properties is only valid with --to confluent)"
        ),
    )
    parser.add_argument(
        "--input-format",
        choices=("json", "yaml", "properties"),
        help=(
            "input format (default: auto-detect from file extension "
            "and content)"
        ),
    )
    parser.add_argument(
        "--on-unmapped",
        choices=("raise", "warn", "ignore"),
        default="warn",
        help="what to do with keys that have no equivalent (default: warn)",
    )
    parser.add_argument(
        "-o",
        "--output",
        default="-",
        help="output file, or '-' for stdout (default)",
    )
    return parser


# --- validate subcommand -------------------------------------------------


def _lookup_key(key: str, source: str) -> tuple[str | None, Any]:
    """Resolve ``key`` for ``source`` to (canonical confluent key, converter).

    Returns ``(None, None)`` when the key is not recognized for the source.
    """
    if source == "confluent":
        mapping = BY_CONFLUENT_KEY.get(key)
        if mapping is not None:
            return mapping.confluent_key, mapping.to_snake
        ssl_mapping = BY_CONFLUENT_SSL_KEY.get(key)
        if ssl_mapping is not None:
            return ssl_mapping.confluent_key, ssl_mapping.to_snake
        if key in SSL_CONFLUENT_KEYS:
            return key, None
        return None, None
    if source == "faust":
        entry = BY_FAUST_KEY.get(key)
        if entry is None:
            return None, None
        mapping, converter = entry
        return mapping.confluent_key, converter
    # snake_case sources (aiokafka / kafka-python)
    mapping = BY_SNAKE_KEY.get(key)
    if mapping is not None:
        return mapping.confluent_key, mapping.to_confluent
    if source == "kafka-python":
        ssl_mapping = BY_SNAKE_SSL_KEY.get(key)
        if ssl_mapping is not None:
            return ssl_mapping.confluent_key, ssl_mapping.to_confluent
    return None, None


def _targets_for(canonical: str) -> list[str]:
    """Libraries (besides the source) a confluent-canonical key is usable with."""
    targets = ["confluent"]
    if canonical in BY_CONFLUENT_KEY or canonical in SSL_CONFLUENT_KEYS:
        targets.append("aiokafka")
    if canonical in BY_CONFLUENT_KEY or canonical in BY_CONFLUENT_SSL_KEY:
        targets.append("kafka-python")
    mapping = BY_CONFLUENT_KEY.get(canonical)
    faust_key = mapping.confluent_key if mapping is not None else canonical
    if faust_key in BY_CONFLUENT_KEY_FOR_FAUST:
        targets.append("faust")
    return targets


def _build_validation_report(
    config: dict[str, Any], source: str
) -> dict[str, Any]:
    ok: list[str] = []
    java_only: dict[str, str] = {}
    go_binding: dict[str, str] = {}
    unrecognized: list[str] = []
    invalid: dict[str, str] = {}
    portable: dict[str, list[str]] = {
        lib: [] for lib in LIBRARIES if lib != source
    }

    for key, value in config.items():
        canonical, converter = _lookup_key(key, source)
        if canonical is None:
            if source == "confluent":
                go_guidance = go_binding_guidance(key)
                if go_guidance is not None:
                    go_binding[key] = go_guidance
                    continue
                guidance = java_only_guidance(key, value)
                if guidance is not None:
                    java_only[key] = guidance
                    continue
            unrecognized.append(key)
            continue
        if converter is not None:
            try:
                converter(value)
            except (ValueError, TypeError) as exc:
                invalid[key] = str(exc)
                continue
        ok.append(key)
        for target in _targets_for(canonical):
            if target in portable:
                portable[target].append(key)

    return {
        "source": source,
        "total": len(config),
        "ok": ok,
        "java_only": java_only,
        "go_binding": go_binding,
        "unrecognized": unrecognized,
        "invalid": invalid,
        "portability": {
            lib: {"portable": len(keys), "total": len(config), "keys": keys}
            for lib, keys in portable.items()
        },
        "valid": not invalid,
    }


def _print_validation_report(report: dict[str, Any], out: TextIO) -> None:
    print(f"source: {report['source']} ({report['total']} keys)", file=out)
    if report["ok"]:
        print(f"ok ({len(report['ok'])}): {', '.join(report['ok'])}", file=out)
    if report["java_only"]:
        print(f"java-only ({len(report['java_only'])}):", file=out)
        for key, guidance in report["java_only"].items():
            print(f"  {key}: {guidance}", file=out)
    if report["go_binding"]:
        print(f"go-binding ({len(report['go_binding'])}):", file=out)
        for key, guidance in report["go_binding"].items():
            print(f"  {key}: {guidance}", file=out)
    if report["unrecognized"]:
        print(
            f"unrecognized ({len(report['unrecognized'])}): "
            f"{', '.join(report['unrecognized'])} — may still be valid for "
            "the source client, but kafkaesq cannot map them",
            file=out,
        )
    if report["invalid"]:
        print(f"invalid ({len(report['invalid'])}):", file=out)
        for key, error in report["invalid"].items():
            print(f"  {key}: {error}", file=out)
    portability = " · ".join(
        f"{lib} {stats['portable']}/{stats['total']}"
        for lib, stats in report["portability"].items()
    )
    print(f"portability: {portability}", file=out)
    warning_count = (
        len(report["java_only"])
        + len(report["go_binding"])
        + len(report["unrecognized"])
    )
    if not report["valid"]:
        print("result: INVALID (bad values)", file=out)
    elif warning_count:
        print(f"result: OK ({warning_count} warning(s))", file=out)
    else:
        print("result: OK", file=out)


def _build_validate_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="kafkaesq validate",
        description=(
            "Validate a Kafka client config file: check that keys are "
            "recognized, values parse, and report which libraries the "
            "config is portable to. Java-only keys (JKS truststores, JAAS, "
            "class-based serializers, ...) get targeted guidance."
        ),
    )
    parser.add_argument(
        "input",
        nargs="?",
        default="-",
        help="config file to read, or '-' for stdin (default)",
    )
    parser.add_argument(
        "--from",
        dest="source",
        choices=LIBRARIES,
        help="source library (default: auto-detect from key style)",
    )
    parser.add_argument(
        "--input-format",
        choices=("json", "yaml", "properties"),
        help="input format (default: auto-detect)",
    )
    parser.add_argument(
        "--format",
        "-f",
        dest="fmt",
        choices=("text", "json"),
        default="text",
        help="report format (default: text)",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="also fail (exit 1) on unrecognized or Java-only keys",
    )
    return parser


def _validate_main(argv: list[str], stderr: TextIO) -> int:
    args = _build_validate_parser().parse_args(argv)
    try:
        text = _read_input(args.input)
        input_format = args.input_format or _detect_input_format(args.input, text)
        config = _load_config(text, input_format)
        source = args.source or _detect_source(config)
        report = _build_validation_report(config, source)
        if args.fmt == "json":
            print(json.dumps(report, indent=2))
        else:
            _print_validation_report(report, sys.stdout)
    except (CLIError, ValueError) as exc:
        print(f"error: {exc}", file=stderr)
        return 1
    if not report["valid"]:
        return 1
    if args.strict and (
        report["java_only"] or report["go_binding"] or report["unrecognized"]
    ):
        return 1
    return 0


def main(argv: list[str] | None = None, *, stderr: TextIO | None = None) -> int:
    stderr = stderr if stderr is not None else sys.stderr
    argv = list(sys.argv[1:]) if argv is None else list(argv)
    if argv[:1] == ["validate"]:
        return _validate_main(argv[1:], stderr)
    args = _build_parser().parse_args(argv)

    if args.fmt == "properties" and args.target != "confluent":
        print(
            "error: --format properties is only valid with --to confluent "
            f"({args.target} takes snake_case kwargs, not a properties file)",
            file=stderr,
        )
        return 2

    try:
        text = _read_input(args.input)
        input_format = args.input_format or _detect_input_format(args.input, text)
        config = _load_config(text, input_format)
        source = args.source or _detect_source(config)

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always", KafkaesqWarning)
            result = _convert(config, source, args.target, args.on_unmapped)
        for caught_warning in caught:
            print(f"warning: {caught_warning.message}", file=stderr)

        if args.target == "aiokafka":
            conf_view = _to_confluent_view(config, source, "ignore")
            if _needs_ssl_context(conf_view):
                print(_ssl_context_note(conf_view), file=stderr)
        elif args.target == "faust":
            conf_view = _to_confluent_view(config, source, "ignore")
            if _security_keys(conf_view):
                print(
                    "note: faust configures authentication with the "
                    "broker_credentials app setting, a runtime object that "
                    "cannot be written to a config file — e.g. "
                    "faust.SASLCredentials(username=..., password=...)",
                    file=stderr,
                )

        if args.fmt == "properties":
            output_text = _to_properties(result)
        elif args.fmt == "yaml":
            output_text = _require_yaml().safe_dump(
                result, default_flow_style=False, sort_keys=True
            )
        else:
            output_text = json.dumps(result, indent=2, sort_keys=True) + "\n"
        _write_output(args.output, output_text)
    except (CLIError, UnmappedConfigError, ValueError) as exc:
        print(f"error: {exc}", file=stderr)
        return 1
    return 0


def run() -> None:
    sys.exit(main())
