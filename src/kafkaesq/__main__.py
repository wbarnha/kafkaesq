"""Allow ``python -m kafkaesq`` to invoke the CLI."""

from .cli import run

if __name__ == "__main__":
    run()
