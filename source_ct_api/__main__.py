"""Entry point: python -m source_ct_api <command> [options]"""

import sys

from airbyte_cdk.entrypoint import launch

from .source import SourceCtApi


def run():
    source = SourceCtApi()
    launch(source, sys.argv[1:])


if __name__ == "__main__":
    run()
