"""Entry point: python -m source_customer_thermometer <command> [options]"""

import sys

from airbyte_cdk.entrypoint import launch

from .source import SourceCustomerThermometer


def run():
    source = SourceCustomerThermometer()
    launch(source, sys.argv[1:])


if __name__ == "__main__":
    run()
