import click

import sys
from os.path import abspath, dirname

# Add the parent directory to the sys.path
sys.path.append(dirname(dirname(abspath(__file__))))

from utils.logger import configure_logging


@click.group()
@click.option("--debug", is_flag=True, help="Enable debug mode.")
@click.pass_context
def cli(ctx, debug):
    """Ahorramas CLI Tool"""
    ctx.ensure_object(dict)
    configure_logging(debug)


@cli.command()
@click.pass_context
def get_supermarkets(ctx):
    pass


if __name__ == "__main__":
    cli()
