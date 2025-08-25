import click


@click.group()
@click.pass_context
def cli(ctx):
    """Ahorramas CLI Tool"""
    ctx.ensure_object(dict)


@cli.command()
def test():
    print("Test")


if __name__ == "__main__":
    cli()
