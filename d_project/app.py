from typer import Typer, Context, Argument, Option
from pathlib import Path
from .utils import parse_config_overrides, project_run, print_run_help, project_document
import srsly



PROJECT_FILE = "project.yml"
PROJECT_LOCK = "project.lock"
app= Typer()


@app.command('run')
def run(ctx: Context,
        subcommand: str = Argument(None, help=f"Name of command defined in the {PROJECT_FILE}"),
        project_dir: Path = Argument(Path.cwd(), help="Location of project directory. Defaults to current working directory.", exists=True, file_okay=False),
        dry: bool = Option(False, "--dry", "-D", help="Perform a dry run and don't execute scripts"),
        show_help: bool = Option(False, "--help", help="Show help message and available subcommands"),
        force: bool = Option(False, "--force", "-F", help="Force re-running steps, even if nothing changed")):
    if show_help or not subcommand:
        print_run_help(project_dir=project_dir, subcommand=subcommand)
    else:
        overrides = parse_config_overrides(ctx.args)
        project_run(project_dir, subcommand, overrides=overrides, force=force, dry=dry)
        


@app.command('document')
def project_document_cli(
    project_dir: Path = Argument(Path.cwd(), help="Path to cloned project. Defaults to current working directory.", exists=True, file_okay=False),
    output_file: Path = Option("-", "--output", "-o", help="Path to output Markdown file for output. Defaults to - for standard output"),
    no_emoji: bool = Option(False, "--no-emoji", "-NE", help="Don't use emoji")):
    """
    Auto-generate a README.md for a project. If the content is saved to a file,
    hidden markers are added so you can add custom content before or after the
    auto-generated section and only the auto-generated docs will be replaced
    when you re-run the command.
    """
    project_document(project_dir, output_file, no_emoji=no_emoji)


BASE_CONTENT = {'title': 'project-demo',
                'description': 'describe project details',
                'vars': {'name': 'demo'},
                'check_requirements': False,
                'assets': [{'dest': 'assets/demo.txt', 'url': 'https://demo.com', 'description': 'demo description'}],
                'directories': ['assets'],
                'workflows': {'all': ['command1', 'command2']},
                'commands': [{'name': 'command1',
                              'help': 'command1 help',
                              'script': ['python **']}, 
                             {'name': 'command2',
                              'help': 'command2 help',
                              'script': ['python **']}]}

@app.command('init')
def init_project(path: str = Argument(default='./project.yml', help='初始化项目文件project.yml的路径')):
    srsly.write_yaml(path=path, data=BASE_CONTENT)