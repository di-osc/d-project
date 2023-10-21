from pathlib import Path
from typing import Optional, Dict, Any, Union, List, Sequence, Tuple, Iterable, Iterator
from rich.console import Console
import srsly
from .schema import ProjectConfigSchema, validate
import sys
from contextlib import contextmanager
from confection import ConfigValidationError, Config
from configparser import InterpolationError
from click import NoSuchOption
from click.parser import split_arg_string
from wasabi import msg
import os
import shlex
import subprocess
import pkg_resources
import hashlib
from rich import print
from rich.table import Table


console = Console(color_system='auto')

PROJECT_FILE = "project.yml"
PROJECT_LOCK = "project.lock"

class ENV_VARS:
    CONFIG_OVERRIDES = "D_CONFIG_OVERRIDES"
    PROJECT_USE_GIT_VERSION = "D_PROJECT_USE_GIT_VERSION"


class SimpleFrozenDict(dict):
    """Simplified implementation of a frozen dict, mainly used as default
    function or method argument (for arguments that should default to empty
    dictionary). Will raise an error if user or spaCy attempts to add to dict.
    """

    def __init__(self, *args, **kwargs) -> None:
        """Initialize the frozen dict. Can be initialized with pre-defined
        values.
        error (str): The error message when user tries to assign to dict.
        """
        super().__init__(*args, **kwargs)

    def __setitem__(self, key, value):
        raise NotImplementedError()

    def pop(self, key, default=None):
        raise NotImplementedError()

    def update(self, other):
        raise NotImplementedError()


class SimpleFrozenList(list):
    """Wrapper class around a list that lets us raise custom errors if certain
    attributes/methods are accessed. Mostly used for properties like
    Language.pipeline that return an immutable list (and that we don't want to
    convert to a tuple to not break too much backwards compatibility). If a user
    accidentally calls nlp.pipeline.append(), we can raise a more helpful error.
    """

    def __init__(self, *args) -> None:
        """Initialize the frozen list.
        error (str): The error message when user tries to mutate the list.
        """
        super().__init__(*args)

    def append(self, *args, **kwargs):
        raise NotImplementedError()

    def clear(self, *args, **kwargs):
        raise NotImplementedError()

    def extend(self, *args, **kwargs):
        raise NotImplementedError()

    def insert(self, *args, **kwargs):
        raise NotImplementedError()

    def pop(self, *args, **kwargs):
        raise NotImplementedError()

    def remove(self, *args, **kwargs):
        raise NotImplementedError()

    def reverse(self, *args, **kwargs):
        raise NotImplementedError()

    def sort(self, *args, **kwargs):
        raise NotImplementedError()
    


def validate_project_commands(config: Dict[str, Any]) -> None:
    """Check that project commands and workflows are valid, don't contain
    duplicates, don't clash  and only refer to commands that exist.
    config (Dict[str, Any]): The loaded config.
    """
    command_names = [cmd["name"] for cmd in config.get("commands", [])]
    workflows = config.get("workflows", {})
    duplicates = set([cmd for cmd in command_names if command_names.count(cmd) > 1])
    if duplicates:
        err = f"Duplicate commands defined in {PROJECT_FILE}: {', '.join(duplicates)}"
        console.print(err)
    for workflow_name, workflow_steps in workflows.items():
        if workflow_name in command_names:
            err = f"Can't use workflow name '{workflow_name}': name already exists as a command"
            console.print(err)
        for step in workflow_steps:
            if step not in command_names:
                console.print(
                    f"Unknown command specified in workflow '{workflow_name}': {step}",
                    f"Workflows can only refer to commands defined in the 'commands' "
                    f"section of the {PROJECT_FILE}.")
                
@contextmanager
def show_validation_error(
    file_path: Optional[Union[str, Path]] = None,
    *,
    title: Optional[str] = None,
    desc: str = "",
    show_config: Optional[bool] = None,
    hint_fill: bool = True,
):
    """Helper to show custom config validation errors on the CLI.
    file_path (str / Path): Optional file path of config file, used in hints.
    title (str): Override title of custom formatted error.
    desc (str): Override description of custom formatted error.
    show_config (bool): Whether to output the config the error refers to.
    hint_fill (bool): Show hint about filling config.
    """
    try:
        yield
    except ConfigValidationError as e:
        title = title if title is not None else e.title
        if e.desc:
            desc = f"{e.desc}" if not desc else f"{e.desc}\n\n{desc}"
        # Re-generate a new error object with overrides
        err = e.from_error(e, title="", desc=desc, show_config=show_config)
        console.print(title)
        print(err.text.strip())
        if hint_fill and "value_error.missing" in err.error_types:
            config_path = (
                file_path
                if file_path is not None and str(file_path) != "-"
                else "config.cfg"
            )
            console.print(
                "If your config contains missing values, you can run the 'init "
                "fill-config' command to fill in all the defaults, if possible:")
        sys.exit(1)
    except InterpolationError as e:
        console.print("Config validation error", e, exits=1)
        
def _parse_overrides(args: List[str], is_cli: bool = False) -> Dict[str, Any]:
    result = {}
    while args:
        opt = args.pop(0)
        err = f"Invalid config override '{opt}'"
        if opt.startswith("--"):  # new argument
            orig_opt = opt
            opt = opt.replace("--", "")
            if "." not in opt:
                if is_cli:
                    raise NoSuchOption(orig_opt)
                else:
                    msg.fail(f"{err}: can't override top-level sections", exits=1)
            if "=" in opt:  # we have --opt=value
                opt, value = opt.split("=", 1)
                opt = opt.replace("-", "_")
            else:
                if not args or args[0].startswith("--"):  # flag with no value
                    value = "true"
                else:
                    value = args.pop(0)
            result[opt] = _parse_override(value)
        else:
            msg.fail(f"{err}: name should start with --", exits=1)
    return result

def _parse_override(value: Any) -> Any:
    # Just like we do in the config, we're calling json.loads on the
    # values. But since they come from the CLI, it'd be unintuitive to
    # explicitly mark strings with escaped quotes. So we're working
    # around that here by falling back to a string if parsing fails.
    # TODO: improve logic to handle simple types like list of strings?
    try:
        return srsly.json_loads(value)
    except ValueError:
        return str(value)
        
        
def substitute_project_variables(config: Dict[str, Any],
                                 overrides: Dict[str, Any] = SimpleFrozenDict(),
                                 key: str = "vars",
                                 env_key: str = "env") -> Dict[str, Any]:
    """Interpolate variables in the project file using the config system.
    config (Dict[str, Any]): The project config.
    overrides (Dict[str, Any]): Optional config overrides.
    key (str): Key containing variables in project config.
    env_key (str): Key containing environment variable mapping in project config.
    RETURNS (Dict[str, Any]): The interpolated project config.
    """
    config.setdefault(key, {})
    config.setdefault(env_key, {})
    # Substitute references to env vars with their values
    for config_var, env_var in config[env_key].items():
        config[env_key][config_var] = _parse_override(os.environ.get(env_var, ""))
    # Need to put variables in the top scope again so we can have a top-level
    # section "project" (otherwise, a list of commands in the top scope wouldn't)
    # be allowed by Thinc's config system
    cfg = Config({"project": config, key: config[key], env_key: config[env_key]})
    cfg = Config().from_str(cfg.to_str(), overrides=overrides)
    interpolated = cfg.interpolate()
    return dict(interpolated["project"])


def load_project_config(path: Path, 
                        interpolate: bool = True, 
                        overrides: Dict[str, Any] = SimpleFrozenDict()) -> Dict[str, Any]:
    """Load the project.yml file from a directory and validate it. Also make
    sure that all directories defined in the config exist.
    path (Path): The path to the project directory.
    interpolate (bool): Whether to substitute project variables.
    overrides (Dict[str, Any]): Optional config overrides.
    RETURNS (Dict[str, Any]): The loaded project.yml.
    """
    config_path = Path(path, PROJECT_FILE)
    if not config_path.exists():
        console.print(f"Can't find {PROJECT_FILE}")
    invalid_err = f"Invalid {PROJECT_FILE}. Double-check that the YAML is correct."
    try:
        config = srsly.read_yaml(config_path)
    except ValueError as e:
        console.print(invalid_err)
    errors = validate(ProjectConfigSchema, config)
    if errors:
        console.print(invalid_err)
        print("\n".join(errors))
        sys.exit(1)
    validate_project_commands(config)
    # Make sure directories defined in config exist
    for subdir in config.get("directories", []):
        dir_path = path / subdir
        if not dir_path.exists():
            dir_path.mkdir(parents=True)
    if interpolate:
        err = f"{PROJECT_FILE} validation error"
        with show_validation_error(title=err, hint_fill=False):
            config = substitute_project_variables(config, overrides)
    return config


def is_cwd(path: Union[Path, str]) -> bool:
    """Check whether a path is the current working directory.
    path (Union[Path, str]): The directory path.
    RETURNS (bool): Whether the path is the current working directory.
    """
    return str(Path(path).resolve()).lower() == str(Path.cwd().resolve()).lower()

def split_command(command: str) -> List[str]:
    """Split a string command using shlex. Handles platform compatibility.
    command (str) : The command to split
    RETURNS (List[str]): The split command.
    """
    is_windows = sys.platform.startswith('win')
    return shlex.split(command, posix=not is_windows)


def join_command(command: List[str]) -> str:
    """Join a command using shlex. shlex.join is only available for Python 3.8+,
    so we're using a workaround here.
    command (List[str]): The command to join.
    RETURNS (str): The joined command
    """
    return " ".join(shlex.quote(cmd) for cmd in command)


def run_command(command: Union[str, List[str]],
                *,
                stdin: Optional[Any] = None,
                capture: bool = False,) -> subprocess.CompletedProcess:
    """Run a command on the command line as a subprocess. If the subprocess
    returns a non-zero exit code, a system exit is performed.
    command (str / List[str]): The command. If provided as a string, the
        string will be split using shlex.split.
    stdin (Optional[Any]): stdin to read from or None.
    capture (bool): Whether to capture the output and errors. If False,
        the stdout and stderr will not be redirected, and if there's an error,
        sys.exit will be called with the return code. You should use capture=False
        when you want to turn over execution to the command, and capture=True
        when you want to run the command more like a function.
    RETURNS (Optional[CompletedProcess]): The process object.
    """
    if isinstance(command, str):
        cmd_list = split_command(command)
        cmd_str = command
    else:
        cmd_list = command
        cmd_str = " ".join(command)
    try:
        ret = subprocess.run(
            cmd_list,
            env=os.environ.copy(),
            input=stdin,
            encoding="utf8",
            check=False,
            stdout=subprocess.PIPE if capture else None,
            stderr=subprocess.STDOUT if capture else None,
        )
    except FileNotFoundError:
        # Indicates the *command* wasn't found, it's an error before the command
        # is run.
        raise FileNotFoundError()
    if ret.returncode != 0 and capture:
        message = f"Error running command:\n\n{cmd_str}\n\n"
        message += f"Subprocess exited with status {ret.returncode}"
        if ret.stdout is not None:
            message += f"\n\nProcess log (stdout and stderr):\n\n"
            message += ret.stdout
        error = subprocess.SubprocessError(message)
        error.ret = ret  # type: ignore[attr-defined]
        error.command = cmd_str  # type: ignore[attr-defined]
        raise error
    elif ret.returncode != 0:
        sys.exit(ret.returncode)
    return ret




def print_run_help(project_dir: Path, subcommand: Optional[str] = None) -> None:
    """打印帮助信息

    Args:
        project_dir (Path): project.yml文件目录
        subcommand (Optional[str], optional): 子命令. Defaults to None.
    """
    config = load_project_config(project_dir)
    config_commands = config.get("commands", [])
    commands = {cmd["name"]: cmd for cmd in config_commands}
    workflows = config.get("workflows", {})
    if subcommand:
        validate_subcommand(list(commands.keys()), list(workflows.keys()), subcommand)
        command_table = Table()
        command_table.add_column('command', style='cyan')
        command_table.add_column('describe', style='yellow')
        if subcommand in commands:
            help_text = commands[subcommand].get("help")
            command_table.add_row(subcommand, help_text)
            console.print(command_table)
        elif subcommand in workflows:
            flow_table = Table()
            flow_table.add_column('workflow', style='cyan')
            flow_table.add_column('Usage', style='green')
            flow_table.add_column('steps', style='red')
            steps = workflows[subcommand]
            step_flow = ' -> '.join(steps)
            flow_table.add_row(subcommand, f'project run {subcommand}', f"{step_flow}")
            console.print(flow_table)
            steps = list(set(steps))
            for step in steps:
                help_text = commands[step].get("help")
                command_table.add_row(step, help_text)
            console.print(command_table)
    else:
        print("")
        title = config.get("title")
        if config_commands:
            cmd_table = Table(title=f'[bold red]commands in {PROJECT_FILE}')
            cmd_table.add_column(header='command', style='cyan')
            cmd_table.add_column(header='Usage', style='green')
            cmd_table.add_column(header='describe', style='red')
            for cmd in config_commands:
                cmd_table.add_row(cmd['name'], f"project run {cmd['name']}", cmd['help'])
            console.print(cmd_table, justify='left')
        if workflows:
            flow_table = Table(title=f"[bold red]workflows in {PROJECT_FILE}")
            flow_table.add_column(header='workflow', style='cyan')
            flow_table.add_column(header='Usage', style='green')
            flow_table.add_column(header='steps')
            for name, steps in workflows.items():
                flow_table.add_row(name, f'project run {name}'," -> ".join(steps))
            console.print(flow_table, justify='left')
            
            
def get_lock_entry(project_dir: Path, command: Dict[str, Any]) -> Dict[str, Any]:
    """Get a lockfile entry for a given command. An entry includes the command,
    the script (command steps) and a list of dependencies and outputs with
    their paths and file hashes, if available. The format is based on the
    dvc.lock files, to keep things consistent.
    project_dir (Path): The current project directory.
    command (Dict[str, Any]): The command, as defined in the project.yml.
    RETURNS (Dict[str, Any]): The lockfile entry.
    """
    deps = get_fileinfo(project_dir, command.get("deps", []))
    outs = get_fileinfo(project_dir, command.get("outputs", []))
    outs_nc = get_fileinfo(project_dir, command.get("outputs_no_cache", []))
    return {
        "cmd": f"project run {command['name']}",
        "script": command["script"],
        "deps": deps,
        "outs": [*outs, *outs_nc]
    }
    
def get_fileinfo(project_dir: Path, paths: List[str]) -> List[Dict[str, Optional[str]]]:
    """Generate the file information for a list of paths (dependencies, outputs).
    Includes the file path and the file's checksum.
    project_dir (Path): The current project directory.
    paths (List[str]): The file paths.
    RETURNS (List[Dict[str, str]]): The lockfile entry for a file.
    """
    data = []
    for path in paths:
        file_path = project_dir / path
        md5 = get_checksum(file_path) if file_path.exists() else None
        data.append({"path": path, "md5": md5})
    return data
    
def validate_subcommand(commands: Sequence[str], workflows: Sequence[str], subcommand: str) -> None:
    """Check that a subcommand is valid and defined. Raises an error otherwise.
    commands (Sequence[str]): The available commands.
    subcommand (str): The subcommand.
    """
    if not commands and not workflows:
        msg.fail(f"No commands or workflows defined in {PROJECT_FILE}", exits=1)
    if subcommand not in commands and subcommand not in workflows:
        help_msg = []
        if subcommand in ["assets", "asset"]:
            help_msg.append("Did you mean to run: python -m spacy project assets?")
        if commands:
            help_msg.append(f"Available commands: {', '.join(commands)}")
        if workflows:
            help_msg.append(f"Available workflows: {', '.join(workflows)}")
        msg.fail(
            f"Can't find command or workflow '{subcommand}' in {PROJECT_FILE}",
            ". ".join(help_msg),
            exits=1,
        )
        
def update_lockfile(project_dir: Path, command: Dict[str, Any]) -> None:
    """Update the lockfile after running a command. Will create a lockfile if
    it doesn't yet exist and will add an entry for the current command, its
    script and dependencies/outputs.
    project_dir (Path): The current project directory.
    command (Dict[str, Any]): The command, as defined in the project.yml.
    """
    lock_path = project_dir / PROJECT_LOCK
    if not lock_path.exists():
        srsly.write_yaml(lock_path, {})
        data = {}
    else:
        data = srsly.read_yaml(lock_path)
    data[command["name"]] = get_lock_entry(project_dir, command)
    srsly.write_yaml(lock_path, data)
    
def get_checksum(path: Union[Path, str]) -> str:
    """Get the checksum for a file or directory given its file path. If a
    directory path is provided, this uses all files in that directory.
    path (Union[Path, str]): The file or directory path.
    RETURNS (str): The checksum.
    """
    path = Path(path)
    if not (path.is_file() or path.is_dir()):
        msg.fail(f"Can't get checksum for {path}: not a file or directory", exits=1)
    if path.is_file():
        return hashlib.md5(Path(path).read_bytes()).hexdigest()
    else:
        # TODO: this is currently pretty slow
        dir_checksum = hashlib.md5()
        for sub_file in sorted(fp for fp in path.rglob("*") if fp.is_file()):
            dir_checksum.update(sub_file.read_bytes())
        return dir_checksum.hexdigest()
    
def _check_requirements(requirements: List[str]) -> Tuple[bool, bool]:
    """Checks whether requirements are installed and free of version conflicts.
    requirements (List[str]): List of requirements.
    RETURNS (Tuple[bool, bool]): Whether (1) any packages couldn't be imported, (2) any packages with version conflicts
        exist.
    """

    failed_pkgs_msgs: List[str] = []
    conflicting_pkgs_msgs: List[str] = []

    for req in requirements:
        try:
            pkg_resources.require(req)
        except pkg_resources.DistributionNotFound as dnf:
            failed_pkgs_msgs.append(dnf.report())
        except pkg_resources.VersionConflict as vc:
            conflicting_pkgs_msgs.append(vc.report())

    if len(failed_pkgs_msgs) or len(conflicting_pkgs_msgs):
        msg.warn(
            title="Missing requirements or requirement conflicts detected. Make sure your Python environment is set up "
            "correctly and you installed all requirements specified in your project's requirements.txt: "
        )
        for pgk_msg in failed_pkgs_msgs + conflicting_pkgs_msgs:
            msg.text(pgk_msg)

    return len(failed_pkgs_msgs) > 0, len(conflicting_pkgs_msgs) > 0


def get_hash(data, exclude: Iterable[str] = tuple()) -> str:
    """Get the hash for a JSON-serializable object.
    data: The data to hash.
    exclude (Iterable[str]): Top-level keys to exclude if data is a dict.
    RETURNS (str): The hash.
    """
    if isinstance(data, dict):
        data = {k: v for k, v in data.items() if k not in exclude}
    data_str = srsly.json_dumps(data, sort_keys=True).encode("utf8")
    return hashlib.md5(data_str).hexdigest()


def check_rerun(project_dir: Path, command: Dict[str, Any]) -> bool:
    """Check if a command should be rerun because its settings or inputs/outputs
    changed.
    project_dir (Path): The current project directory.
    command (Dict[str, Any]): The command, as defined in the project.yml.
    strict_version (bool):
    RETURNS (bool): Whether to re-run the command.
    """
    # Always rerun if no-skip is set
    if command.get("no_skip", False):
        return True
    lock_path = project_dir / PROJECT_LOCK
    if not lock_path.exists():  # We don't have a lockfile, run command
        return True
    data = srsly.read_yaml(lock_path)
    if command["name"] not in data:  # We don't have info about this command
        return True
    entry = data[command["name"]]
    # Always run commands with no outputs (otherwise they'd always be skipped)
    if not entry.get("outs", []):
        return True
    # Always rerun if spaCy version or commit hash changed
    # If the entry in the lockfile matches the lockfile entry that would be
    # generated from the current command, we don't rerun because it means that
    # all inputs/outputs, hashes and scripts are the same and nothing changed
    lock_entry = get_lock_entry(project_dir, command)
    return get_hash(lock_entry) != get_hash(entry)


def run_commands(commands: Iterable[str] = SimpleFrozenList(),
                 silent: bool = False,
                 dry: bool = False,
                 capture: bool = False) -> None:
    """Run a sequence of commands in a subprocess, in order.
    commands (List[str]): The string commands.
    silent (bool): Don't print the commands.
    dry (bool): Perform a dry run and don't execut anything.
    capture (bool): Whether to capture the output and errors of individual commands.
        If False, the stdout and stderr will not be redirected, and if there's an error,
        sys.exit will be called with the return code. You should use capture=False
        when you want to turn over execution to the command, and capture=True
        when you want to run the command more like a function.
    """
    for c in commands:
        command = split_command(c)
        # Not sure if this is needed or a good idea. Motivation: users may often
        # use commands in their config that reference "python" and we want to
        # make sure that it's always executing the same Python that spaCy is
        # executed with and the pip in the same env, not some other Python/pip.
        # Also ensures cross-compatibility if user 1 writes "python3" (because
        # that's how it's set up on their system), and user 2 without the
        # shortcut tries to re-run the command.
        if len(command) and command[0] in ("python", "python3"):
            command[0] = sys.executable
        elif len(command) and command[0] in ("pip", "pip3"):
            command = [sys.executable, "-m", "pip", *command[1:]]
        if not silent:
            msg.info(title=f"Running command: {join_command(command)}")
        if not dry:
            run_command(command, capture=capture)
            
@contextmanager
def working_dir(path: Union[str, Path]) -> Iterator[Path]:
    """Change current working directory and returns to previous on exit.
    path (str / Path): The directory to navigate to.
    YIELDS (Path): The absolute path to the current working directory. This
        should be used if the block needs to perform actions within the working
        directory, to prevent mismatches with relative paths.
    """
    prev_cwd = Path.cwd()
    current = Path(path).resolve()
    os.chdir(str(current))
    try:
        yield current
    finally:
        os.chdir(str(prev_cwd))


def project_run(project_dir: Path,
                subcommand: str,
                *,
                overrides: Dict[str, Any] = SimpleFrozenDict(),
                force: bool = False,
                dry: bool = False,
                capture: bool = False) -> None:
    """Run a named script defined in the project.yml. If the script is part
    of the default pipeline (defined in the "run" section), DVC is used to
    execute the command, so it can determine whether to rerun it. It then
    calls into "exec" to execute it.
    project_dir (Path): Path to project directory.
    subcommand (str): Name of command to run.
    overrides (Dict[str, Any]): Optional config overrides.
    force (bool): Force re-running, even if nothing changed.
    dry (bool): Perform a dry run and don't execute commands.
    capture (bool): Whether to capture the output and errors of individual commands.
        If False, the stdout and stderr will not be redirected, and if there's an error,
        sys.exit will be called with the return code. You should use capture=False
        when you want to turn over execution to the command, and capture=True
        when you want to run the command more like a function.
    """
    config = load_project_config(project_dir, overrides=overrides)
    commands = {cmd["name"]: cmd for cmd in config.get("commands", [])}
    workflows = config.get("workflows", {})
    validate_subcommand(list(commands.keys()), list(workflows.keys()), subcommand)

    req_path = project_dir / "requirements.txt"
    if config.get("check_requirements", True) and os.path.exists(req_path):
        with req_path.open() as requirements_file:
            _check_requirements([req.replace("\n", "") for req in requirements_file])

    if subcommand in workflows:
        msg.info(f"Running workflow '{subcommand}'")
        for cmd in workflows[subcommand]:
            project_run(
                project_dir,
                cmd,
                overrides=overrides,
                force=force,
                dry=dry,
                capture=capture,
            )
    else:
        cmd = commands[subcommand]
        for dep in cmd.get("deps", []):
            if not (project_dir / dep).exists():
                err = f"Missing dependency specified by command '{subcommand}': {dep}"
                err_help = "Maybe you forgot to run the 'project assets' command or a previous step?"
                err_kwargs = {"exits": 1} if not dry else {}
                msg.fail(err, err_help, **err_kwargs)
        with working_dir(project_dir) as current_dir:
            console.rule(title=subcommand)
            rerun = check_rerun(current_dir, cmd)
            if not rerun and not force:
                msg.info(f"Skipping '{cmd['name']}': nothing changed")
            else:
                run_commands(cmd["script"], dry=dry, capture=capture)
                if not dry:
                    update_lockfile(current_dir, cmd)
                    
def parse_config_overrides(args: List[str], 
                           env_var: Optional[str] = ENV_VARS.CONFIG_OVERRIDES) -> Dict[str, Any]:
    """Generate a dictionary of config overrides based on the extra arguments
    provided on the CLI, e.g. --training.batch_size to override
    "training.batch_size". Arguments without a "." are considered invalid,
    since the config only allows top-level sections to exist.
    env_vars (Optional[str]): Optional environment variable to read from.
    RETURNS (Dict[str, Any]): The parsed dict, keyed by nested config setting.
    """
    env_string = os.environ.get(env_var, "") if env_var else ""
    env_overrides = _parse_overrides(split_arg_string(env_string))
    cli_overrides = _parse_overrides(args, is_cli=True)
    if cli_overrides:
        keys = [k for k in cli_overrides if k not in env_overrides]
        console.log(f"Config overrides from CLI: {keys}")
    if env_overrides:
        console.log(f"Config overrides from env variables: {list(env_overrides)}")
    return {**cli_overrides, **env_overrides}


def _parse_overrides(args: List[str], is_cli: bool = False) -> Dict[str, Any]:
    result = {}
    while args:
        opt = args.pop(0)
        err = f"Invalid config override '{opt}'"
        if opt.startswith("--"):  # new argument
            orig_opt = opt
            opt = opt.replace("--", "")
            if "." not in opt:
                if is_cli:
                    raise NoSuchOption(orig_opt)
                else:
                    msg.fail(f"{err}: can't override top-level sections", exits=1)
            if "=" in opt:  # we have --opt=value
                opt, value = opt.split("=", 1)
                opt = opt.replace("-", "_")
            else:
                if not args or args[0].startswith("--"):  # flag with no value
                    value = "true"
                else:
                    value = args.pop(0)
            result[opt] = _parse_override(value)
        else:
            msg.fail(f"{err}: name should start with --", exits=1)
    return result


def _parse_override(value: Any) -> Any:
    # Just like we do in the config, we're calling json.loads on the
    # values. But since they come from the CLI, it'd be unintuitive to
    # explicitly mark strings with escaped quotes. So we're working
    # around that here by falling back to a string if parsing fails.
    # TODO: improve logic to handle simple types like list of strings?
    try:
        return srsly.json_loads(value)
    except ValueError:
        return str(value)
    
    
    
from pathlib import Path
from wasabi import msg, MarkdownRenderer



INTRO_PROJECT = f"""The [`{PROJECT_FILE}`]({PROJECT_FILE}) defines the data assets required by the
project, as well as the available commands and workflows. """
INTRO_COMMANDS = f"""The following commands are defined by the project. They
can be executed using `project run [name]`.Commands are only re-run if their inputs have changed."""
INTRO_WORKFLOWS = f"""The following workflows are defined by the project. They
can be executed using `project run [name]`
and will run the specified commands in order. Commands are only re-run if their
inputs have changed."""
INTRO_ASSETS = f"""The following assets are defined by the project. They can
be fetched by running `project assets` in the project directory."""
# These markers are added to the Markdown and can be used to update the file in
# place if it already exists. Only the auto-generated part will be replaced.
MARKER_START = "<!-- PROJECT: AUTO-GENERATED DOCS START (do not remove) -->"
MARKER_END = "<!-- PROJECT: AUTO-GENERATED DOCS END (do not remove) -->"
# If this marker is used in an existing README, it's ignored and not replaced
MARKER_IGNORE = "<!-- PROJECT: IGNORE -->"

## 中文介绍
INTRO_PROJECT_ZH = f"""[`{PROJECT_FILE}`]({PROJECT_FILE})定义了项目所有的命令以及由命令组成的流程. """
INTRO_COMMANDS_ZH = f"""以下是项目中的命令. 它们都可以通过`project run [name]`来运行."""
INTRO_WORKFLOWS_ZH = f"""以下是项目中的流程. 它们都可以通过 `project run [name]`来运行,并且会按照顺序依次运行命令."""
INTRO_ASSETS_ZH = f"""以下是项目中定义的数据. 他们都可以通过 `project assets`来下载."""


from enum import Enum
class AvailableLanguages(str, Enum):
    zh = "zh"
    en = "en"



def project_document(project_dir: Path,
                     output_file: Path, 
                     *, 
                     no_emoji: bool = False,
                     lang: AvailableLanguages = AvailableLanguages.zh) -> None:
    is_stdout = str(output_file) == "-"
    config = load_project_config(project_dir)
    if lang == AvailableLanguages.en:
        md = MarkdownRenderer(no_emoji=no_emoji)
        md.add(MARKER_START)
        title = config.get("title")
        description = config.get("description")
        md.add(md.title(1, f"Project{f': {title}' if title else ''}", "🪐"))
        if description:
            md.add(description)
        md.add(md.title(2, PROJECT_FILE, "📋"))
        md.add(INTRO_PROJECT)
        # Commands
        cmds = config.get("commands", [])
        data = [(md.code(cmd["name"]), cmd.get("help", "")) for cmd in cmds]
        if data:
            md.add(md.title(3, "Commands", "⏯"))
            md.add(INTRO_COMMANDS)
            md.add(md.table(data, ["Command", "Description"]))
        # Workflows
        wfs = config.get("workflows", {}).items()
        data = [(md.code(n), " &rarr; ".join(md.code(w) for w in stp)) for n, stp in wfs]
        if data:
            md.add(md.title(3, "Workflows", "⏭"))
            md.add(INTRO_WORKFLOWS)
            md.add(md.table(data, ["Workflow", "Steps"]))
        # Assets
        assets = config.get("assets", [])
        data = []
        for a in assets:
            source = "Git" if a.get("git") else "URL" if a.get("url") else "Local"
            dest_path = a["dest"]
            dest = md.code(dest_path)
            if source == "Local":
                # Only link assets if they're in the repo
                with working_dir(project_dir) as p:
                    if (p / dest_path).exists():
                        dest = md.link(dest, dest_path)
            data.append((dest, source, a.get("description", "")))
        if data:
            md.add(md.title(3, "Assets", "🗂"))
            md.add(INTRO_ASSETS)
            md.add(md.table(data, ["File", "Source", "Description"]))
        md.add(MARKER_END)
        # Output result
        if is_stdout:
            print(md.text)
        else:
            content = md.text
            if output_file.exists():
                with output_file.open("r", encoding="utf8") as f:
                    existing = f.read()
                if MARKER_IGNORE in existing:
                    msg.warn("Found ignore marker in existing file: skipping", output_file)
                    return
                if MARKER_START in existing and MARKER_END in existing:
                    msg.info("Found existing file: only replacing auto-generated docs")
                    before = existing.split(MARKER_START)[0]
                    after = existing.split(MARKER_END)[1]
                    content = f"{before}{content}{after}"
                else:
                    msg.warn("Replacing existing file")
            with output_file.open("w", encoding="utf8") as f:
                f.write(content)
            msg.good("Saved project documentation", output_file)
    elif lang == AvailableLanguages.zh:
        md = MarkdownRenderer(no_emoji=no_emoji)
        md.add(MARKER_START)
        title = config.get("title")
        description = config.get("description")
        md.add(md.title(1, f"项目{f': {title}' if title else ''}", "🪐"))
        if description:
            md.add(description)
        md.add(md.title(2, PROJECT_FILE, "📋"))
        md.add(INTRO_PROJECT_ZH)
        # Commands
        cmds = config.get("commands", [])
        data = [(md.code(cmd["name"]), cmd.get("help", "")) for cmd in cmds]
        if data:
            md.add(md.title(3, "命令", "⏯"))
            md.add(INTRO_COMMANDS_ZH)
            md.add(md.table(data, ["命令", "描述"]))
        # Workflows
        wfs = config.get("workflows", {}).items()
        data = [(md.code(n), " &rarr; ".join(md.code(w) for w in stp)) for n, stp in wfs]
        if data:
            md.add(md.title(3, "流程", "⏭"))
            md.add(INTRO_WORKFLOWS_ZH)
            md.add(md.table(data, ["流程", "步骤"]))
        # Assets
        assets = config.get("assets", [])
        data = []
        for a in assets:
            source = "Git" if a.get("git") else "URL" if a.get("url") else "Local"
            dest_path = a["dest"]
            dest = md.code(dest_path)
            if source == "Local":
                # Only link assets if they're in the repo
                with working_dir(project_dir) as p:
                    if (p / dest_path).exists():
                        dest = md.link(dest, dest_path)
            data.append((dest, source, a.get("description", "")))
        if data:
            md.add(md.title(3, "Assets", "🗂"))
            md.add(INTRO_ASSETS_ZH)
            md.add(md.table(data, ["File", "Source", "Description"]))
        md.add(MARKER_END)
        # Output result
        if is_stdout:
            print(md.text)
        else:
            content = md.text
            if output_file.exists():
                with output_file.open("r", encoding="utf8") as f:
                    existing = f.read()
                if MARKER_IGNORE in existing:
                    msg.warn("Found ignore marker in existing file: skipping", output_file)
                    return
                if MARKER_START in existing and MARKER_END in existing:
                    msg.info("Found existing file: only replacing auto-generated docs")
                    before = existing.split(MARKER_START)[0]
                    after = existing.split(MARKER_END)[1]
                    content = f"{before}{content}{after}"
                else:
                    msg.warn("Replacing existing file")
            with output_file.open("w", encoding="utf8") as f:
                f.write(content)
            msg.good("Saved project documentation", output_file)