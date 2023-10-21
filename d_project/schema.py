from pydantic import BaseModel, ValidationError, StrictStr, Field
from typing import Type, Dict, List, Any, Optional
from collections import defaultdict


def validate(schema: Type[BaseModel], obj: Dict[str, Any]) -> List[str]:
    """Validate data against a given pydantic schema.
    obj (Dict[str, Any]): JSON-serializable data to validate.
    schema (pydantic.BaseModel): The schema to validate against.
    RETURNS (List[str]): A list of error messages, if available.
    """
    try:
        schema(**obj)
        return []
    except ValidationError as e:
        errors = e.errors()
        data = defaultdict(list)
        for error in errors:
            err_loc = " -> ".join([str(p) for p in error.get("loc", [])])
            data[err_loc].append(error.get("msg"))
        return [f"[{loc}] {', '.join(msg)}" for loc, msg in data.items()] 
    


class ProjectConfigCommand(BaseModel):
    # fmt: off
    name: StrictStr = Field(..., title="Name of command")
    help: Optional[StrictStr] = Field(None, title="Command description")
    script: List[StrictStr] = Field([], title="List of CLI commands to run, in order")
    deps: List[StrictStr] = Field([], title="File dependencies required by this command")
    outputs: List[StrictStr] = Field([], title="Outputs produced by this command")
    outputs_no_cache: List[StrictStr] = Field([], title="Outputs not tracked by DVC (DVC only)")
    no_skip: bool = Field(False, title="Never skip this command, even if nothing changed")
    # fmt: on

    class Config:
        title = "A single named command specified in a project config"
        extra = "forbid"
        
    
class ProjectConfigSchema(BaseModel):
    # fmt: off
    vars: Dict[StrictStr, Any] = Field({}, title="Optional variables to substitute in commands")
    env: Dict[StrictStr, Any] = Field({}, title="Optional variable names to substitute in commands, mapped to environment variable names")
    workflows: Dict[StrictStr, List[StrictStr]] = Field({}, title="Named workflows, mapped to list of project commands to run in order")
    commands: List[ProjectConfigCommand] = Field([], title="Project command shortucts")
    title: Optional[str] = Field(None, title="Project title")
    description: Optional[str] = Field(None, title="Project description")
    # fmt: on

    class Config:
        title = "Schema for project configuration file"