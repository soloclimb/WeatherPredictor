from typing import Any, Dict, Mapping
from aws_cdk import (Environment, IStackSynthesizer, PermissionsBoundary, Stack)
from constructs import Construct

class ETLStack(Stack):
    def __init__(self, scope: Construct | None = None, id: str | None = None, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)