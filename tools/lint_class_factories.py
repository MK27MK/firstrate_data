#!/usr/bin/env python3
"""Flag module-level functions that return a class defined in the same module.

Such a function is a factory for that class, and the class is the only place
that knows how to make one of itself. Written as a module-level function, the
constructor lives away from the type it constructs: readers of the class do not
see it, and every caller has to import two names to make one object. Written as
a classmethod, the factory sits with the invariants it satisfies and is reached
through the class it produces.

    BAD:  def dataset_of(asset_type: AssetType) -> Dataset: ...
    GOOD: class Dataset(StrEnum):
              @classmethod
              def of(cls, asset_type: AssetType) -> Dataset: ...

Only classes defined in the file under check are reported: a foreign class
cannot be given a classmethod from here.

Usage:
    python tools/lint_class_factories.py src/firstrate_data/domain.py
"""

from __future__ import annotations

import argparse
import ast
import sys
from pathlib import Path
from typing import NamedTuple

RULE_CODE = "FCT001"
SUPPRESSION = f"noqa: {RULE_CODE}"


class Violation(NamedTuple):
    """One module-level factory function that belongs on its return class."""

    path: Path
    line: int
    column: int
    function_name: str
    class_name: str

    def format_as_alert(self) -> str:
        return (
            f"{self.path}:{self.line}:{self.column}: {RULE_CODE} "
            f"`{self.function_name}` returns `{self.class_name}`, a class defined in "
            f"this module; make it a classmethod of `{self.class_name}`"
        )


def name_returned_class(annotation: ast.expr | None) -> str | None:
    """The class name a return annotation names, unwrapping an optional.

    `Dataset`, `Dataset | None` and `Optional[Dataset]` all name `Dataset`. A
    container such as `list[Dataset]` names no single class and yields None,
    since a function producing many of a thing is not that thing's constructor.
    """
    match annotation:
        case ast.Name(id=name):
            return name
        case ast.Constant(value=str() as name):  # a quoted forward reference
            return name.strip()
        case ast.BinOp(op=ast.BitOr(), left=left, right=right):
            sides = [name_returned_class(side) for side in (left, right)]
            named = [side for side in sides if side not in (None, "None")]
            return named[0] if len(named) == 1 else None
        case ast.Subscript(value=ast.Name(id="Optional"), slice=inner):
            return name_returned_class(inner)
        case _:
            return None


def find_factory_functions(tree: ast.Module, path: Path, source: str) -> list[Violation]:
    """Every module-level function returning a class the module itself defines."""
    lines = source.splitlines()
    class_names = {
        node.name for node in tree.body if isinstance(node, ast.ClassDef)
    }
    violations = []
    for node in tree.body:
        if not isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            continue
        returned_class = name_returned_class(node.returns)
        if returned_class not in class_names:
            continue
        if SUPPRESSION in lines[node.lineno - 1]:
            continue
        violations.append(
            Violation(path, node.lineno, node.col_offset + 1, node.name, returned_class)
        )
    return violations


def check_file(path: Path) -> list[Violation]:
    source = path.read_text(encoding="utf-8")
    return find_factory_functions(ast.parse(source, str(path)), path, source)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("paths", nargs="+", type=Path, help="Python files to check.")
    parser.add_argument(
        "--exit-zero",
        action="store_true",
        help="Report the alerts but succeed anyway.",
    )
    arguments = parser.parse_args(argv)

    violations = [
        violation for path in arguments.paths for violation in check_file(path)
    ]
    for violation in violations:
        print(violation.format_as_alert(), file=sys.stderr)
    if violations:
        print(
            f"Found {len(violations)} {RULE_CODE} alert(s). "
            f"Silence one with a `# {SUPPRESSION}` comment on its `def` line.",
            file=sys.stderr,
        )
    return 1 if violations and not arguments.exit_zero else 0


if __name__ == "__main__":
    raise SystemExit(main())
