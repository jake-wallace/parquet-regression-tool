from dataclasses import dataclass, field


@dataclass
class SchemaDiff:
    """A structured object to hold the details of a schema comparison."""

    is_identical: bool = True
    added_columns: dict = field(default_factory=dict)  # {col_name: dtype}
    removed_columns: dict = field(default_factory=dict)  # {col_name: dtype}
    type_changes: dict = field(
        default_factory=dict
    )  # {col_name: (before_dtype, after_dtype)}
