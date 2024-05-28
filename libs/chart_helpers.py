from typing import Iterable, List, Tuple
from snowflake.snowpark.table import Table


def mk_labels(_iter: Iterable[str]) -> Tuple[str, ...]:
    """Produces the labels configuration for plotly.graph_objects.Sankey"""
    first_label = "Original data"
    return (first_label,) + tuple(map(lambda _: f"Filter: '{_}'", _iter)) + ("Result",)


def mk_links(table_sequence: List[Table]) -> dict:
    """Produces the links configuration for plotly.graph_objects.Sankey"""
    return dict(
        source=list(range(len(table_sequence))),
        target=list(range(1, len(table_sequence) + 1)),
        value=[_.count() for _ in table_sequence],
    )
