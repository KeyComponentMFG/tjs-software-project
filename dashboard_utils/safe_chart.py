"""Safe chart utilities — Phase 2 of sparse-data hardening.

Provides:
  @safe_chart(name)      — decorator that catches exceptions and returns a placeholder
  no_data_figure(...)    — standalone placeholder figure builder
  min_data_guard(...)    — returns True if there's enough data, False otherwise
"""

import functools
import logging

import plotly.graph_objects as go

_logger = logging.getLogger("dashboard.safe_chart")

# Dashboard theme constants
_BG = "#0f0f1a"
_CHART_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(color="#ccc"),
)


def no_data_figure(title="Not enough data", message="", height=300):
    """Return a styled placeholder figure with centered gray text on dark background."""
    display_text = f"<b>{title}</b>"
    if message:
        display_text += f"<br><span style='font-size:12px;color:#888'>{message}</span>"

    fig = go.Figure()
    fig.add_annotation(
        text=display_text,
        xref="paper", yref="paper", x=0.5, y=0.5,
        showarrow=False,
        font=dict(size=16, color="#666"),
        align="center",
    )
    fig.update_layout(
        **_CHART_LAYOUT,
        height=height,
        xaxis=dict(visible=False),
        yaxis=dict(visible=False),
    )
    return fig


def min_data_guard(months_sorted, min_months=2):
    """Return True if there are enough months of data, False otherwise.

    Usage::

        if not min_data_guard(state.months_sorted, min_months=3):
            return no_data_figure("Revenue Projection", "Need 3+ months of data")
    """
    if months_sorted is None:
        return False
    return len(months_sorted) >= min_months


def safe_chart(name):
    """Decorator that wraps a chart-building function with error handling.

    On success the figure is returned normally.
    On exception a placeholder figure is returned and the error is logged.

    Usage::

        @safe_chart("Revenue Projection")
        def build_projection_chart(state):
            ...
            return fig
    """
    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            try:
                return fn(*args, **kwargs)
            except Exception as exc:
                _logger.warning("Chart '%s' failed: %s", name, exc)
                return no_data_figure(
                    f"Not enough data for {name}",
                    f"Error: {exc}",
                )
        return wrapper
    return decorator
