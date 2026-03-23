"""
Callback error guard decorator for Dash callbacks.

Wraps callbacks with standardized error handling, logging, and tracking.

Usage:
    from etsy_dashboard.callback_guard import guard_callback

    @app.callback(Output("my-div", "children"), Input("my-input", "value"))
    @guard_callback(n_outputs=1)
    def my_callback(value):
        ...
"""

import functools
import traceback
import datetime

import dash
from dash import html

from etsy_dashboard.logging_config import get_logger

_logger = get_logger("callbacks")

# ── Error tracking ───────────────────────────────────────────────────────────
# Maps callback_name -> {"count": int, "last_error": str, "last_time": str}
CALLBACK_ERRORS: dict = {}


def get_error_summary() -> dict:
    """Return a copy of the current error state for all tracked callbacks."""
    return dict(CALLBACK_ERRORS)


def _truncate(value, max_len=100) -> str:
    """Truncate a value's string representation to max_len characters."""
    s = str(value)
    if len(s) > max_len:
        return s[:max_len] + "..."
    return s


def guard_callback(n_outputs: int = 1):
    """Decorator factory for guarding Dash callbacks.

    Parameters
    ----------
    n_outputs : int
        Number of Output components the callback returns.
        If 1, returns an error Div on failure.
        If > 1, returns the error Div as the first element and
        dash.no_update for the remaining outputs.
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except dash.exceptions.PreventUpdate:
                raise  # let PreventUpdate propagate normally
            except Exception:
                cb_name = func.__name__
                tb = traceback.format_exc()

                # Build a readable summary of argument values
                arg_strs = [_truncate(a) for a in args]
                kwarg_strs = [f"{k}={_truncate(v)}" for k, v in kwargs.items()]
                all_args = ", ".join(arg_strs + kwarg_strs)

                _logger.error(
                    "Exception in callback '%s'\nArgs: %s\n%s",
                    cb_name, all_args, tb
                )

                # Track the error
                now_str = datetime.datetime.now().isoformat()
                if cb_name not in CALLBACK_ERRORS:
                    CALLBACK_ERRORS[cb_name] = {
                        "count": 0,
                        "last_error": "",
                        "last_time": "",
                    }
                CALLBACK_ERRORS[cb_name]["count"] += 1
                CALLBACK_ERRORS[cb_name]["last_error"] = tb
                CALLBACK_ERRORS[cb_name]["last_time"] = now_str

                # Build an error div for the user
                error_div = html.Div([
                    html.Div(
                        f"Error in {cb_name}:",
                        style={
                            "color": "#e74c3c",
                            "fontWeight": "bold",
                            "padding": "20px",
                        },
                    ),
                    html.Pre(
                        tb,
                        style={
                            "color": "#f39c12",
                            "padding": "20px",
                            "whiteSpace": "pre-wrap",
                            "fontSize": "12px",
                        },
                    ),
                ])

                if n_outputs == 1:
                    return error_div
                else:
                    return tuple(
                        [error_div] + [dash.no_update] * (n_outputs - 1)
                    )

        return wrapper

    return decorator
