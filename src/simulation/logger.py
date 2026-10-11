"""Event logging utilities for simulation execution."""
from typing import Any, Dict, List, Optional
import pandas as pd


class EventLogger:
    """Collects structured events for auditing and statistical analysis."""

    def __init__(self) -> None:
        self.events: List[Dict[str, Any]] = []

    def log(
        self,
        time: float,
        process_id: int,
        event_type: str,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Append an event record."""
        self.events.append({
            "time": float(time),
            "process_id": process_id,
            "event_type": event_type,
            "details": details,
        })

    def to_dataframe(self) -> pd.DataFrame:
        """Export events as a pandas DataFrame."""
        return pd.DataFrame(self.events)

    def show(self, n: int = 10) -> pd.DataFrame:
        """Return head of event DataFrame."""
        return self.to_dataframe().head(n)
