"""Process representation for distributed simulation."""
from dataclasses import dataclass
from typing import Optional


@dataclass
class Process:
    """Represents a simulated node participating in mutual exclusion."""

    process_id: int
    active: bool = True
    requesting: bool = False
    in_critical_section: bool = False
    request_time: Optional[float] = None
    entry_time: Optional[float] = None
    exit_time: Optional[float] = None

    def __repr__(self) -> str:
        return (
            f"Process(id={self.process_id}, "
            f"active={self.active}, "
            f"requesting={self.requesting}, "
            f"in_cs={self.in_critical_section})"
        )
