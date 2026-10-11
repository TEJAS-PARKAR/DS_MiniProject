"""Domain models for synthetic blood-bank inventory and allocation requests.

DISCLAIMER:
This module is a distributed-systems research and benchmarking prototype.
It is NOT designed, validated, or certified for clinical decision support,
transfusion therapy, patient matching, or human healthcare applications.
"""
from dataclasses import dataclass, field
from enum import Enum
import time
from typing import Dict, Any, Optional, Set, List


class BloodGroup(str, Enum):
    """Supported synthetic ABO/Rh blood groups."""
    A_POS = "A+"
    A_NEG = "A-"
    B_POS = "B+"
    B_NEG = "B-"
    AB_POS = "AB+"
    AB_NEG = "AB-"
    O_POS = "O+"
    O_NEG = "O-"

    @classmethod
    def from_str(cls, val: str) -> "BloodGroup":
        """Parse string to BloodGroup with explicit validation."""
        if not isinstance(val, str):
            raise ValueError(f"BloodGroup must be a string, got {type(val).__name__}")
        cleaned = val.strip().upper()
        for member in cls:
            if member.value == cleaned:
                return member
        raise ValueError(
            f"Unsupported blood group '{val}'. Supported: {[m.value for m in cls]}"
        )


class ComponentType(str, Enum):
    """Supported synthetic blood component types."""
    RBC = "RBC"                  # Red Blood Cells / Packed RBC
    WHOLE_BLOOD = "WHOLE_BLOOD"  # Whole Blood
    PLATELETS = "PLATELETS"      # Platelet Concentrate
    PLASMA = "PLASMA"            # Fresh Frozen Plasma (FFP)
    CRYO = "CRYO"                # Cryoprecipitate

    @classmethod
    def from_str(cls, val: str) -> "ComponentType":
        """Parse string to ComponentType with explicit validation."""
        if not isinstance(val, str):
            raise ValueError(f"ComponentType must be a string, got {type(val).__name__}")
        cleaned = val.strip().upper()
        for member in cls:
            if member.value == cleaned:
                return member
        raise ValueError(
            f"Unsupported component type '{val}'. Supported: {[m.value for m in cls]}"
        )


class UnitStatus(str, Enum):
    """Lifecycle status of an individual blood unit."""
    AVAILABLE = "AVAILABLE"
    RESERVED = "RESERVED"
    ALLOCATED = "ALLOCATED"


class InvalidStateTransitionError(Exception):
    """Raised when an illegal unit lifecycle state transition is attempted."""
    pass


class InventoryValidationError(Exception):
    """Raised when inventory item or dataset validation fails."""
    pass


@dataclass
class BloodUnit:
    """Represents an indivisible physical blood unit in the synthetic inventory.

    Invariants:
    - `unit_id` must be globally unique across all branches.
    - Transitions must follow:
        AVAILABLE -> RESERVED
        AVAILABLE -> ALLOCATED
        RESERVED -> ALLOCATED
        RESERVED -> AVAILABLE (reservation cancellation)
    - ALLOCATED is a terminal state.
    """
    unit_id: str
    blood_group: BloodGroup
    component_type: ComponentType
    branch_id: str
    status: UnitStatus = UnitStatus.AVAILABLE
    allocation_id: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.unit_id or not isinstance(self.unit_id, str) or not self.unit_id.strip():
            raise InventoryValidationError("Unit ID must be a non-empty string.")
        if not self.branch_id or not isinstance(self.branch_id, str) or not self.branch_id.strip():
            raise InventoryValidationError("Branch ID must be a non-empty string.")

        if isinstance(self.blood_group, str):
            self.blood_group = BloodGroup.from_str(self.blood_group)
        if isinstance(self.component_type, str):
            self.component_type = ComponentType.from_str(self.component_type)
        if isinstance(self.status, str):
            try:
                self.status = UnitStatus(self.status)
            except ValueError:
                raise InventoryValidationError(f"Invalid unit status '{self.status}'.")

    def reserve(self, allocation_id: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        """Transition unit from AVAILABLE to RESERVED."""
        if not allocation_id or not str(allocation_id).strip():
            raise InventoryValidationError("Allocation ID required to reserve unit.")

        if self.status != UnitStatus.AVAILABLE:
            raise InvalidStateTransitionError(
                f"Cannot reserve unit '{self.unit_id}': current status is '{self.status.value}', "
                f"expected '{UnitStatus.AVAILABLE.value}'."
            )

        self.status = UnitStatus.RESERVED
        self.allocation_id = allocation_id
        self.metadata["reserved_at"] = time.time()
        if metadata:
            self.metadata.update(metadata)

    def allocate(self, allocation_id: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        """Transition unit to ALLOCATED from AVAILABLE or RESERVED (for this allocation)."""
        if not allocation_id or not str(allocation_id).strip():
            raise InventoryValidationError("Allocation ID required to allocate unit.")

        if self.status == UnitStatus.ALLOCATED:
            raise InvalidStateTransitionError(
                f"Cannot allocate unit '{self.unit_id}': already ALLOCATED to '{self.allocation_id}'."
            )

        if self.status == UnitStatus.RESERVED and self.allocation_id != allocation_id:
            raise InvalidStateTransitionError(
                f"Cannot allocate unit '{self.unit_id}': RESERVED for '{self.allocation_id}', "
                f"not '{allocation_id}'."
            )

        self.status = UnitStatus.ALLOCATED
        self.allocation_id = allocation_id
        self.metadata["allocated_at"] = time.time()
        if metadata:
            self.metadata.update(metadata)

    def release_reservation(self) -> None:
        """Release reservation, reverting unit back to AVAILABLE."""
        if self.status != UnitStatus.RESERVED:
            raise InvalidStateTransitionError(
                f"Cannot release reservation for unit '{self.unit_id}': "
                f"current status is '{self.status.value}', expected '{UnitStatus.RESERVED.value}'."
            )

        self.status = UnitStatus.AVAILABLE
        self.allocation_id = None
        self.metadata["reservation_released_at"] = time.time()

    def to_dict(self) -> Dict[str, Any]:
        """Convert unit to serializable dictionary."""
        return {
            "unit_id": self.unit_id,
            "blood_group": self.blood_group.value,
            "component_type": self.component_type.value,
            "branch_id": self.branch_id,
            "status": self.status.value,
            "allocation_id": self.allocation_id,
            "metadata": dict(self.metadata),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "BloodUnit":
        """Construct BloodUnit from dictionary."""
        return cls(
            unit_id=data["unit_id"],
            blood_group=BloodGroup.from_str(data["blood_group"]),
            component_type=ComponentType.from_str(data["component_type"]),
            branch_id=data["branch_id"],
            status=UnitStatus(data.get("status", UnitStatus.AVAILABLE.value)),
            allocation_id=data.get("allocation_id"),
            metadata=dict(data.get("metadata", {})),
        )


@dataclass
class AllocationRequest:
    """Validated request from a branch to allocate blood units."""
    request_id: str
    requesting_branch: str
    blood_group: BloodGroup
    component_type: ComponentType
    quantity: int
    metadata: Dict[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)

    def __post_init__(self) -> None:
        if isinstance(self.blood_group, str):
            self.blood_group = BloodGroup.from_str(self.blood_group)
        if isinstance(self.component_type, str):
            self.component_type = ComponentType.from_str(self.component_type)
        self.validate()

    def validate(self, valid_branches: Optional[Set[str]] = None) -> None:
        """Validate request fields against business constraints."""
        if not self.request_id or not isinstance(self.request_id, str) or not self.request_id.strip():
            raise ValueError("Request ID must be a non-empty string.")

        if not self.requesting_branch or not isinstance(self.requesting_branch, str) or not self.requesting_branch.strip():
            raise ValueError("Requesting branch must be a non-empty string.")

        if valid_branches is not None and self.requesting_branch not in valid_branches:
            raise ValueError(
                f"Unknown requesting branch '{self.requesting_branch}'. "
                f"Valid branches: {sorted(valid_branches)}"
            )

        if not isinstance(self.quantity, int) or self.quantity <= 0:
            raise ValueError(
                f"Requested quantity must be a strictly positive integer, got {self.quantity}"
            )

        if not isinstance(self.blood_group, BloodGroup):
            raise ValueError(f"Invalid blood group instance: {self.blood_group}")

        if not isinstance(self.component_type, ComponentType):
            raise ValueError(f"Invalid component type instance: {self.component_type}")

    def to_dict(self) -> Dict[str, Any]:
        """Convert request to dictionary."""
        return {
            "request_id": self.request_id,
            "requesting_branch": self.requesting_branch,
            "blood_group": self.blood_group.value,
            "component_type": self.component_type.value,
            "quantity": self.quantity,
            "metadata": dict(self.metadata),
            "timestamp": self.timestamp,
        }


class AllocationStatus(str, Enum):
    """Outcome status of an allocation operation."""
    SUCCESS = "SUCCESS"
    INSUFFICIENT_STOCK = "INSUFFICIENT_STOCK"
    VALIDATION_FAILED = "VALIDATION_FAILED"
    MUTUAL_EXCLUSION_FAILED = "MUTUAL_EXCLUSION_FAILED"
    FAILED_CLOSED = "FAILED_CLOSED"
    ERROR = "ERROR"


@dataclass
class AllocationResult:
    """Result of an allocation operation under mutual exclusion."""
    request_id: str
    requesting_branch: str
    status: AllocationStatus
    allocated_units: List[str] = field(default_factory=list)
    allocated_units_details: List[Dict[str, Any]] = field(default_factory=list)
    error_message: Optional[str] = None
    algorithm: Optional[str] = None
    duration_sec: float = 0.0
    cs_entered: bool = False

    def to_dict(self) -> Dict[str, Any]:
        """Convert result to dictionary."""
        return {
            "request_id": self.request_id,
            "requesting_branch": self.requesting_branch,
            "status": self.status.value,
            "allocated_units": list(self.allocated_units),
            "allocated_units_details": list(self.allocated_units_details),
            "error_message": self.error_message,
            "algorithm": self.algorithm,
            "duration_sec": self.duration_sec,
            "cs_entered": self.cs_entered,
        }
