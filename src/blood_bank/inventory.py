"""Authoritative in-memory inventory store for synthetic blood units.

CONSISTENCY MODEL (OPTION A):
This class represents the authoritative blood-bank inventory state.
In Phase 6, access to this authoritative inventory is strictly coordinated by
distributed mutual exclusion protocols (Ricart-Agrawala, Suzuki-Kasami, Raymond).
The inventory maintains full transactional integrity locally, enforcing that
each physical unit has a globally unique ID and is never double-allocated.
"""
import threading
import time
from typing import Dict, List, Optional, Any, Set
from src.blood_bank.models import (
    BloodUnit,
    BloodGroup,
    ComponentType,
    UnitStatus,
    InvalidStateTransitionError,
    InventoryValidationError,
)
from src.blood_bank.compatibility import SyntheticCompatibilityPolicy


class BloodBankInventory:
    """Authoritative inventory container managing synthetic blood units."""

    def __init__(self, units: Optional[List[BloodUnit]] = None) -> None:
        self._units: Dict[str, BloodUnit] = {}
        self._lock = threading.RLock()

        if units:
            for u in units:
                self.add_unit(u)

    def add_unit(self, unit: BloodUnit) -> None:
        """Register a new unit into inventory, enforcing global uniqueness of unit_id."""
        with self._lock:
            if unit.unit_id in self._units:
                raise InventoryValidationError(
                    f"Duplicate unit ID '{unit.unit_id}' detected. "
                    f"Physical units must have globally unique IDs."
                )
            self._units[unit.unit_id] = unit

    def get_unit(self, unit_id: str) -> Optional[BloodUnit]:
        """Retrieve unit by ID."""
        with self._lock:
            return self._units.get(unit_id)

    def total_count(self) -> int:
        """Total count of units in inventory."""
        with self._lock:
            return len(self._units)

    def available_count(self) -> int:
        """Count of units currently in AVAILABLE status."""
        with self._lock:
            return sum(1 for u in self._units.values() if u.status == UnitStatus.AVAILABLE)

    def find_eligible_units(
        self,
        recipient_group: BloodGroup,
        component_type: ComponentType,
        quantity: int,
        compatibility_policy: SyntheticCompatibilityPolicy,
        preferred_branch: Optional[str] = None,
    ) -> List[BloodUnit]:
        """Find eligible AVAILABLE units satisfying component type and compatibility.

        Sorting strategy:
        1. Units located at the requesting preferred_branch first (minimizing transit).
        2. Exact blood group match first.
        3. Other compatible blood groups in order of compatibility policy.
        4. Deterministic unit_id tie-breaking.
        """
        if quantity <= 0:
            return []

        with self._lock:
            compatible_groups = compatibility_policy.get_compatible_donor_groups(
                recipient_group, component_type
            )
            compatible_set = set(compatible_groups)
            group_rank = {g: idx for idx, g in enumerate(compatible_groups)}

            candidates: List[BloodUnit] = []
            for unit in self._units.values():
                if unit.status != UnitStatus.AVAILABLE:
                    continue
                if unit.component_type != component_type:
                    continue
                if unit.blood_group in compatible_set:
                    candidates.append(unit)

            # Sort candidates
            def sort_key(u: BloodUnit):
                branch_rank = 0 if (preferred_branch and u.branch_id == preferred_branch) else 1
                bg_rank = group_rank.get(u.blood_group, 999)
                return (branch_rank, bg_rank, u.unit_id)

            candidates.sort(key=sort_key)
            return candidates[:quantity]

    def reserve(
        self,
        unit_ids: List[str],
        allocation_id: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> List[BloodUnit]:
        """Atomically transition a list of units to RESERVED."""
        with self._lock:
            # Phase 1: Validate all units exist and are AVAILABLE
            for uid in unit_ids:
                unit = self._units.get(uid)
                if not unit:
                    raise InventoryValidationError(f"Unit ID '{uid}' not found in inventory.")
                if unit.status != UnitStatus.AVAILABLE:
                    raise InvalidStateTransitionError(
                        f"Cannot reserve unit '{uid}': status is '{unit.status.value}', "
                        f"expected '{UnitStatus.AVAILABLE.value}'."
                    )

            # Phase 2: Perform atomic reservation
            reserved_units: List[BloodUnit] = []
            for uid in unit_ids:
                unit = self._units[uid]
                unit.reserve(allocation_id=allocation_id, metadata=metadata)
                reserved_units.append(unit)

            return reserved_units

    def allocate(
        self,
        unit_ids: List[str],
        allocation_id: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> List[BloodUnit]:
        """Atomically transition a list of units to ALLOCATED."""
        with self._lock:
            # Phase 1: Validate all units exist and are eligible
            for uid in unit_ids:
                unit = self._units.get(uid)
                if not unit:
                    raise InventoryValidationError(f"Unit ID '{uid}' not found in inventory.")
                if unit.status == UnitStatus.ALLOCATED:
                    raise InvalidStateTransitionError(
                        f"Cannot allocate unit '{uid}': already ALLOCATED to '{unit.allocation_id}'."
                    )
                if unit.status == UnitStatus.RESERVED and unit.allocation_id != allocation_id:
                    raise InvalidStateTransitionError(
                        f"Cannot allocate unit '{uid}': RESERVED for '{unit.allocation_id}', "
                        f"not '{allocation_id}'."
                    )

            # Phase 2: Perform atomic allocation
            allocated_units: List[BloodUnit] = []
            for uid in unit_ids:
                unit = self._units[uid]
                unit.allocate(allocation_id=allocation_id, metadata=metadata)
                allocated_units.append(unit)

            return allocated_units

    def cancel_reservation(self, unit_ids: List[str]) -> None:
        """Cancel reservation for the given units, reverting them to AVAILABLE."""
        with self._lock:
            # Validate first
            for uid in unit_ids:
                unit = self._units.get(uid)
                if not unit:
                    raise InventoryValidationError(f"Unit ID '{uid}' not found in inventory.")
                if unit.status != UnitStatus.RESERVED:
                    raise InvalidStateTransitionError(
                        f"Cannot cancel reservation for unit '{uid}': "
                        f"status is '{unit.status.value}', expected '{UnitStatus.RESERVED.value}'."
                    )

            # Revert
            for uid in unit_ids:
                self._units[uid].release_reservation()

    def mark_reservation_uncertain(
        self, allocation_id: str, reason: str = "node_failure"
    ) -> List[str]:
        """Mark all units currently reserved for an allocation as uncertain due to node failure."""
        with self._lock:
            affected_ids = []
            for unit in self._units.values():
                if unit.status == UnitStatus.RESERVED and unit.allocation_id == allocation_id:
                    unit.metadata["is_uncertain"] = True
                    unit.metadata["uncertain_reason"] = reason
                    unit.metadata["marked_uncertain_at"] = time.time()
                    affected_ids.append(unit.unit_id)
            return affected_ids

    def get_uncertain_reservations(self) -> List[Dict[str, Any]]:
        """Retrieve all reserved units currently flagged as uncertain."""
        with self._lock:
            return [
                unit.to_dict()
                for unit in self._units.values()
                if unit.status == UnitStatus.RESERVED and unit.metadata.get("is_uncertain") is True
            ]

    def reconcile_uncertain_reservation(
        self,
        allocation_id: str,
        action: str,
        operator_note: Optional[str] = None,
    ) -> List[BloodUnit]:
        """Explicitly resolve an uncertain reservation via coordinated administrative intervention.

        Actions:
        - 'COMMIT': Confirms allocation (units transition to ALLOCATED).
        - 'ABORT': Safe rollback (units revert to AVAILABLE).
        """
        norm_action = action.strip().upper()
        if norm_action not in ("COMMIT", "ABORT"):
            raise ValueError(f"Invalid reconciliation action '{action}'. Must be 'COMMIT' or 'ABORT'.")

        with self._lock:
            target_units = [
                u for u in self._units.values()
                if u.status == UnitStatus.RESERVED and u.allocation_id == allocation_id
            ]
            if not target_units:
                raise InventoryValidationError(
                    f"No reserved units found for allocation_id '{allocation_id}' to reconcile."
                )

            unit_ids = [u.unit_id for u in target_units]

            if norm_action == "COMMIT":
                meta = {
                    "reconciled": True,
                    "reconciliation_action": "COMMIT",
                    "operator_note": operator_note or "Administrative manual commit",
                }
                for u in target_units:
                    u.metadata.pop("is_uncertain", None)
                return self.allocate(unit_ids, allocation_id, metadata=meta)
            else:  # ABORT
                for u in target_units:
                    u.metadata.pop("is_uncertain", None)
                    u.metadata["reconciled"] = True
                    u.metadata["reconciliation_action"] = "ABORT"
                    if operator_note:
                        u.metadata["operator_note"] = operator_note
                self.cancel_reservation(unit_ids)
                return [self._units[uid] for uid in unit_ids]

    def get_summary(self) -> Dict[str, Any]:
        """Diagnostic summary of inventory counts."""
        with self._lock:
            by_status: Dict[str, int] = {}
            by_group: Dict[str, int] = {}
            by_component: Dict[str, int] = {}
            by_branch: Dict[str, int] = {}

            for u in self._units.values():
                by_status[u.status.value] = by_status.get(u.status.value, 0) + 1
                by_group[u.blood_group.value] = by_group.get(u.blood_group.value, 0) + 1
                by_component[u.component_type.value] = by_component.get(u.component_type.value, 0) + 1
                by_branch[u.branch_id] = by_branch.get(u.branch_id, 0) + 1

            return {
                "total_units": len(self._units),
                "by_status": by_status,
                "by_group": by_group,
                "by_component": by_component,
                "by_branch": by_branch,
            }
