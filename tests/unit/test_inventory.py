"""Unit tests for blood-bank inventory models, state transitions, and compatibility."""
import pytest
from src.blood_bank.models import (
    BloodGroup,
    ComponentType,
    UnitStatus,
    BloodUnit,
    InvalidStateTransitionError,
    InventoryValidationError,
)
from src.blood_bank.inventory import BloodBankInventory
from src.blood_bank.compatibility import SyntheticCompatibilityPolicy
from src.blood_bank.synthetic_data import generate_synthetic_inventory


def test_valid_blood_unit_creation():
    """Verify valid BloodUnit initialization and dictionary round-trip."""
    unit = BloodUnit(
        unit_id="UNIT-001",
        blood_group=BloodGroup.O_NEG,
        component_type=ComponentType.RBC,
        branch_id="branch-0",
        status=UnitStatus.AVAILABLE,
    )
    assert unit.unit_id == "UNIT-001"
    assert unit.blood_group == BloodGroup.O_NEG
    assert unit.component_type == ComponentType.RBC
    assert unit.branch_id == "branch-0"
    assert unit.status == UnitStatus.AVAILABLE

    d = unit.to_dict()
    assert d["unit_id"] == "UNIT-001"
    assert d["blood_group"] == "O-"
    assert d["component_type"] == "RBC"

    reconstructed = BloodUnit.from_dict(d)
    assert reconstructed.unit_id == unit.unit_id
    assert reconstructed.blood_group == unit.blood_group
    assert reconstructed.status == unit.status


def test_invalid_blood_unit_attributes():
    """Verify validation rejects invalid unit attributes."""
    # Empty unit ID
    with pytest.raises(InventoryValidationError):
        BloodUnit("", BloodGroup.A_POS, ComponentType.RBC, "branch-0")

    # Empty branch ID
    with pytest.raises(InventoryValidationError):
        BloodUnit("UNIT-1", BloodGroup.A_POS, ComponentType.RBC, "")

    # Unsupported blood group string
    with pytest.raises(ValueError):
        BloodUnit("UNIT-1", "XYZ", ComponentType.RBC, "branch-0")

    # Unsupported component type string
    with pytest.raises(ValueError):
        BloodUnit("UNIT-1", BloodGroup.A_POS, "NANOBOTS", "branch-0")


def test_unique_unit_ids_enforced_in_inventory():
    """Ensure duplicate unit IDs cannot be added to inventory."""
    inv = BloodBankInventory()
    unit1 = BloodUnit("U-100", BloodGroup.A_POS, ComponentType.RBC, "branch-0")
    unit2 = BloodUnit("U-100", BloodGroup.B_POS, ComponentType.PLASMA, "branch-1")

    inv.add_unit(unit1)
    with pytest.raises(InventoryValidationError, match="Duplicate unit ID"):
        inv.add_unit(unit2)


def test_valid_state_transitions():
    """Verify correct lifecycle transitions: AVAILABLE -> RESERVED -> ALLOCATED and direct ALLOCATED."""
    # 1. AVAILABLE -> RESERVED -> ALLOCATED
    unit = BloodUnit("U-1", BloodGroup.O_POS, ComponentType.RBC, "branch-0")
    assert unit.status == UnitStatus.AVAILABLE

    unit.reserve("alloc-1")
    assert unit.status == UnitStatus.RESERVED
    assert unit.allocation_id == "alloc-1"

    unit.allocate("alloc-1")
    assert unit.status == UnitStatus.ALLOCATED
    assert unit.allocation_id == "alloc-1"

    # 2. AVAILABLE -> ALLOCATED directly
    unit2 = BloodUnit("U-2", BloodGroup.O_POS, ComponentType.RBC, "branch-0")
    unit2.allocate("alloc-2")
    assert unit2.status == UnitStatus.ALLOCATED

    # 3. AVAILABLE -> RESERVED -> AVAILABLE (release reservation)
    unit3 = BloodUnit("U-3", BloodGroup.O_POS, ComponentType.RBC, "branch-0")
    unit3.reserve("alloc-3")
    unit3.release_reservation()
    assert unit3.status == UnitStatus.AVAILABLE
    assert unit3.allocation_id is None


def test_invalid_state_transitions():
    """Verify illegal lifecycle transitions are strictly rejected."""
    unit = BloodUnit("U-1", BloodGroup.A_POS, ComponentType.RBC, "branch-0")
    unit.allocate("alloc-1")

    # Cannot reserve an already allocated unit
    with pytest.raises(InvalidStateTransitionError):
        unit.reserve("alloc-2")

    # Cannot allocate an already allocated unit (double allocation)
    with pytest.raises(InvalidStateTransitionError):
        unit.allocate("alloc-3")

    # Cannot release reservation of an allocated unit
    with pytest.raises(InvalidStateTransitionError):
        unit.release_reservation()

    # Unit in AVAILABLE cannot have reservation released
    unit_avail = BloodUnit("U-2", BloodGroup.A_POS, ComponentType.RBC, "branch-0")
    with pytest.raises(InvalidStateTransitionError):
        unit_avail.release_reservation()


def test_no_allocation_of_reserved_units_by_different_requester():
    """Reserved units cannot be allocated by a different allocation ID."""
    unit = BloodUnit("U-1", BloodGroup.B_POS, ComponentType.RBC, "branch-0")
    unit.reserve("alloc-owner-1")

    with pytest.raises(InvalidStateTransitionError, match="RESERVED for 'alloc-owner-1'"):
        unit.allocate("alloc-intruder-2")


def test_inventory_atomic_reserve_and_allocate():
    """Verify inventory batch reserve, allocate, and cancel."""
    inv = BloodBankInventory()
    u1 = BloodUnit("U-1", BloodGroup.O_NEG, ComponentType.RBC, "branch-0")
    u2 = BloodUnit("U-2", BloodGroup.O_NEG, ComponentType.RBC, "branch-0")
    inv.add_unit(u1)
    inv.add_unit(u2)

    # Reserve both
    reserved = inv.reserve(["U-1", "U-2"], "alloc-batch-1")
    assert len(reserved) == 2
    assert all(u.status == UnitStatus.RESERVED for u in reserved)
    assert inv.available_count() == 0

    # Cancel one reservation
    inv.cancel_reservation(["U-1"])
    assert inv.get_unit("U-1").status == UnitStatus.AVAILABLE
    assert inv.get_unit("U-2").status == UnitStatus.RESERVED
    assert inv.available_count() == 1

    # Allocate U-2 with owner ID
    allocated = inv.allocate(["U-2"], "alloc-batch-1")
    assert len(allocated) == 1
    assert inv.get_unit("U-2").status == UnitStatus.ALLOCATED


def test_synthetic_compatibility_rbc():
    """Verify synthetic RBC compatibility matrix."""
    policy = SyntheticCompatibilityPolicy()

    # O- is universal RBC donor
    assert policy.is_compatible(BloodGroup.O_NEG, BloodGroup.AB_POS, ComponentType.RBC)
    assert policy.is_compatible(BloodGroup.O_NEG, BloodGroup.O_POS, ComponentType.RBC)
    assert policy.is_compatible(BloodGroup.O_NEG, BloodGroup.A_POS, ComponentType.RBC)

    # AB+ cannot donate RBC to O-
    assert not policy.is_compatible(BloodGroup.AB_POS, BloodGroup.O_NEG, ComponentType.RBC)

    # Exact match is always compatible
    assert policy.is_compatible(BloodGroup.B_POS, BloodGroup.B_POS, ComponentType.RBC)


def test_synthetic_compatibility_plasma():
    """Verify synthetic Plasma compatibility (AB is universal donor)."""
    policy = SyntheticCompatibilityPolicy()

    # AB+ can donate plasma to O-
    assert policy.is_compatible(BloodGroup.AB_POS, BloodGroup.O_NEG, ComponentType.PLASMA)

    # O- cannot donate plasma to AB+
    assert not policy.is_compatible(BloodGroup.O_NEG, BloodGroup.AB_POS, ComponentType.PLASMA)


def test_find_eligible_units_and_branch_preference():
    """Verify inventory eligible unit search respects branch proximity preference."""
    inv = BloodBankInventory()
    # Branch 1 has O-, Branch 0 has O-
    u_other = BloodUnit("U-OTHER", BloodGroup.O_NEG, ComponentType.RBC, "branch-1")
    u_local = BloodUnit("U-LOCAL", BloodGroup.O_NEG, ComponentType.RBC, "branch-0")
    inv.add_unit(u_other)
    inv.add_unit(u_local)

    policy = SyntheticCompatibilityPolicy()
    eligible = inv.find_eligible_units(
        recipient_group=BloodGroup.O_NEG,
        component_type=ComponentType.RBC,
        quantity=2,
        compatibility_policy=policy,
        preferred_branch="branch-0",
    )

    # Local unit must be selected first
    assert len(eligible) == 2
    assert eligible[0].unit_id == "U-LOCAL"
    assert eligible[1].unit_id == "U-OTHER"


def test_synthetic_inventory_generator():
    """Verify synthetic data generator produces valid, unique dataset."""
    branches = ["branch-0", "branch-1", "branch-2"]
    inv = generate_synthetic_inventory(branches, units_per_branch=20, seed=123)

    assert inv.total_count() == 60
    assert inv.available_count() == 60

    summary = inv.get_summary()
    assert summary["total_units"] == 60
    assert summary["by_status"]["AVAILABLE"] == 60
    assert len(summary["by_branch"]) == 3
