"""Synthetic data generation for blood-bank inventory experiments."""
import random
from typing import List, Optional
from src.blood_bank.models import BloodUnit, BloodGroup, ComponentType, UnitStatus
from src.blood_bank.inventory import BloodBankInventory


def generate_synthetic_inventory(
    branches: List[str],
    units_per_branch: int = 25,
    seed: Optional[int] = 42,
) -> BloodBankInventory:
    """Generate a deterministic synthetic inventory across multiple branches.

    Guarantees:
    - Every generated BloodUnit has a globally unique ID across all branches.
    - All units start in AVAILABLE status.
    - Blood groups and component types are drawn from documented supported sets.
    """
    rng = random.Random(seed)
    inventory = BloodBankInventory()

    blood_groups = list(BloodGroup)
    component_types = list(ComponentType)

    # Approximate distribution weights for synthetic diversity
    group_weights = [0.30, 0.07, 0.15, 0.03, 0.05, 0.01, 0.33, 0.06]  # A+, A-, B+, B-, AB+, AB-, O+, O-
    comp_weights = [0.45, 0.15, 0.20, 0.15, 0.05]  # RBC, WHOLE_BLOOD, PLATELETS, PLASMA, CRYO

    for branch_id in branches:
        for idx in range(units_per_branch):
            bg = rng.choices(blood_groups, weights=group_weights, k=1)[0]
            ct = rng.choices(component_types, weights=comp_weights, k=1)[0]

            # Formulate globally unique unit identifier: UNIT-{branch}-{group_clean}-{idx}
            clean_bg = bg.value.replace("+", "POS").replace("-", "NEG")
            unit_id = f"UNIT-{branch_id}-{clean_bg}-{idx:04d}"

            unit = BloodUnit(
                unit_id=unit_id,
                blood_group=bg,
                component_type=ct,
                branch_id=branch_id,
                status=UnitStatus.AVAILABLE,
                metadata={"batch_number": f"BATCH-{branch_id}-2026", "unit_seq": idx},
            )
            inventory.add_unit(unit)

    return inventory
