"""Synthetic blood compatibility rules for distributed allocation simulations.

================================================================================
CRITICAL CLINICAL & REGULATORY DISCLAIMER:
This module implements a simplified synthetic ABO/Rh compatibility model
exclusively for computer science research and distributed mutual-exclusion
performance benchmarks.

IT IS NOT VALIDATED, ACCREDITED, OR SUITABLE FOR CLINICAL DECISION SUPPORT,
BLOOD TRANSFUSION PRACTICE, OR HUMAN MEDICAL APPLICATIONS.

Real-world clinical transfusion requires comprehensive pre-transfusion testing,
including:
1. Recipient ABO and Rh(D) typing.
2. Unexpected red cell antibody screening (indirect antiglobulin testing).
3. Immediate-spin or full crossmatching between patient serum and donor cells.
4. Kell, Duffy, Kidd, MNS, and other minor antigen typing when alloimmunized.
5. CMV, irradiation, and leukoreduction requirements.
6. Licensed physician prescription and dual-nurse bedside verification.
================================================================================
"""
from typing import Dict, List, Set
from src.blood_bank.models import BloodGroup, ComponentType


# Synthetic RBC / Whole Blood compatible donor groups for a given recipient
# Red cells carry ABO/Rh antigens; recipient serum contains antibodies against foreign antigens.
# Universal RBC donor: O- (O Rhesus Negative)
# Universal RBC recipient: AB+ (AB Rhesus Positive)
_RBC_COMPATIBILITY: Dict[BloodGroup, List[BloodGroup]] = {
    BloodGroup.O_NEG: [BloodGroup.O_NEG],
    BloodGroup.O_POS: [BloodGroup.O_POS, BloodGroup.O_NEG],
    BloodGroup.A_NEG: [BloodGroup.A_NEG, BloodGroup.O_NEG],
    BloodGroup.A_POS: [BloodGroup.A_POS, BloodGroup.A_NEG, BloodGroup.O_POS, BloodGroup.O_NEG],
    BloodGroup.B_NEG: [BloodGroup.B_NEG, BloodGroup.O_NEG],
    BloodGroup.B_POS: [BloodGroup.B_POS, BloodGroup.B_NEG, BloodGroup.O_POS, BloodGroup.O_NEG],
    BloodGroup.AB_NEG: [BloodGroup.AB_NEG, BloodGroup.A_NEG, BloodGroup.B_NEG, BloodGroup.O_NEG],
    BloodGroup.AB_POS: [
        BloodGroup.AB_POS, BloodGroup.AB_NEG,
        BloodGroup.A_POS, BloodGroup.A_NEG,
        BloodGroup.B_POS, BloodGroup.B_NEG,
        BloodGroup.O_POS, BloodGroup.O_NEG,
    ],
}

# Synthetic Plasma compatibility (FFP)
# Plasma carries antibodies, not red cells. Universal plasma donor is AB (no anti-A or anti-B).
# Universal plasma recipient: O
_PLASMA_COMPATIBILITY: Dict[BloodGroup, List[BloodGroup]] = {
    BloodGroup.O_NEG: [BloodGroup.O_NEG, BloodGroup.O_POS, BloodGroup.A_NEG, BloodGroup.A_POS,
                       BloodGroup.B_NEG, BloodGroup.B_POS, BloodGroup.AB_NEG, BloodGroup.AB_POS],
    BloodGroup.O_POS: [BloodGroup.O_POS, BloodGroup.O_NEG, BloodGroup.A_POS, BloodGroup.A_NEG,
                       BloodGroup.B_POS, BloodGroup.B_NEG, BloodGroup.AB_POS, BloodGroup.AB_NEG],
    BloodGroup.A_NEG: [BloodGroup.A_NEG, BloodGroup.A_POS, BloodGroup.AB_NEG, BloodGroup.AB_POS],
    BloodGroup.A_POS: [BloodGroup.A_POS, BloodGroup.A_NEG, BloodGroup.AB_POS, BloodGroup.AB_NEG],
    BloodGroup.B_NEG: [BloodGroup.B_NEG, BloodGroup.B_POS, BloodGroup.AB_NEG, BloodGroup.AB_POS],
    BloodGroup.B_POS: [BloodGroup.B_POS, BloodGroup.B_NEG, BloodGroup.AB_POS, BloodGroup.AB_NEG],
    BloodGroup.AB_NEG: [BloodGroup.AB_NEG, BloodGroup.AB_POS],
    BloodGroup.AB_POS: [BloodGroup.AB_POS, BloodGroup.AB_NEG],
}

# Platelets and Cryoprecipitate: in synthetic model, prefer identical group or compatible ABO
_PLATELET_COMPATIBILITY: Dict[BloodGroup, List[BloodGroup]] = {
    BloodGroup.O_NEG: [BloodGroup.O_NEG, BloodGroup.O_POS],
    BloodGroup.O_POS: [BloodGroup.O_POS, BloodGroup.O_NEG],
    BloodGroup.A_NEG: [BloodGroup.A_NEG, BloodGroup.A_POS, BloodGroup.O_NEG],
    BloodGroup.A_POS: [BloodGroup.A_POS, BloodGroup.A_NEG, BloodGroup.O_POS, BloodGroup.O_NEG],
    BloodGroup.B_NEG: [BloodGroup.B_NEG, BloodGroup.B_POS, BloodGroup.O_NEG],
    BloodGroup.B_POS: [BloodGroup.B_POS, BloodGroup.B_NEG, BloodGroup.O_POS, BloodGroup.O_NEG],
    BloodGroup.AB_NEG: [BloodGroup.AB_NEG, BloodGroup.AB_POS, BloodGroup.A_NEG, BloodGroup.B_NEG],
    BloodGroup.AB_POS: [BloodGroup.AB_POS, BloodGroup.AB_NEG, BloodGroup.A_POS, BloodGroup.B_POS, BloodGroup.O_POS],
}


class SyntheticCompatibilityPolicy:
    """Evaluates synthetic donor-recipient compatibility for simulated allocations."""

    def __init__(self, exact_match_only: bool = False) -> None:
        self.exact_match_only = exact_match_only

    def get_compatible_donor_groups(
        self, recipient_group: BloodGroup, component_type: ComponentType
    ) -> List[BloodGroup]:
        """Return ordered list of compatible donor groups for the given component and recipient.

        The recipient's own exact group is always placed first.
        """
        if self.exact_match_only:
            return [recipient_group]

        if component_type in (ComponentType.RBC, ComponentType.WHOLE_BLOOD):
            groups = _RBC_COMPATIBILITY.get(recipient_group, [recipient_group])
        elif component_type == ComponentType.PLASMA:
            groups = _PLASMA_COMPATIBILITY.get(recipient_group, [recipient_group])
        elif component_type in (ComponentType.PLATELETS, ComponentType.CRYO):
            groups = _PLATELET_COMPATIBILITY.get(recipient_group, [recipient_group])
        else:
            groups = [recipient_group]

        # Prioritize exact match first
        ordered = [recipient_group]
        for g in groups:
            if g != recipient_group:
                ordered.append(g)
        return ordered

    def is_compatible(
        self, donor_group: BloodGroup, recipient_group: BloodGroup, component_type: ComponentType
    ) -> bool:
        """Check if donor group is compatible with recipient group for the specified component."""
        compatible_groups = self.get_compatible_donor_groups(recipient_group, component_type)
        return donor_group in compatible_groups
