"""Distributed Allocation Service protected by mutual-exclusion runtime."""
import asyncio
import logging
import time
from typing import Dict, List, Optional, Any, Set
from src.blood_bank.models import (
    AllocationRequest,
    AllocationResult,
    AllocationStatus,
    BloodUnit,
    InvalidStateTransitionError,
    InventoryValidationError,
)
from src.blood_bank.inventory import BloodBankInventory
from src.blood_bank.compatibility import SyntheticCompatibilityPolicy
from src.node.branch_node import BranchNode

logger = logging.getLogger("blood_bank.allocation")


class AllocationService:
    """Coordinates blood unit allocation requests protected by distributed mutual exclusion.

    Execution Flow:
    1. Pre-validates request (rejecting invalid quantities, branches, or types before CS).
    2. Requests CS through the local branch node's selected mutual-exclusion protocol.
    3. Asynchronously awaits permission grant from the protocol.
    4. Enters critical section when permission is safely confirmed.
    5. Checks authoritative inventory for eligible available units under mutual exclusion.
    6. Commits atomic state transitions (ALLOCATED or RESERVED).
    7. Exits and releases critical section via protocol cleanup path.
    8. Records detailed business diagnostics.
    """

    def __init__(
        self,
        node: BranchNode,
        inventory: BloodBankInventory,
        compatibility_policy: Optional[SyntheticCompatibilityPolicy] = None,
        valid_branches: Optional[Set[str]] = None,
        fail_closed_on_unhealthy: bool = False,
    ) -> None:
        self.node = node
        self.inventory = inventory
        self.compatibility_policy = compatibility_policy or SyntheticCompatibilityPolicy()
        self.valid_branches = valid_branches
        self.fail_closed_on_unhealthy = fail_closed_on_unhealthy
        self.history: List[AllocationResult] = []
        self._lock = asyncio.Lock()

    async def execute_allocation(
        self,
        request: AllocationRequest,
        timeout_sec: float = 10.0,
        reserve_only: bool = False,
        fail_closed_on_unhealthy: Optional[bool] = None,
    ) -> AllocationResult:
        """Execute a protected blood unit allocation operation."""
        start_time = time.time()
        req_id = request.request_id
        algo_name = self.node.config.system.algorithm
        should_fail_closed = (
            self.fail_closed_on_unhealthy
            if fail_closed_on_unhealthy is None
            else fail_closed_on_unhealthy
        )

        logger.info(
            "ALLOCATION_REQUEST_RECEIVED: req_id=%s branch=%s group=%s comp=%s qty=%d",
            req_id, request.requesting_branch, request.blood_group.value,
            request.component_type.value, request.quantity
        )

        # Step 1: Pre-validation before requesting CS
        try:
            request.validate(valid_branches=self.valid_branches)
        except Exception as val_err:
            logger.warning(
                "ALLOCATION_VALIDATION_FAILED: req_id=%s reason='%s'",
                req_id, str(val_err)
            )
            result = AllocationResult(
                request_id=req_id,
                requesting_branch=request.requesting_branch,
                status=AllocationStatus.VALIDATION_FAILED,
                error_message=str(val_err),
                algorithm=algo_name,
                duration_sec=time.time() - start_time,
                cs_entered=False,
            )
            self.history.append(result)
            return result

        # Step 1b: Pre-check peer liveness for algorithms requiring unanimous peer participation
        if should_fail_closed and self.node.failure_detector.has_unhealthy_peers():
            unhealthy = self.node.failure_detector.get_unhealthy_peers()
            logger.warning(
                "ALLOCATION_FAILED_CLOSED: req_id=%s unhealthy_peers=%s",
                req_id, unhealthy
            )
            result = AllocationResult(
                request_id=req_id,
                requesting_branch=request.requesting_branch,
                status=AllocationStatus.FAILED_CLOSED,
                error_message=(
                    f"Fail-closed: Participating peers {unhealthy} are suspected or confirmed failed. "
                    f"Quorum cannot be safely formed without split-brain risk; manual or coordinated recovery required."
                ),
                algorithm=algo_name,
                duration_sec=time.time() - start_time,
                cs_entered=False,
            )
            self.history.append(result)
            return result

        # Step 2: Request entry through local mutual-exclusion protocol
        logger.info(
            "ALLOCATION_CS_REQUESTED: req_id=%s node_id=%s algo=%s",
            req_id, self.node.node_id, algo_name
        )
        await self.node.request_critical_section(req_id)

        # Step 3: Asynchronously await grant
        granted = await self.node.wait_for_cs_grant(timeout=timeout_sec)
        if not granted:
            unhealthy_peers = self.node.failure_detector.get_unhealthy_peers()
            if should_fail_closed and unhealthy_peers:
                logger.error(
                    "ALLOCATION_FAILED_CLOSED_TIMEOUT: req_id=%s node_id=%s unhealthy=%s",
                    req_id, self.node.node_id, unhealthy_peers
                )
                result = AllocationResult(
                    request_id=req_id,
                    requesting_branch=request.requesting_branch,
                    status=AllocationStatus.FAILED_CLOSED,
                    error_message=(
                        f"Fail-closed: Timed out waiting for CS while peers {unhealthy_peers} are unhealthy. "
                        f"Token regeneration or unilateral CS entry on timeout is prohibited to avoid duplicate allocation."
                    ),
                    algorithm=algo_name,
                    duration_sec=time.time() - start_time,
                    cs_entered=False,
                )
            else:
                logger.error(
                    "ALLOCATION_CS_GRANT_TIMEOUT: req_id=%s node_id=%s timeout=%.2fs",
                    req_id, self.node.node_id, timeout_sec
                )
                result = AllocationResult(
                    request_id=req_id,
                    requesting_branch=request.requesting_branch,
                    status=AllocationStatus.MUTUAL_EXCLUSION_FAILED,
                    error_message=f"Timed out after {timeout_sec:.2f}s waiting for critical section permission.",
                    algorithm=algo_name,
                    duration_sec=time.time() - start_time,
                    cs_entered=False,
                )
            # CRITICAL: We NEVER release protocol if CS was never granted/entered
            self.history.append(result)
            return result

        # Step 4: Protocol has granted entry; enter critical section
        cs_entered = False
        try:
            await self.node.enter_critical_section(req_id)
            cs_entered = True
            logger.info(
                "ALLOCATION_CS_ENTERED: req_id=%s node_id=%s",
                req_id, self.node.node_id
            )

            # Step 5: Check inventory for eligible units under mutual exclusion
            eligible_units = self.inventory.find_eligible_units(
                recipient_group=request.blood_group,
                component_type=request.component_type,
                quantity=request.quantity,
                compatibility_policy=self.compatibility_policy,
                preferred_branch=request.requesting_branch,
            )

            if len(eligible_units) < request.quantity:
                logger.warning(
                    "ALLOCATION_INSUFFICIENT_STOCK: req_id=%s requested=%d available=%d",
                    req_id, request.quantity, len(eligible_units)
                )
                result = AllocationResult(
                    request_id=req_id,
                    requesting_branch=request.requesting_branch,
                    status=AllocationStatus.INSUFFICIENT_STOCK,
                    allocated_units=[],
                    error_message=(
                        f"Insufficient inventory: requested {request.quantity} units of "
                        f"{request.blood_group.value} {request.component_type.value}, "
                        f"found {len(eligible_units)} eligible units."
                    ),
                    algorithm=algo_name,
                    duration_sec=time.time() - start_time,
                    cs_entered=True,
                )
            else:
                # Step 6: Perform atomic local state transition
                selected = eligible_units[:request.quantity]
                selected_ids = [u.unit_id for u in selected]

                if reserve_only:
                    committed = self.inventory.reserve(
                        unit_ids=selected_ids,
                        allocation_id=req_id,
                        metadata={
                            "requesting_branch": request.requesting_branch,
                            "timestamp": time.time(),
                            **request.metadata,
                        },
                    )
                else:
                    committed = self.inventory.allocate(
                        unit_ids=selected_ids,
                        allocation_id=req_id,
                        metadata={
                            "requesting_branch": request.requesting_branch,
                            "timestamp": time.time(),
                            **request.metadata,
                        },
                    )

                logger.info(
                    "ALLOCATION_COMMITTED: req_id=%s mode=%s units=%s",
                    req_id, "RESERVED" if reserve_only else "ALLOCATED", selected_ids
                )

                result = AllocationResult(
                    request_id=req_id,
                    requesting_branch=request.requesting_branch,
                    status=AllocationStatus.SUCCESS,
                    allocated_units=selected_ids,
                    allocated_units_details=[u.to_dict() for u in committed],
                    algorithm=algo_name,
                    duration_sec=time.time() - start_time,
                    cs_entered=True,
                )

        except Exception as exc:
            logger.error(
                "ALLOCATION_INTERNAL_ERROR: req_id=%s error=%s",
                req_id, str(exc), exc_info=True
            )
            result = AllocationResult(
                request_id=req_id,
                requesting_branch=request.requesting_branch,
                status=AllocationStatus.ERROR,
                error_message=str(exc),
                algorithm=algo_name,
                duration_sec=time.time() - start_time,
                cs_entered=cs_entered,
            )

        finally:
            # Step 7 & 8: Safe cleanup path - release protocol ONLY if actually entered
            if cs_entered:
                try:
                    await self.node.release_critical_section(req_id)
                    logger.info(
                        "ALLOCATION_CS_RELEASED: req_id=%s node_id=%s",
                        req_id, self.node.node_id
                    )
                except Exception as rel_err:
                    logger.error(
                        "CRITICAL: Failed to release CS for req_id=%s: %s",
                        req_id, str(rel_err)
                    )

        self.history.append(result)
        return result
