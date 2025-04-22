from enum import Enum
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

import aiorwlock
from pydantic import BaseModel


class ProcessorWorkerDetails(BaseModel):
    """
    Details of a processor worker.
    """
    unique_name: str
    pid: Union[None, int] = None


class SnapshotSubmissionSignerState(BaseModel):
    """
    State of a snapshot submission signer.
    """
    address: str
    private_key: str
    nonce: int
    nonce_lock: aiorwlock.RWLock

    class Config:
        arbitrary_types_allowed = True


class SnapshotterReportState(Enum):
    """
    Enumeration of possible states for a snapshotter report.
    """
    MISSED_SNAPSHOT = 'MISSED_SNAPSHOT'
    SUBMITTED_INCORRECT_SNAPSHOT = 'SUBMITTED_INCORRECT_SNAPSHOT'
    SHUTDOWN_INITIATED = 'SHUTDOWN_INITIATED'
    CRASHED_CHILD_WORKER = 'CRASHED_CHILD_WORKER'
    CRASHED_REPORTER_THREAD = 'CRASHED_REPORTER_THREAD'
    UNHEALTHY_EPOCH_PROCESSING = 'UNHEALTHY_EPOCH_PROCESSING'
    ONLY_FINALIZED_SNAPSHOT_RECIEVED = 'ONLY_FINALIZED_SNAPSHOT_RECIEVED'
    DELEGATE_TASK_FAILURE = 'DELEGATE_TASK_FAILURE'


class SnapshotterStates(Enum):
    """
    Enumeration of possible states for a snapshotter.
    """
    PRELOAD = 'PRELOAD'
    SNAPSHOT_BUILD = 'SNAPSHOT_BUILD'
    SNAPSHOT_SUBMIT_PAYLOAD_COMMIT = 'SNAPSHOT_SUBMIT_PAYLOAD_COMMIT'
    RELAYER_SEND = 'RELAYER_SEND'
    SNAPSHOT_FINALIZE = 'SNAPSHOT_FINALIZE'
    SNAPSHOT_SUBMIT_COLLECTOR = 'SNAPSHOT_SUBMIT_COLLECTOR'


class SigningWorkStates(Enum):
    """
    Enumeration of possible states for signing work.
    """
    PROJECT_IDS_SNAPSHOTTED = 'PROJECT_IDS_SNAPSHOTTED'  # key value update in redis
    SLOTS_SELECTED = 'SLOTS_SELECTED'  # key value update in redis
    PROJECT_SLOT_ASSIGNMENT = 'PROJECT_SLOT_ASSIGNMENT'  # htable update in redis
    SLOT_COLLECTOR_SUBMISSION = 'SLOT_COLLECTOR_SUBMISSION'  # htable update in redis


class ServiceNames(Enum):
    """Enumeration of service names used in the application."""
    SYSTEM_EVENT_DETECTOR = 'system_event_detector'
    PROCESSOR_DISTRIBUTOR = 'processor_distributor'
    SNAPSHOT_WORKER = 'snapshot_worker'
    AGGREGATION_WORKER = 'aggregation_worker'


class SigningWorkProjectsSnapshottedStateItem(BaseModel):
    """
    State item for snapshotted projects in signing work.
    """
    projectIDs: List[str]


class SigningWorkSlotSelectionStateItem(BaseModel):
    """
    State item for slot selection in signing work.
    """
    timeSlot: int
    slotIDs: List[int]


class PowerloomSnapshotSignMessage(BaseModel):
    """
    Message for signing a Powerloom snapshot.
    """
    epochId: int
    slotId: int
    projectIds: List[str]
    snapshotterAddr: str


class SigningWorkOverallProjectSlotAssignmentStateItem(BaseModel):
    """
    State item for overall project slot assignment in signing work.
    """
    slotID: int


class SnapshotterStateUpdate(BaseModel):
    """
    Update for snapshotter state.
    """
    status: str
    error: Optional[str] = None
    extra: Optional[Dict[str, Any]] = None
    timestamp: int


class SnapshotterEpochProcessingReportItem(BaseModel):
    """
    Report item for snapshotter epoch processing.
    """
    epochId: int = 0
    epochEnd: int = 0
    # map transition like EPOCH_RELEASED to its status
    transitionStatus: Dict[str, Union[SnapshotterStateUpdate, None, Dict[str, SnapshotterStateUpdate]]] = dict()


class SnapshotterIssue(BaseModel):
    """
    Representation of an issue encountered by a snapshotter.
    """
    instanceID: str
    issueType: str
    projectID: str
    epochId: str
    timeOfReporting: str
    extra: Optional[str] = ''


class DelegateTaskProcessorIssue(BaseModel):
    """
    Representation of an issue encountered by a delegate task processor.
    """
    instanceID: str
    issueType: str
    epochId: str
    timeOfReporting: str
    extra: Optional[str] = ''


class TimeoutConfig(BaseModel):
    """
    Configuration for timeouts.
    """
    basic: int
    archival: int
    connection_init: int


class RLimitConfig(BaseModel):
    """
    Configuration for resource limits.
    """
    file_descriptors: int


# Event detector related models
class EventBase(BaseModel):
    """
    Base class for all event models.
    """
    timestamp: int


class EpochReleasedEvent(EventBase):
    """
    Event model for when an epoch is released.
    """
    epochId: int
    begin: int
    end: int


class SnapshotFinalizedEvent(EventBase):
    """
    Event model for when a snapshot is finalized.
    """
    epochId: int
    epochEnd: int
    projectId: str
    snapshotCid: str


class ProjectsUpdatedEvent(EventBase):
    """
    Event model for when projects are updated.
    """
    projectId: str
    allowed: bool
    enableEpochId: int


class SnapshottersUpdatedEvent(EventBase):
    """
    Event model for when snapshotters are updated.
    """
    snapshotterAddress: str
    allowed: bool


class SnapshotSubmittedEvent(EventBase):
    """
    Event model for when a snapshot is submitted.
    """
    snapshotCid: str
    epochId: int
    projectId: str


class ProjectSpecificState(BaseModel):
    """
    State specific to a project.
    """
    first_epoch_id: int
    finalized_cids: Dict[int, str]  # mapping of epoch ID to snapshot CID


class ProtocolState(BaseModel):
    """
    Overall state of the protocol.
    """
    project_specific_states: Dict[str, ProjectSpecificState]  # project ID -> project specific state
    synced_till_epoch_id: int


class ProjectStatus(BaseModel):
    """
    Status of a project.
    """
    projectId: str
    successfulSubmissions: int = 0
    incorrectSubmissions: int = 0
    missedSubmissions: int = 0


class SnapshotterMissedSubmission(BaseModel):
    """
    Details of a missed submission by a snapshotter.
    """
    epochId: int
    reason: str


class SnapshotterIncorrectSubmission(BaseModel):
    """
    Details of an incorrect submission by a snapshotter.
    """
    epochId: int
    incorrectCid: str
    payloadDump: str
    reason: str = ''


class SnapshotterStatusReport(BaseModel):
    """
    Status report for a snapshotter.
    """
    submittedSnapshotCid: str
    submittedSnapshot: Dict[str, Any] = {}
    finalizedSnapshotCid: str
    finalizedSnapshot: Dict[str, Any] = {}
    state: SnapshotterReportState
    reason: str = ''


class SnapshotterMissedSnapshotSubmission(BaseModel):
    """
    Details of a missed snapshot submission by a snapshotter.
    """
    epochId: int
    finalizedSnapshotCid: str
    reason: str


class SnapshotterIncorrectSnapshotSubmission(BaseModel):
    """
    Details of an incorrect snapshot submission by a snapshotter.
    """
    epochId: int
    submittedSnapshotCid: str
    submittedSnapshot: Optional[Dict[str, Any]]
    finalizedSnapshotCid: str
    finalizedSnapshot: Optional[Dict[str, Any]]
    reason: str = ''


class SnapshotterProjectStatus(BaseModel):
    """
    Status of a project for a snapshotter.
    """
    missedSubmissions: List[SnapshotterMissedSnapshotSubmission]
    incorrectSubmissions: List[SnapshotterIncorrectSnapshotSubmission]


class UnfinalizedSnapshot(BaseModel):
    """
    Representation of an unfinalized snapshot.
    """
    snapshotCid: str
    snapshot: Dict[str, Any]


class TaskStatusRequest(BaseModel):
    """
    Request for the status of a task.
    """
    task_type: str
    wallet_address: str


class GenericTxnIssue(BaseModel):
    """
    Representation of a generic transaction issue.
    """
    accountAddress: str
    issueType: str
    projectId: Optional[str]
    epochBegin: Optional[str]
    epochId: Optional[str]
    extra: Optional[str] = ''


class SignRequest(BaseModel):
    """
    Request for signing a snapshot.
    """
    slotId: int
    deadline: int
    snapshotCid: str
    epochId: int
    projectId: str


class TxnPayload(BaseModel):
    """
    Payload for a transaction.
    """
    slotId: int
    projectId: str
    snapshotCid: str
    epochId: int
    request: SignRequest
    signature: str
    contractAddress: str


class SnapshotBatchSubmittedEvent(EventBase):
    """
    Event model for when a snapshot batch is finalized.
    """
    epochId: int
    batchCid: str
    timestamp: int
    transactionHash: str


class TelegramMessage(BaseModel):
    chatId: str
    slotId: int


class TelegramEpochProcessingReportMessage(TelegramMessage):
    issue: SnapshotterIssue


class TelegramSnapshotterCoreReportMessage(TelegramMessage):
    issue: SnapshotterIssue
