from enum import Enum
from typing import Any
from typing import Dict
from typing import Optional

from pydantic import BaseModel


class SnapshotStatus(Enum):
    """
    Represents the status of a snapshot.
    """
    SUBMITTED = 0
    SEQUENCER_FINALIZED = 1
    FINALIZED = 2
    NULL = -1


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
    SNAPSHOT_SEQUENCER_FINALIZE = 'SNAPSHOT_SEQUENCER_FINALIZE'
    SNAPSHOT_FINALIZE = 'SNAPSHOT_FINALIZE'
    SNAPSHOT_SUBMIT_COLLECTOR = 'SNAPSHOT_SUBMIT_COLLECTOR'


class SnapshotterStateUpdate(BaseModel):
    """
    Update for snapshotter state.
    """
    status: str
    error: Optional[str] = None
    extra: Optional[Dict[str, Any]] = None
    timestamp: int


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


# --- EIP-712 related models ---
class EIP712Domain(BaseModel):
    name: str
    version: str
    chainId: int
    verifyingContract: str # Should be checksummed address


class EIPRequest(BaseModel):
    slotId: int
    deadline: int
    snapshotCid: str
    epochId: int
    projectId: str


class EpochBaseSnapshot(BaseModel):
    """Represents a block range for an epoch."""
    begin: int  # Start of the epoch 
    end: int    # End of the epoch 


class EpochIdentifier(BaseModel):
    """
    Identifies a specific epoch and its associated snapshot CID.
    """
    epoch_id: int
    snapshot_cid: str


class ClosestEpochs(BaseModel):
    """
    Represents the closest epochs (before and after) to a requested epoch when an exact match is not found.
    """
    previous: Optional[EpochIdentifier] = None  # The closest epoch before the requested epoch
    next: Optional[EpochIdentifier] = None  # The closest epoch after the requested epoch


class ExactEpochSnapshot(BaseModel):
    """
    Represents a snapshot that exactly matches the requested epoch.
    """
    epoch_id: int
    snapshot_cid: str
    data: Dict[str, Any]


class EpochSnapshotResponse(BaseModel):
    """
    A union type response that represents either:
    1. An exact match for the requested epoch
    2. The closest epochs when seek=True and no exact match exists
    3. No data found
    """
    exact_match: Optional[ExactEpochSnapshot] = None  # Present only when exact epoch match is found
    closest_epochs: Optional[ClosestEpochs] = None  # Present only when seek=True and no exact match
    
    @property
    def has_data(self) -> bool:
        """Returns whether this response contains any useful data"""
        return self.exact_match is not None or self.closest_epochs is not None
    
    @property
    def is_exact_match(self) -> bool:
        """Returns whether this response contains an exact epoch match"""
        return self.exact_match is not None
    
    @property
    def has_closest_epochs(self) -> bool:
        """Returns whether this response contains closest epoch information"""
        return self.closest_epochs is not None and (
            self.closest_epochs.previous is not None or 
            self.closest_epochs.next is not None
        )


class PreloaderResult(BaseModel):
    """Result from a preloader task (e.g. eth_price)."""
    result: Optional[Dict[str, Any]] = None


class BlockSearchType(Enum):
    """
    Represents the type of block search to perform when fetching a block at a given timestamp.
    """
    BEFORE_OR_AT = 1
    AFTER_OR_AT = 2
