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
