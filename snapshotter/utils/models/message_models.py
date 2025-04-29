from typing import List
from typing import Optional

from pydantic import BaseModel
from pydantic import Field


class TxLogsModel(BaseModel):
    """Model representing transaction logs."""
    logIndex: str
    blockNumber: str
    blockHash: str
    transactionHash: str
    transactionIndex: str
    address: str
    data: str
    topics: List[str]


class EthTransactionReceipt(BaseModel):
    """Model representing an Ethereum transaction receipt."""
    transactionHash: str
    transactionIndex: str
    blockHash: str
    blockNumber: str
    from_field: str = Field(..., alias='from')  # 'from' is a reserved keyword in Python
    to: Optional[str]
    cumulativeGasUsed: str
    gasUsed: str
    effectiveGasPrice: str
    logs: List[TxLogsModel]
    contractAddress: Optional[str] = None
    logsBloom: str
    status: str
    type: Optional[str]
    root: Optional[str]


class EpochBase(BaseModel):
    """Base model for epoch-related data."""
    epochId: int
    begin: int
    end: int


class SnapshotProcessMessage(EpochBase):
    """Model for snapshot process messages."""
    data_source: Optional[str] = None
    primary_data_source: Optional[str] = None


class SnapshotFinalizedMessage(BaseModel):
    """Model for snapshot finalized messages."""
    epochId: int
    epochEnd: int
    projectId: str
    snapshotCid: str
    timestamp: int


class SnapshotBatchSubmittedMessage(BaseModel):
    """Model for snapshot batch submitted messages."""
    epochId: int
    batchCid: str
    timestamp: int
    transactionHash: str


class SnapshotSubmittedMessage(BaseModel):
    """Model for snapshot submission messages."""
    snapshotCid: str
    epochId: int
    projectId: str
    timestamp: int


class CalculateAggregateMessage(BaseModel):
    """Model for calculate aggregate messages."""
    messages: List[SnapshotSubmittedMessage]
    epochId: int
    timestamp: int


class AggregateBase(BaseModel):
    """Base model for aggregate-related data."""
    epochId: int


class PayloadCommitMessage(BaseModel):
    """Model for payload commit messages."""
    sourceChainId: int
    projectId: str
    epochId: int
    snapshotCID: str


class PayloadCommitFinalizedMessage(BaseModel):
    """Model for payload commit finalized messages."""
    message: SnapshotFinalizedMessage
    sourceChainId: int
