from enum import Enum
from typing import List, Literal
from typing import Optional
from typing import Union

from ipfs_client.settings.data_models import IPFSConfig
from pydantic import BaseModel
from pydantic import Field
from pydantic import computed_field
from pydantic_settings import BaseSettings
from pydantic_settings import SettingsConfigDict
from rpc_helper.utils.models.settings_model import RPCConfigBase
from rpc_helper.utils.models.settings_model import RPCConfigFull


class Auth(BaseModel):
    """Authentication configuration model."""
    enabled: bool = Field(True, description='Whether auth is enabled or not')
    header_key: str = Field('X-API-KEY', description='Key used for auth')


class CoreAPI(BaseModel):
    """Core API configuration model."""
    host: str
    port: int
    auth: Auth
    public_rate_limit: str


class RLimit(BaseModel):
    """Resource limit configuration model."""
    file_descriptors: int


class Timeouts(BaseModel):
    """Timeout configuration model."""
    basic: int
    archival: int
    connection_init: int


class QueueConfig(BaseModel):
    """Queue configuration model."""
    num_instances: int


class ReportingConfig(BaseModel):
    """Reporting configuration model."""
    telegram_url: str
    telegram_chat_id: str
    min_reporting_interval: int


class Redis(BaseModel):
    """Redis configuration model."""
    host: str
    port: int
    db: int
    password: Union[str, None] = None
    ssl: bool = False
    cluster_mode: bool = False


class RedisReader(BaseModel):
    """Redis reader configuration model."""
    host: str
    port: int
    db: int
    password: Union[str, None] = None
    ssl: bool = False
    cluster_mode: bool = False


class Logs(BaseModel):
    """Logging configuration model."""
    debug_mode: bool
    write_to_files: bool


class AsyncTaskConfig(BaseModel):
    """Async task configuration model."""
    task_timeout: int
    task_cleanup_interval: int


class EventContract(BaseModel):
    """Event contract configuration model."""
    address: str
    abi: str
    deadline_buffer: int
    day_counter_buffer: int


class CallbackWorkerConfig(BaseModel):
    """Callback worker configuration model."""
    num_snapshot_workers: int
    num_aggregation_workers: int


class IPFSWriterRateLimit(BaseModel):
    """IPFS writer rate limit configuration model."""
    req_per_sec: int
    burst: int


class ExternalAPIAuth(BaseModel):
    """External API authentication configuration model."""
    apiKey: str
    apiSecret: str = ''  # This is most likely used as a basic auth tuple of (username, password)


class RelayerService(BaseModel):
    """Relayer service configuration model."""
    host: str
    port: str
    keepalive_secs: int


class SignerConfig(BaseModel):
    """Signer configuration model."""
    address: str
    private_key: str


class TxSubmissionConfig(BaseModel):
    """Transaction submission configuration model."""
    enabled: bool = False
    # relayer: RelayerService
    signers: List[SignerConfig] = []


class HTTPXConfig(BaseModel):
    """HTTPX client configuration model."""
    pool_timeout: int
    connect_timeout: int
    read_timeout: int
    write_timeout: int


class IPFSUnpinningConfig(BaseModel):
    """IPFS unpinning configuration model."""
    enabled: bool
    unpin_after: int


class MppConfig(BaseSettings):
    """MPP (Machine Payment Protocol) — env MPP_* overrides optional JSON under settings.mpp."""

    model_config = SettingsConfigDict(env_prefix="MPP_", extra="ignore")

    enabled: bool = False
    charge_amount: str = "0.01"
    # One Tempo charge per SSE connection for /mpp/stream/... (flat session fee).
    stream_amount: str = "0.0001"
    tempo_recipient: str = ""
    tempo_currency: str = ""
    # pympp defaults to mainnet (4217) if unset; must match where the payer is funded.
    tempo_chain_id: int = 42431  # Moderato testnet; use 4217 for Tempo mainnet
    # Tempo JSON-RPC URL — set explicitly for testnet vs mainnet alongside MPP_TEMPO_CHAIN_ID.
    tempo_rpc_url: str = "https://rpc.moderato.tempo.xyz"
    protected_paths: str = (
        "/mpp/snapshot/base,/mpp/snapshot/allTrades,/mpp/snapshot/trades,/mpp/stream/allTrades"
    )
    # tempo = pympp + Tempo ChargeIntent (default). signup_api = deduct credits via bds-agent-signup HTTP.
    billing_mode: Literal["tempo", "signup_api"] = "tempo"
    signup_billing_base_url: str = ""
    internal_billing_secret: str = ""

    @computed_field
    @property
    def protected_paths_list(self) -> List[str]:
        return [p.strip() for p in self.protected_paths.split(",") if p.strip()]


class Settings(BaseModel):
    """Main settings configuration model."""
    namespace: str
    signer_private_key: str
    core_api: CoreAPI
    instance_id: str
    slot_id: int
    async_task_config: AsyncTaskConfig
    rpc: RPCConfigFull
    local_collector_port: int
    rlimit: RLimit
    httpx: HTTPXConfig
    reporting: ReportingConfig
    health_report_interval: int
    redis: Redis
    redis_reader: RedisReader
    logs: Logs
    data_market: str
    projects_config_path: str
    preloader_config_path: str
    aggregator_config_path: str
    protocol_state: EventContract
    ipfs: IPFSConfig
    ipfs_unpinning: IPFSUnpinningConfig
    node_version: str
    anchor_chain_rpc: RPCConfigBase
    block_shift_for_bitmap_index: int
    mpp: MppConfig = Field(default_factory=MppConfig)

# Projects related models


class ProcessorConfig(BaseModel):
    """Processor configuration model."""
    module: str
    class_name: str


class ProjectConfig(BaseModel):
    """Project configuration model."""
    project_name: str
    keep_previous_snapshot_data: bool = False
    cache_cids: bool = False
    processor: ProcessorConfig
    preload_tasks: List[str]


class ProjectsConfig(BaseModel):
    """Projects configuration model."""
    config: List[ProjectConfig]


class AggregateFilterConfig(BaseModel):
    """Aggregate filter configuration model."""
    projectId: str


class AggregationConfig(BaseModel):
    """Aggregation configuration model."""
    project_name: str
    depends_on: str
    processor: ProcessorConfig
    keep_previous_snapshot_data: bool = False
    cache_cids: bool = False


class AggregatorConfig(BaseModel):
    """Aggregator configuration model."""
    config: List[AggregationConfig]


class Preloader(BaseModel):
    """Preloader configuration model."""
    task_type: str
    module: str
    class_name: str


class DelegatedTask(BaseModel):
    """Delegated task configuration model."""
    task_type: str
    module: str
    class_name: str


class PreloaderConfig(BaseModel):
    """Preloader configuration model."""
    preloaders: List[Preloader]
    timeout: int
