import os
from dataclasses import dataclass


def _as_bool(value: str, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Settings:
    db_path: str = "./data/mvp_pipeline.db"
    host: str = "127.0.0.1"
    port: int = 8787

    fit_threshold: int = 7
    ingest_provider: str = "coze"
    collab_provider: str = "feishu"
    video_provider: str = "kling"

    feishu_app_id: str = ""
    feishu_app_secret: str = ""
    feishu_app_token: str = ""
    feishu_table_id: str = ""

    callback_verify_mode: str = "permissive"
    callback_shared_secret: str = ""
    slack_signing_secret: str = ""
    feishu_callback_token: str = ""
    feishu_encrypt_key: str = ""
    wecom_callback_token: str = ""
    qq_callback_token: str = ""
    discord_callback_token: str = ""

    coze_verify_mode: str = "permissive"
    coze_signing_secret: str = ""
    coze_workflow_token: str = ""
    coze_workflow_run_url: str = ""
    coze_workflow_graph_parameter_url: str = ""
    coze_workflow_timeout_seconds: int = 60
    coze_workflow_default_inputs_json: str = "{}"
    collector_timeout_seconds: int = 30
    collector_default_max_videos: int = 10
    collector_default_source: str = "native-collector"

    openclaw_enabled: bool = False
    openclaw_base_url: str = ""
    openclaw_api_key: str = ""
    openclaw_ip_host_agent_id: str = "ip-host"
    openclaw_inject_path: str = "/api/system-events/inject"
    openclaw_spawn_path: str = "/api/sessions/spawn"

    retry_base_delay_seconds: int = 30
    retry_max_attempts: int = 5
    stuck_timeout_minutes: int = 30

    publish_webhook_url: str = ""
    publish_webhook_token: str = ""
    publish_provider: str = "webhook"

    bilibili_profile_dir: str = "./data/publish_profiles/bilibili"
    bilibili_profile_name: str = "default"
    bilibili_headless: bool = False
    bilibili_wait_login_seconds: int = 300
    bilibili_publish_timeout_seconds: int = 900
    bilibili_chromium_executable: str = ""
    bilibili_channel: str = "chromium"

    kling_api_key: str = ""
    kling_api_base_url: str = "https://api.klingai.com"
    kling_poll_interval_seconds: int = 6
    kling_timeout_seconds: int = 300

    image_api_provider: str = "keling"
    image_api_base_url: str = ""
    image_api_key: str = ""
    image_api_timeout_seconds: int = 120

    tts_mode: str = "mock"



def load_settings() -> Settings:
    return Settings(
        db_path=os.getenv("MVP_DB_PATH", "./data/mvp_pipeline.db"),
        host=os.getenv("MVP_HOST", "127.0.0.1"),
        port=int(os.getenv("MVP_PORT", "8787")),
        fit_threshold=int(os.getenv("FIT_SCORE_THRESHOLD", "7")),
        ingest_provider=os.getenv("INGEST_PROVIDER", "coze"),
        collab_provider=os.getenv("COLLAB_PROVIDER", "feishu"),
        video_provider=os.getenv("VIDEO_PROVIDER", "kling"),
        feishu_app_id=os.getenv("FEISHU_APP_ID", ""),
        feishu_app_secret=os.getenv("FEISHU_APP_SECRET", ""),
        feishu_app_token=os.getenv("FEISHU_APP_TOKEN", ""),
        feishu_table_id=os.getenv("FEISHU_TABLE_ID", ""),
        callback_verify_mode=os.getenv("CALLBACK_VERIFY_MODE", "permissive"),
        callback_shared_secret=os.getenv("CALLBACK_SHARED_SECRET", ""),
        slack_signing_secret=os.getenv("SLACK_SIGNING_SECRET", ""),
        feishu_callback_token=os.getenv("FEISHU_CALLBACK_TOKEN", ""),
        feishu_encrypt_key=os.getenv("FEISHU_ENCRYPT_KEY", ""),
        wecom_callback_token=os.getenv("WECOM_CALLBACK_TOKEN", ""),
        qq_callback_token=os.getenv("QQ_CALLBACK_TOKEN", ""),
        discord_callback_token=os.getenv("DISCORD_CALLBACK_TOKEN", ""),
        coze_verify_mode=os.getenv("COZE_VERIFY_MODE", "permissive"),
        coze_signing_secret=os.getenv("COZE_SIGNING_SECRET", ""),
        coze_workflow_token=os.getenv("COZE_WORKFLOW_TOKEN", ""),
        coze_workflow_run_url=os.getenv("COZE_WORKFLOW_RUN_URL", ""),
        coze_workflow_graph_parameter_url=os.getenv("COZE_WORKFLOW_GRAPH_PARAMETER_URL", ""),
        coze_workflow_timeout_seconds=int(os.getenv("COZE_WORKFLOW_TIMEOUT_SECONDS", "60")),
        coze_workflow_default_inputs_json=os.getenv("COZE_WORKFLOW_DEFAULT_INPUTS_JSON", "{}"),
        collector_timeout_seconds=int(os.getenv("COLLECTOR_TIMEOUT_SECONDS", "30")),
        collector_default_max_videos=int(os.getenv("COLLECTOR_DEFAULT_MAX_VIDEOS", "10")),
        collector_default_source=os.getenv("COLLECTOR_DEFAULT_SOURCE", "native-collector"),
        openclaw_enabled=_as_bool(os.getenv("OPENCLAW_ENABLED", "false"), False),
        openclaw_base_url=os.getenv("OPENCLAW_BASE_URL", ""),
        openclaw_api_key=os.getenv("OPENCLAW_API_KEY", ""),
        openclaw_ip_host_agent_id=os.getenv("OPENCLAW_IP_HOST_AGENT_ID", "ip-host"),
        openclaw_inject_path=os.getenv("OPENCLAW_INJECT_PATH", "/api/system-events/inject"),
        openclaw_spawn_path=os.getenv("OPENCLAW_SPAWN_PATH", "/api/sessions/spawn"),
        retry_base_delay_seconds=int(os.getenv("RETRY_BASE_DELAY_SECONDS", "30")),
        retry_max_attempts=int(os.getenv("RETRY_MAX_ATTEMPTS", "5")),
        stuck_timeout_minutes=int(os.getenv("STUCK_TIMEOUT_MINUTES", "30")),
        publish_webhook_url=os.getenv("PUBLISH_WEBHOOK_URL", ""),
        publish_webhook_token=os.getenv("PUBLISH_WEBHOOK_TOKEN", ""),
        publish_provider=os.getenv("PUBLISH_PROVIDER", "webhook"),
        bilibili_profile_dir=os.getenv("BILIBILI_PROFILE_DIR", "./data/publish_profiles/bilibili"),
        bilibili_profile_name=os.getenv("BILIBILI_PROFILE_NAME", "default"),
        bilibili_headless=_as_bool(os.getenv("BILIBILI_HEADLESS", "false"), False),
        bilibili_wait_login_seconds=int(os.getenv("BILIBILI_WAIT_LOGIN_SECONDS", "300")),
        bilibili_publish_timeout_seconds=int(os.getenv("BILIBILI_PUBLISH_TIMEOUT_SECONDS", "900")),
        bilibili_chromium_executable=os.getenv("BILIBILI_CHROMIUM_EXECUTABLE", ""),
        bilibili_channel=os.getenv("BILIBILI_CHANNEL", "chromium"),
        kling_api_key=os.getenv("KLING_API_KEY", ""),
        kling_api_base_url=os.getenv("KLING_API_BASE_URL", "https://api.klingai.com"),
        kling_poll_interval_seconds=int(os.getenv("KLING_POLL_INTERVAL_SECONDS", "6")),
        kling_timeout_seconds=int(os.getenv("KLING_TIMEOUT_SECONDS", "300")),
        image_api_provider=os.getenv("IMAGE_API_PROVIDER", "keling"),
        image_api_base_url=os.getenv("IMAGE_API_BASE_URL", ""),
        image_api_key=os.getenv("IMAGE_API_KEY", ""),
        image_api_timeout_seconds=int(os.getenv("IMAGE_API_TIMEOUT_SECONDS", "120")),
        tts_mode=os.getenv("TTS_MODE", "mock"),
    )
