"""
D5: 配置环境变量化 - 测试
- .env 文件加载
- env() 函数
- 敏感配置从环境变量读取
- .env.example 模板完整性
- DATABASE_PATH 可覆盖
"""
import pytest
import os
from pathlib import Path
import sys

PROJECT_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_DIR))


@pytest.fixture(autouse=True)
def _clean_env():
    """每个测试前后清理相关环境变量"""
    env_keys = [
        'DATABASE_PATH', 'TDX_EXPORT_DIR',
        'EMAIL_ENABLED', 'EMAIL_SMTP_SERVER', 'EMAIL_SMTP_PORT',
        'EMAIL_USERNAME', 'EMAIL_PASSWORD', 'EMAIL_RECIPIENTS',
        'WECHAT_ENABLED', 'WECHAT_WEBHOOK_URL',
        'ALERT_DEDUP_INTERVAL_HOURS', 'STALE_THRESHOLD_DAYS',
        'ADVICE_ENABLED',
    ]
    saved = {}
    for k in env_keys:
        saved[k] = os.environ.pop(k, None)
    yield
    for k, v in saved.items():
        if v is not None:
            os.environ[k] = v
        elif k in os.environ:
            del os.environ[k]


class TestEnvFunction:
    """测试 env() 辅助函数"""

    def test_env_returns_default_when_unset(self):
        from config.settings import env
        assert env('D5_TEST_NONEXISTENT', 'default_val') == 'default_val'

    def test_env_returns_none_when_unset_no_default(self):
        from config.settings import env
        assert env('D5_TEST_NONEXISTENT2') is None

    def test_env_reads_system_env(self):
        os.environ['D5_TEST_VAR'] = 'hello_env'
        from config.settings import env
        assert env('D5_TEST_VAR') == 'hello_env'


class TestSettingsEnvOverride:
    """测试 settings.py 配置可被环境变量覆盖"""

    def test_database_path_default(self):
        from config.settings import DATABASE_PATH
        assert 'portfolio.db' in str(DATABASE_PATH)

    def test_email_enabled_false_by_default(self):
        from config.settings import NOTIFICATION_CONFIG
        assert NOTIFICATION_CONFIG['email']['enabled'] is False

    def test_email_password_empty_by_default(self):
        from config.settings import NOTIFICATION_CONFIG
        assert NOTIFICATION_CONFIG['email']['password'] == ''

    def test_wechat_disabled_by_default(self):
        from config.settings import NOTIFICATION_CONFIG
        assert NOTIFICATION_CONFIG['wechat']['enabled'] is False

    def test_wechat_webhook_empty_by_default(self):
        from config.settings import NOTIFICATION_CONFIG
        assert NOTIFICATION_CONFIG['wechat']['webhook_url'] == ''

    def test_monitor_dedup_default(self):
        from config.settings import MONITOR_CONFIG
        assert MONITOR_CONFIG.get('dedup_interval_hours') == 6

    def test_advice_enabled_default(self):
        from config.settings import SMART_ANALYSIS_CONFIG
        assert SMART_ANALYSIS_CONFIG['advice_enabled'] is True


class TestEnvFileLoading:
    """测试 .env 文件加载机制"""

    def test_env_file_loading(self):
        """创建临时 .env 文件到 PROJECT_DIR，验证加载"""
        import config.settings as cs
        env_file = cs.PROJECT_ROOT / ".env"
        env_file.write_text(
            "D5_TEST_FROM_FILE=test_value\n"
            "D5_TEST_NUM=42\n"
            "# This is a comment\n"
            "D5_TEST_EMPTY=\n",
            encoding='utf-8'
        )
        try:
            cs._load_env_file()
            assert os.environ.get('D5_TEST_FROM_FILE') == 'test_value'
            assert os.environ.get('D5_TEST_NUM') == '42'
            assert os.environ.get('D5_TEST_EMPTY') == ''
        finally:
            if env_file.exists():
                env_file.unlink()

    def test_env_file_does_not_override_existing(self):
        os.environ['D5_TEST_NO_OVERRIDE'] = 'original'
        import config.settings as cs
        env_file = cs.PROJECT_ROOT / ".env"
        env_file.write_text("D5_TEST_NO_OVERRIDE=should_not_override\n", encoding='utf-8')
        try:
            cs._load_env_file()
            assert os.environ.get('D5_TEST_NO_OVERRIDE') == 'original'
        finally:
            if env_file.exists():
                env_file.unlink()


class TestEnvExampleTemplate:
    """测试 .env.example 模板文件"""

    def test_env_example_exists(self):
        p = PROJECT_DIR / ".env.example"
        assert p.exists(), ".env.example 模板文件必须存在"

    def test_env_example_has_database_section(self):
        content = (PROJECT_DIR / ".env.example").read_text(encoding='utf-8')
        assert 'DATABASE_PATH' in content

    def test_env_example_has_email_section(self):
        content = (PROJECT_DIR / ".env.example").read_text(encoding='utf-8')
        assert 'EMAIL_SMTP_SERVER' in content
        assert 'EMAIL_USERNAME' in content
        assert 'EMAIL_PASSWORD' in content

    def test_env_example_has_wechat_section(self):
        content = (PROJECT_DIR / ".env.example").read_text(encoding='utf-8')
        assert 'WECHAT_WEBHOOK_URL' in content

    def test_env_example_has_monitor_section(self):
        content = (PROJECT_DIR / ".env.example").read_text(encoding='utf-8')
        assert 'ALERT_DEDUP_INTERVAL_HOURS' in content or 'ALERT_' in content

    def test_env_example_no_real_credentials(self):
        content = (PROJECT_DIR / ".env.example").read_text(encoding='utf-8')
        # Should not contain real passwords or tokens
        assert '@qq.com' not in content or 'your_email' in content.lower()


class TestGitignoreExcludesEnv:
    """验证 .gitignore 排除 .env"""

    def test_gitignore_has_env(self):
        gi = PROJECT_DIR / ".gitignore"
        content = gi.read_text(encoding='utf-8')
        assert '.env' in content

    def test_gitignore_has_env_local(self):
        gi = PROJECT_DIR / ".gitignore"
        content = gi.read_text(encoding='utf-8')
        assert '.env.local' in content

    def test_gitignore_allows_env_example(self):
        gi = PROJECT_DIR / ".gitignore"
        content = gi.read_text(encoding='utf-8')
        # .env.example should NOT be excluded
        lines = content.splitlines()
        env_only = [l.strip() for l in lines if l.strip() == '.env']
        # Check it's not a blanket *.env pattern
        assert not any('*.env' in l for l in lines)


class TestEnvIntegration:
    """集成测试：环境变量覆盖配置的完整流程"""

    def test_email_enabled_via_env(self):
        os.environ['EMAIL_ENABLED'] = 'true'
        # Need to reimport to pick up new env
        import importlib
        import config.settings
        importlib.reload(config.settings)
        assert config.settings.NOTIFICATION_CONFIG['email']['enabled'] is True

    def test_email_password_via_env(self):
        os.environ['EMAIL_PASSWORD'] = 'test_secret_123'
        import importlib
        import config.settings
        importlib.reload(config.settings)
        assert config.settings.NOTIFICATION_CONFIG['email']['password'] == 'test_secret_123'

    def test_monitor_dedup_via_env(self):
        os.environ['ALERT_DEDUP_INTERVAL_HOURS'] = '24'
        import importlib
        import config.settings
        importlib.reload(config.settings)
        assert config.settings.MONITOR_CONFIG['dedup_interval_hours'] == 24
