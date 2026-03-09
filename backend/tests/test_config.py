"""
配置管理单元测试
"""

import os
import pytest
from unittest.mock import patch


class TestConfig:
    """测试配置类"""

    def test_default_values(self):
        """测试默认值"""
        from app.config import Config
        
        assert Config.JSON_AS_ASCII == False
        assert Config.MAX_CONTENT_LENGTH == 50 * 1024 * 1024
        assert Config.DEFAULT_CHUNK_SIZE == 500
        assert Config.DEFAULT_CHUNK_OVERLAP == 50

    def test_flask_config(self):
        """测试 Flask 配置"""
        from app.config import Config
        
        assert hasattr(Config, 'SECRET_KEY')
        assert hasattr(Config, 'DEBUG')
        assert hasattr(Config, 'JSON_AS_ASCII')

    def test_llm_config(self):
        """测试 LLM 配置"""
        from app.config import Config
        
        assert hasattr(Config, 'LLM_API_KEY')
        assert hasattr(Config, 'LLM_BASE_URL')
        assert hasattr(Config, 'LLM_MODEL_NAME')

    def test_zep_config(self):
        """测试 Zep 配置"""
        from app.config import Config
        
        assert hasattr(Config, 'ZEP_API_KEY')

    def test_upload_config(self):
        """测试上传配置"""
        from app.config import Config
        
        assert hasattr(Config, 'MAX_CONTENT_LENGTH')
        assert hasattr(Config, 'UPLOAD_FOLDER')
        assert hasattr(Config, 'ALLOWED_EXTENSIONS')
        
        assert isinstance(Config.ALLOWED_EXTENSIONS, set)
        assert 'pdf' in Config.ALLOWED_EXTENSIONS
        assert 'md' in Config.ALLOWED_EXTENSIONS
        assert 'txt' in Config.ALLOWED_EXTENSIONS

    def test_oasis_config(self):
        """测试 OASIS 配置"""
        from app.config import Config
        
        assert hasattr(Config, 'OASIS_DEFAULT_MAX_ROUNDS')
        assert hasattr(Config, 'OASIS_SIMULATION_DATA_DIR')
        assert hasattr(Config, 'OASIS_TWITTER_ACTIONS')
        assert hasattr(Config, 'OASIS_REDDIT_ACTIONS')
        
        assert isinstance(Config.OASIS_TWITTER_ACTIONS, list)
        assert isinstance(Config.OASIS_REDDIT_ACTIONS, list)
        assert 'CREATE_POST' in Config.OASIS_TWITTER_ACTIONS
        assert 'LIKE_POST' in Config.OASIS_REDDIT_ACTIONS

    def test_report_agent_config(self):
        """测试报告代理配置"""
        from app.config import Config
        
        assert hasattr(Config, 'REPORT_AGENT_MAX_TOOL_CALLS')
        assert hasattr(Config, 'REPORT_AGENT_MAX_REFLECTION_ROUNDS')
        assert hasattr(Config, 'REPORT_AGENT_TEMPERATURE')

    def test_validate_with_missing_keys(self):
        """测试验证缺失的密钥"""
        from app.config import Config
        
        # 在本地 GraphRAG 模式下，只需要检查 LLM_API_KEY
        with patch.object(Config, 'LLM_API_KEY', None):
            with patch.object(Config, 'ZEP_API_KEY', None):
                with patch.object(Config, 'USE_LOCAL_GRAPHRAG', True):
                    errors = Config.validate()
                    
                    assert len(errors) == 1
                    assert any("LLM_API_KEY" in err for err in errors)
    
    def test_validate_with_missing_keys_zep_mode(self):
        """测试Zep模式下验证缺失的密钥"""
        from app.config import Config
        
        # 在 Zep 模式下，需要检查 LLM_API_KEY 和 ZEP_API_KEY
        with patch.object(Config, 'LLM_API_KEY', None):
            with patch.object(Config, 'ZEP_API_KEY', None):
                with patch.object(Config, 'USE_LOCAL_GRAPHRAG', False):
                    errors = Config.validate()
                    
                    assert len(errors) == 2
                    assert any("LLM_API_KEY" in err for err in errors)
                    assert any("ZEP_API_KEY" in err for err in errors)

    def test_validate_with_valid_keys(self):
        """测试验证有效的密钥"""
        from app.config import Config
        
        with patch.object(Config, 'LLM_API_KEY', 'valid-key'):
            with patch.object(Config, 'ZEP_API_KEY', 'valid-key'):
                errors = Config.validate()
                
                assert len(errors) == 0

    def test_debug_mode(self):
        """测试调试模式配置"""
        from app.config import Config
        
        # DEBUG 应该是一个布尔值
        assert isinstance(Config.DEBUG, bool)

    def test_upload_folder_path(self):
        """测试上传文件夹路径"""
        from app.config import Config
        
        assert 'uploads' in Config.UPLOAD_FOLDER
