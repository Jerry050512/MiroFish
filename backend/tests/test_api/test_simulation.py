"""
模拟API测试
测试 simulation.py 中的路由和函数
"""

import pytest
import sys
import os

# 添加 backend 到路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))


class TestSimulationAPI:
    """测试模拟API"""
    
    def test_import_simulation_module(self):
        """测试 simulation 模块可以正确导入"""
        try:
            from app.api.simulation import simulation_bp
            assert simulation_bp is not None
        except IndentationError as e:
            pytest.fail(f"simulation.py 存在缩进错误: {e}")
        except SyntaxError as e:
            pytest.fail(f"simulation.py 存在语法错误: {e}")
        except ImportError as e:
            pytest.fail(f"simulation.py 导入失败: {e}")
    
    def test_get_entity_reader_import(self):
        """测试 get_entity_reader 函数可以正确导入"""
        from app.api.simulation import get_entity_reader
        assert callable(get_entity_reader)
    
    def test_entity_reader_factory_local_mode(self, monkeypatch):
        """测试在本地模式下工厂函数返回 LocalEntityReader"""
        # 模拟本地模式
        monkeypatch.setattr('app.config.Config.USE_LOCAL_GRAPHRAG', True)
        
        from app.services.zep_entity_reader import get_entity_reader
        reader = get_entity_reader()
        
        from app.services.zep_entity_reader import LocalEntityReader
        assert isinstance(reader, LocalEntityReader)
    
    def test_entity_reader_factory_zep_mode(self, monkeypatch):
        """测试在Zep模式下工厂函数返回 ZepEntityReader"""
        # 模拟Zep模式
        monkeypatch.setattr('app.config.Config.USE_LOCAL_GRAPHRAG', False)
        monkeypatch.setattr('app.config.Config.ZEP_API_KEY', 'test_api_key')
        
        from app.services.zep_entity_reader import get_entity_reader
        reader = get_entity_reader()
        
        from app.services.zep_entity_reader import ZepEntityReader
        assert isinstance(reader, ZepEntityReader)


class TestGraphBuilderService:
    """测试图谱构建服务"""
    
    def test_import_graph_builder(self):
        """测试 graph_builder 模块可以正确导入"""
        try:
            from app.services.graph_builder import GraphBuilderService
            assert GraphBuilderService is not None
        except SyntaxError as e:
            pytest.fail(f"graph_builder.py 存在语法错误: {e}")
        except ImportError as e:
            pytest.fail(f"graph_builder.py 导入失败: {e}")
    
    def test_graph_builder_local_mode(self, monkeypatch):
        """测试本地模式下 GraphBuilderService 使用本地 GraphRAG"""
        monkeypatch.setattr('app.config.Config.USE_LOCAL_GRAPHRAG', True)
        
        from app.services.graph_builder import GraphBuilderService
        service = GraphBuilderService()
        
        assert service.use_local is True
        assert service.local_memory is not None
        assert service.client is None
    
    def test_graph_builder_zep_mode(self, monkeypatch):
        """测试Zep模式下 GraphBuilderService 使用 Zep Cloud"""
        monkeypatch.setattr('app.config.Config.USE_LOCAL_GRAPHRAG', False)
        monkeypatch.setattr('app.config.Config.ZEP_API_KEY', 'test_api_key')
        
        from app.services.graph_builder import GraphBuilderService
        service = GraphBuilderService()
        
        assert service.use_local is False
        assert service.client is not None


class TestZepGraphMemoryUpdater:
    """测试图谱记忆更新器"""
    
    def test_import_memory_updater(self):
        """测试 zep_graph_memory_updater 模块可以正确导入"""
        try:
            from app.services.zep_graph_memory_updater import ZepGraphMemoryManager
            assert ZepGraphMemoryManager is not None
        except SyntaxError as e:
            pytest.fail(f"zep_graph_memory_updater.py 存在语法错误: {e}")
        except ImportError as e:
            pytest.fail(f"zep_graph_memory_updater.py 导入失败: {e}")
    
    def test_memory_manager_local_mode(self, monkeypatch):
        """测试本地模式下创建 LocalGraphMemoryUpdater"""
        monkeypatch.setattr('app.config.Config.USE_LOCAL_GRAPHRAG', True)
        
        from app.services.zep_graph_memory_updater import ZepGraphMemoryManager
        from app.services.zep_graph_memory_updater import LocalGraphMemoryUpdater
        
        updater = ZepGraphMemoryManager.create_updater("test_sim", "test_graph")
        assert isinstance(updater, LocalGraphMemoryUpdater)
        
        # 清理
        ZepGraphMemoryManager.stop_updater("test_sim")


class TestConfigValidation:
    """测试配置验证"""
    
    def test_validate_local_mode_no_zep_key(self, monkeypatch):
        """测试本地模式下不需要 ZEP_API_KEY"""
        from app.config import Config
        
        monkeypatch.setattr(Config, 'LLM_API_KEY', 'test_key')
        monkeypatch.setattr(Config, 'ZEP_API_KEY', None)
        monkeypatch.setattr(Config, 'USE_LOCAL_GRAPHRAG', True)
        
        errors = Config.validate()
        
        # 本地模式下不应该有错误
        assert len(errors) == 0, f"本地模式下不应有配置错误: {errors}"
    
    def test_validate_zep_mode_requires_zep_key(self, monkeypatch):
        """测试 Zep 模式下需要 ZEP_API_KEY"""
        from app.config import Config
        
        monkeypatch.setattr(Config, 'LLM_API_KEY', 'test_key')
        monkeypatch.setattr(Config, 'ZEP_API_KEY', None)
        monkeypatch.setattr(Config, 'USE_LOCAL_GRAPHRAG', False)
        
        errors = Config.validate()
        
        # Zep 模式下应该有 ZEP_API_KEY 错误
        assert len(errors) == 1
        assert any("ZEP_API_KEY" in err for err in errors)


class TestZepEntityReader:
    """测试实体读取器"""
    
    def test_import_entity_reader(self):
        """测试 zep_entity_reader 模块可以正确导入"""
        try:
            from app.services.zep_entity_reader import (
                ZepEntityReader, 
                LocalEntityReader,
                get_entity_reader,
                BaseEntityReader
            )
            assert ZepEntityReader is not None
            assert LocalEntityReader is not None
            assert callable(get_entity_reader)
            assert BaseEntityReader is not None
        except SyntaxError as e:
            pytest.fail(f"zep_entity_reader.py 存在语法错误: {e}")
        except ImportError as e:
            pytest.fail(f"zep_entity_reader.py 导入失败: {e}")


class TestSyntaxValidation:
    """测试语法验证"""
    
    def test_all_modules_syntax(self):
        """测试所有修改过的模块语法正确"""
        import py_compile
        import tempfile
        
        modules = [
            'app/api/simulation.py',
            'app/services/graph_builder.py',
            'app/services/zep_graph_memory_updater.py',
            'app/services/zep_entity_reader.py',
        ]
        
        backend_dir = os.path.join(os.path.dirname(__file__), '../..')
        
        for module in modules:
            module_path = os.path.join(backend_dir, module)
            if os.path.exists(module_path):
                try:
                    # 使用 py_compile 检查语法
                    py_compile.compile(module_path, doraise=True)
                except py_compile.PyCompileError as e:
                    pytest.fail(f"{module} 存在语法错误: {e}")


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
