"""
记忆服务适配器
根据配置自动选择使用 Zep 云端服务或本地 GraphRAG 服务
"""

from typing import Optional, Dict, Any, List

from ..config import Config
from ..utils.logger import get_logger

logger = get_logger('mirofish.memory_adapter')

# 延迟导入，避免循环依赖
_local_graphrag = None
_zep_tools = None


def get_memory_service():
    """
    获取记忆服务实例
    
    根据 USE_LOCAL_GRAPHRAG 配置返回相应的服务
    
    Returns:
        记忆服务实例 (LocalMemoryService 或 Zep 相关服务)
    """
    global _local_graphrag, _zep_tools
    
    if Config.USE_LOCAL_GRAPHRAG:
        # 使用本地 GraphRAG
        if _local_graphrag is None:
            from .local_graphrag import LocalMemoryService
            _local_graphrag = LocalMemoryService(
                storage_dir=Config.GRAPHRAG_STORAGE_DIR
            )
            logger.info("Using local GraphRAG memory service")
        return _local_graphrag
    else:
        # 使用 Zep 云端服务
        if _zep_tools is None:
            from .zep_tools import ZepTools
            _zep_tools = ZepTools()
            logger.info("Using Zep cloud memory service")
        return _zep_tools


def get_memory_updater(graph_id: str):
    """
    获取记忆更新器
    
    Args:
        graph_id: 图谱ID
    
    Returns:
        记忆更新器实例
    """
    if Config.USE_LOCAL_GRAPHRAG:
        from .local_graphrag import GraphMemoryUpdater, LocalMemoryService
        memory_service = get_memory_service()
        return GraphMemoryUpdater(memory_service)
    else:
        from .zep_graph_memory_updater import ZepGraphMemoryUpdater
        return ZepGraphMemoryUpdater(graph_id)


def get_entity_reader():
    """
    获取实体读取器
    
    Returns:
        实体读取器实例
    """
    if Config.USE_LOCAL_GRAPHRAG:
        from .local_graphrag import LocalMemoryService
        return LocalGraphRAEntityReader(get_memory_service())
    else:
        from .zep_entity_reader import ZepEntityReader
        return ZepEntityReader()


class LocalGraphRAEntityReader:
    """
    本地 GraphRAG 实体读取器
    兼容 ZepEntityReader 接口
    """
    
    def __init__(self, memory_service):
        self.memory_service = memory_service
    
    def read_all_entities(self, entity_types: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        读取所有实体
        
        Args:
            entity_types: 实体类型过滤
        
        Returns:
            实体数据
        """
        entities = self.memory_service.get_all_entities()
        
        # 类型过滤
        if entity_types:
            entities = [e for e in entities if e.get('type') in entity_types]
        
        return {
            "entities": entities,
            "total_count": len(entities),
            "filtered_count": len(entities)
        }
    
    def get_entity_subgraph(self, entity_name: str, depth: int = 2) -> Dict[str, Any]:
        """
        获取实体子图
        
        Args:
            entity_name: 实体名称
            depth: 遍历深度
        
        Returns:
            子图数据
        """
        return self.memory_service.get_entity_subgraph(entity_name, depth)


class MemoryServiceFactory:
    """
    记忆服务工厂
    统一管理记忆服务的创建和配置
    """
    
    _instance = None
    _local_service = None
    _zep_service = None
    
    @classmethod
    def get_instance(cls):
        """获取单例实例"""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance
    
    def get_service(self):
        """获取当前配置的记忆服务"""
        return get_memory_service()
    
    def get_updater(self, graph_id: str):
        """获取记忆更新器"""
        return get_memory_updater(graph_id)
    
    def get_reader(self):
        """获取实体读取器"""
        return get_entity_reader()
    
    @property
    def is_local(self) -> bool:
        """是否使用本地 GraphRAG"""
        return Config.USE_LOCAL_GRAPHRAG
    
    def get_service_info(self) -> Dict[str, Any]:
        """获取服务信息"""
        return {
            "type": "local_graphrag" if self.is_local else "zep_cloud",
            "storage_dir": Config.GRAPHRAG_STORAGE_DIR if self.is_local else None,
            "graph_stats": self.get_service().get_graph_stats() if self.is_local else None
        }
