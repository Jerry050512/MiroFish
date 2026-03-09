"""
本地 GraphRAG 记忆服务
提供私有化部署的图谱记忆功能，替代云端 Zep 服务

基于 Microsoft GraphRAG 架构设计，包含以下核心组件:

1. **存储层 (store.py)**
   - GraphStore: 基于 NetworkX 的图数据库存储
   - 支持 JSON 文件持久化
   - 实体、关系、文本块、社区的存储管理

2. **索引管道 (indexer.py)**
   - TextChunker: 文本分块
   - GraphExtractor: 实体和关系提取
   - IndexingPipeline: 完整的索引流程
   - CommunityManager: 社区检测和摘要生成

3. **查询引擎 (retriever.py)**
   - LocalSearcher: 基于实体的局部搜索
   - GlobalSearcher: 基于社区摘要的全局搜索
   - GraphRetriever: 统一的检索接口

4. **服务层 (memory_service.py)**
   - LocalMemoryService: 与 Zep 兼容的统一接口
   - GraphMemoryUpdater: 模拟过程中的实时记忆更新

使用示例:
    ```python
    from app.services.local_graphrag import LocalMemoryService
    
    # 初始化服务
    memory = LocalMemoryService(storage_dir="./data/graphrag")
    
    # 添加记忆
    memory_id = memory.add_memory("张三是一名软件工程师，在阿里巴巴工作。")
    
    # 搜索记忆
    results = memory.search("张三的工作是什么？", search_type="local")
    
    # 获取实体信息
    entity = memory.get_entity("张三")
    ```
"""

from .models import (
    Entity,
    Relation,
    TextChunk,
    Community,
    SearchResult,
    ExtractedEntity,
    ExtractedRelation,
    generate_id,
)
from .store import GraphStore
from .extractor import EntityExtractor, RelationExtractor, GraphExtractor
from .indexer import IndexingPipeline, TextChunker
from .retriever import GraphRetriever, LocalSearchResult, GlobalSearchResult
from .memory_service import LocalMemoryService, GraphMemoryUpdater, AgentActivity
from .community import CommunityDetector, CommunitySummarizer, CommunityManager

__version__ = "0.1.0"

__all__ = [
    # 数据模型
    "Entity",
    "Relation",
    "TextChunk",
    "Community",
    "SearchResult",
    "ExtractedEntity",
    "ExtractedRelation",
    "generate_id",
    # 存储层
    "GraphStore",
    # 提取器
    "EntityExtractor",
    "RelationExtractor",
    "GraphExtractor",
    # 索引管道
    "IndexingPipeline",
    "TextChunker",
    # 检索器
    "GraphRetriever",
    "LocalSearchResult",
    "GlobalSearchResult",
    # 社区
    "CommunityDetector",
    "CommunitySummarizer",
    "CommunityManager",
    # 服务层
    "LocalMemoryService",
    "GraphMemoryUpdater",
    "AgentActivity",
]
