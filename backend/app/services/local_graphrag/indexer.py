"""
GraphRAG 索引管道
实现完整的索引流程：文本分块 -> 实体提取 -> 关系抽取 -> 社区检测 -> 生成摘要
"""

import hashlib
from typing import List, Optional, Callable

from ...utils.llm_client import LLMClient
from ...utils.logger import get_logger
from .models import (
    Entity, Relation, TextChunk, Community,
    generate_id, ExtractedEntity, ExtractedRelation
)
from .store import GraphStore
from .extractor import GraphExtractor
from .community import CommunityManager

logger = get_logger('mirofish.graphrag.indexer')


class TextChunker:
    """文本分块器"""
    
    def __init__(
        self,
        chunk_size: int = 500,
        chunk_overlap: int = 50
    ):
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
    
    def chunk_text(self, text: str, document_id: str = "") -> List[TextChunk]:
        """
        将文本分块
        
        Args:
            text: 输入文本
            document_id: 文档ID
        
        Returns:
            文本块列表
        """
        chunks = []
        
        # 简单的滑动窗口分块
        start = 0
        index = 0
        
        while start < len(text):
            end = start + self.chunk_size
            chunk_text = text[start:end]
            
            # 生成ID
            chunk_id = hashlib.md5(
                f"{document_id}_{index}_{chunk_text[:50]}".encode()
            ).hexdigest()
            
            chunk = TextChunk(
                id=chunk_id,
                text=chunk_text,
                index=index,
                document_id=document_id
            )
            chunks.append(chunk)
            
            # 滑动窗口
            start = end - self.chunk_overlap
            index += 1
            
            # 避免无限循环
            if start >= end:
                break
        
        logger.info(f"Chunked text into {len(chunks)} chunks")
        return chunks


class IndexingPipeline:
    """
    GraphRAG 索引管道
    
    完整流程：
    1. 文本分块
    2. 实体提取
    3. 关系抽取
    4. 保存到图存储
    5. 社区检测
    6. 生成社区摘要
    """
    
    def __init__(
        self,
        graph_store: GraphStore,
        llm_client: Optional[LLMClient] = None,
        chunk_size: int = 500,
        chunk_overlap: int = 50
    ):
        self.graph_store = graph_store
        self.llm_client = llm_client or LLMClient()
        
        # 初始化组件
        self.chunker = TextChunker(chunk_size, chunk_overlap)
        self.extractor = GraphExtractor(self.llm_client)
        self.community_manager = CommunityManager(graph_store, self.llm_client)
        
        # 统计信息
        self.stats = {
            "texts_processed": 0,
            "chunks_created": 0,
            "entities_extracted": 0,
            "relations_extracted": 0,
            "communities_created": 0,
        }
    
    def index_text(
        self,
        text: str,
        document_id: str = "",
        progress_callback: Optional[Callable[[str, int, int], None]] = None
    ):
        """
        索引单个文本
        
        Args:
            text: 文本内容
            document_id: 文档ID
            progress_callback: 进度回调函数 (stage, current, total)
        """
        logger.info(f"Indexing text (doc_id={document_id}, length={len(text)})")
        
        # 1. 文本分块
        if progress_callback:
            progress_callback("chunking", 0, 100)
        
        chunks = self.chunker.chunk_text(text, document_id)
        self.stats["chunks_created"] += len(chunks)
        
        if progress_callback:
            progress_callback("chunking", 100, 100)
        
        # 2. 处理每个文本块
        total_chunks = len(chunks)
        for i, chunk in enumerate(chunks):
            if progress_callback:
                progress_callback("extracting", i, total_chunks)
            
            self._process_chunk(chunk)
        
        if progress_callback:
            progress_callback("extracting", total_chunks, total_chunks)
        
        # 3. 保存数据
        if progress_callback:
            progress_callback("saving", 0, 100)
        
        self.graph_store.save_data()
        
        if progress_callback:
            progress_callback("saving", 100, 100)
        
        self.stats["texts_processed"] += 1
        
        logger.info(f"Text indexed successfully: {document_id}")
    
    def _process_chunk_from_text(self, text: str, document_id: str = "", index: int = 0):
        """从文本直接创建并处理块"""
        chunk_id = hashlib.md5(f"{document_id}_{index}_{text[:50]}".encode()).hexdigest()
        chunk = TextChunk(
            id=chunk_id,
            text=text,
            index=index,
            document_id=document_id
        )
        self._process_chunk(chunk)

    def _process_chunk(self, chunk: TextChunk):
        """处理单个文本块"""
        # 保存文本块
        self.graph_store.add_text_chunk(chunk)
        
        # 提取实体和关系
        extraction_result = self.extractor.extract(chunk.text, chunk.id)
        
        # 保存实体
        entity_id_map = {}  # 名称 -> ID 映射
        for extracted_entity in extraction_result.entities:
            entity = Entity(
                id=generate_id(),
                name=extracted_entity.name,
                type=extracted_entity.type,
                description=extracted_entity.description,
                source_ids=[chunk.id]
            )
            entity_id = self.graph_store.add_entity(entity)
            entity_id_map[extracted_entity.name] = entity_id
            self.stats["entities_extracted"] += 1
        
        # 保存关系
        for extracted_relation in extraction_result.relations:
            source_id = entity_id_map.get(extracted_relation.source_name)
            target_id = entity_id_map.get(extracted_relation.target_name)
            
            if source_id and target_id:
                relation = Relation(
                    id=generate_id(),
                    source_id=source_id,
                    target_id=target_id,
                    source_name=extracted_relation.source_name,
                    target_name=extracted_relation.target_name,
                    relation_type=extracted_relation.relation_type,
                    description=extracted_relation.description,
                    source_ids=[chunk.id]
                )
                self.graph_store.add_relation(relation)
                self.stats["relations_extracted"] += 1
    
    def build_communities(
        self,
        resolution: float = 1.0,
        levels: int = 2,
        progress_callback: Optional[Callable[[str, int, int], None]] = None
    ) -> List[Community]:
        """
        构建社区
        
        Args:
            resolution: 社区检测分辨率
            levels: 社区层级数
            progress_callback: 进度回调函数
        """
        if progress_callback:
            progress_callback("detecting", 0, 100)
        
        communities = self.community_manager.build_communities(resolution, levels)
        self.stats["communities_created"] = len(communities)
        
        if progress_callback:
            progress_callback("detecting", 100, 100)
        
        logger.info(f"Built {len(communities)} communities")
        return communities
    
    def index_documents(
        self,
        documents: List[tuple],
        progress_callback: Optional[Callable[[str, int, int], None]] = None
    ):
        """
        批量索引文档
        
        Args:
            documents: 文档列表 [(doc_id, text), ...]
            progress_callback: 进度回调函数
        """
        total = len(documents)
        
        for i, (doc_id, text) in enumerate(documents):
            logger.info(f"Indexing document {i+1}/{total}: {doc_id}")
            
            def doc_progress(stage: str, current: int, total: int):
                if progress_callback:
                    overall_progress = int((i / total + current / total / total) * 100)
                    progress_callback(f"{stage}_doc_{i+1}", overall_progress, 100)
            
            self.index_text(text, doc_id, doc_progress)
        
        # 构建社区
        self.build_communities(progress_callback=progress_callback)
        
        logger.info(f"Indexed {total} documents")
    
    def get_stats(self) -> dict:
        """获取索引统计信息"""
        return {
            **self.stats,
            "graph_stats": self.graph_store.get_graph_stats()
        }
    
    def reset_stats(self):
        """重置统计信息"""
        self.stats = {
            "texts_processed": 0,
            "chunks_created": 0,
            "entities_extracted": 0,
            "relations_extracted": 0,
            "communities_created": 0,
        }
