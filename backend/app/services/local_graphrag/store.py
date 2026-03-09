"""
GraphRAG 图存储层
基于 NetworkX 实现图数据存储，支持 JSON 序列化
"""

import json
import os
from typing import Dict, Any, List, Optional, Set, Tuple
from pathlib import Path

import networkx as nx
import numpy as np
from numpy.linalg import norm

from .models import Entity, Relation, TextChunk, Community, generate_id
from ...utils.logger import get_logger

logger = get_logger('mirofish.graphrag.store')


class GraphStore:
    """
    图存储管理器
    
    使用 NetworkX 存储图结构，支持：
    - 实体（节点）的增删改查
    - 关系（边）的增删改查
    - 文本块的存储
    - 社区的存储
    - 向量的相似度搜索
    - 数据的持久化（JSON格式）
    """
    
    def __init__(self, storage_dir: str):
        """
        初始化图存储
        
        Args:
            storage_dir: 存储目录路径
        """
        self.storage_dir = Path(storage_dir)
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        
        # 初始化图
        self.graph = nx.DiGraph()  # 有向图
        
        # 数据存储
        self.entities: Dict[str, Entity] = {}
        self.relations: Dict[str, Relation] = {}
        self.text_chunks: Dict[str, TextChunk] = {}
        self.communities: Dict[str, Community] = {}
        
        # 实体名称到ID的映射（用于快速查找）
        self.entity_name_to_id: Dict[str, str] = {}
        
        # 加载已有数据
        self._load_data()
        
        logger.info(f"GraphStore initialized: {storage_dir}")
    
    def _load_data(self):
        """从磁盘加载数据"""
        # 加载实体
        entities_file = self.storage_dir / "entities.json"
        if entities_file.exists():
            with open(entities_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                for entity_data in data:
                    entity = Entity.from_dict(entity_data)
                    self.entities[entity.id] = entity
                    self.entity_name_to_id[entity.name] = entity.id
                    self.graph.add_node(entity.id, **entity.to_dict())
            logger.info(f"Loaded {len(self.entities)} entities")
        
        # 加载关系
        relations_file = self.storage_dir / "relations.json"
        if relations_file.exists():
            with open(relations_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                for relation_data in data:
                    relation = Relation.from_dict(relation_data)
                    self.relations[relation.id] = relation
                    self.graph.add_edge(
                        relation.source_id,
                        relation.target_id,
                        **relation.to_dict()
                    )
            logger.info(f"Loaded {len(self.relations)} relations")
        
        # 加载文本块
        chunks_file = self.storage_dir / "text_chunks.json"
        if chunks_file.exists():
            with open(chunks_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                for chunk_data in data:
                    chunk = TextChunk.from_dict(chunk_data)
                    self.text_chunks[chunk.id] = chunk
            logger.info(f"Loaded {len(self.text_chunks)} text chunks")
        
        # 加载社区
        communities_file = self.storage_dir / "communities.json"
        if communities_file.exists():
            with open(communities_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                for community_data in data:
                    community = Community.from_dict(community_data)
                    self.communities[community.id] = community
            logger.info(f"Loaded {len(self.communities)} communities")
    
    def save_data(self):
        """保存数据到磁盘"""
        # 保存实体
        with open(self.storage_dir / "entities.json", 'w', encoding='utf-8') as f:
            json.dump([e.to_dict() for e in self.entities.values()], f, ensure_ascii=False, indent=2)
        
        # 保存关系
        with open(self.storage_dir / "relations.json", 'w', encoding='utf-8') as f:
            json.dump([r.to_dict() for r in self.relations.values()], f, ensure_ascii=False, indent=2)
        
        # 保存文本块
        with open(self.storage_dir / "text_chunks.json", 'w', encoding='utf-8') as f:
            json.dump([c.to_dict() for c in self.text_chunks.values()], f, ensure_ascii=False, indent=2)
        
        # 保存社区
        with open(self.storage_dir / "communities.json", 'w', encoding='utf-8') as f:
            json.dump([c.to_dict() for c in self.communities.values()], f, ensure_ascii=False, indent=2)
        
        logger.info("Data saved to disk")
    
    # ==================== 实体操作 ====================
    
    def add_entity(self, entity: Entity) -> str:
        """添加实体"""
        # 检查是否已存在同名实体
        if entity.name in self.entity_name_to_id:
            existing_id = self.entity_name_to_id[entity.name]
            logger.debug(f"Entity '{entity.name}' already exists, returning existing ID")
            return existing_id
        
        self.entities[entity.id] = entity
        self.entity_name_to_id[entity.name] = entity.id
        self.graph.add_node(entity.id, **entity.to_dict())
        return entity.id
    
    def get_entity(self, entity_id: str) -> Optional[Entity]:
        """获取实体"""
        return self.entities.get(entity_id)
    
    def get_entity_by_name(self, name: str) -> Optional[Entity]:
        """根据名称获取实体"""
        entity_id = self.entity_name_to_id.get(name)
        if entity_id:
            return self.entities.get(entity_id)
        return None
    
    def get_all_entities(self) -> List[Entity]:
        """获取所有实体"""
        return list(self.entities.values())
    
    def update_entity(self, entity_id: str, **kwargs) -> bool:
        """更新实体"""
        if entity_id not in self.entities:
            return False
        
        entity = self.entities[entity_id]
        for key, value in kwargs.items():
            if hasattr(entity, key):
                setattr(entity, key, value)
        
        # 更新图中的节点
        self.graph.nodes[entity_id].update(entity.to_dict())
        return True
    
    def delete_entity(self, entity_id: str) -> bool:
        """删除实体"""
        if entity_id not in self.entities:
            return False
        
        entity = self.entities[entity_id]
        del self.entities[entity_id]
        del self.entity_name_to_id[entity.name]
        self.graph.remove_node(entity_id)
        return True
    
    # ==================== 关系操作 ====================
    
    def add_relation(self, relation: Relation) -> str:
        """添加关系"""
        self.relations[relation.id] = relation
        self.graph.add_edge(
            relation.source_id,
            relation.target_id,
            **relation.to_dict()
        )
        return relation.id
    
    def get_relation(self, relation_id: str) -> Optional[Relation]:
        """获取关系"""
        return self.relations.get(relation_id)
    
    def get_all_relations(self) -> List[Relation]:
        """获取所有关系"""
        return list(self.relations.values())
    
    def get_relations_by_entity(self, entity_id: str) -> List[Relation]:
        """获取与实体相关的所有关系"""
        relations = []
        for relation in self.relations.values():
            if relation.source_id == entity_id or relation.target_id == entity_id:
                relations.append(relation)
        return relations
    
    def get_neighbors(self, entity_id: str, depth: int = 1) -> Dict[str, List[Entity]]:
        """
        获取实体的邻居
        
        Args:
            entity_id: 实体ID
            depth: 遍历深度
        
        Returns:
            按深度分层的邻居实体
        """
        if entity_id not in self.graph:
            return {}
        
        neighbors_by_depth = {}
        visited = {entity_id}
        current_level = {entity_id}
        
        for d in range(1, depth + 1):
            next_level = set()
            neighbors = []
            
            for node in current_level:
                for neighbor in self.graph.neighbors(node):
                    if neighbor not in visited:
                        visited.add(neighbor)
                        next_level.add(neighbor)
                        if neighbor in self.entities:
                            neighbors.append(self.entities[neighbor])
            
            if neighbors:
                neighbors_by_depth[d] = neighbors
            current_level = next_level
        
        return neighbors_by_depth
    
    # ==================== 文本块操作 ====================
    
    def add_text_chunk(self, chunk: TextChunk) -> str:
        """添加文本块"""
        self.text_chunks[chunk.id] = chunk
        return chunk.id
    
    def get_text_chunk(self, chunk_id: str) -> Optional[TextChunk]:
        """获取文本块"""
        return self.text_chunks.get(chunk_id)
    
    def get_all_text_chunks(self) -> List[TextChunk]:
        """获取所有文本块"""
        return list(self.text_chunks.values())
    
    # ==================== 社区操作 ====================
    
    def add_community(self, community: Community) -> str:
        """添加社区"""
        self.communities[community.id] = community
        return community.id
    
    def get_community(self, community_id: str) -> Optional[Community]:
        """获取社区"""
        return self.communities.get(community_id)
    
    def get_all_communities(self) -> List[Community]:
        """获取所有社区"""
        return list(self.communities.values())
    
    def get_communities_by_level(self, level: int) -> List[Community]:
        """获取指定层级的社区"""
        return [c for c in self.communities.values() if c.level == level]
    
    # ==================== 向量搜索 ====================
    
    def vector_search(
        self,
        query_embedding: List[float],
        top_k: int = 10,
        entity_types: Optional[List[str]] = None
    ) -> List[Tuple[Entity, float]]:
        """
        基于向量的实体搜索
        
        Args:
            query_embedding: 查询向量
            top_k: 返回结果数量
            entity_types: 实体类型过滤
        
        Returns:
            (实体, 相似度分数) 列表
        """
        results = []
        query_vec = np.array(query_embedding)
        
        for entity in self.entities.values():
            # 类型过滤
            if entity_types and entity.type not in entity_types:
                continue
            
            # 向量相似度计算
            if entity.embedding:
                entity_vec = np.array(entity.embedding)
                similarity = np.dot(query_vec, entity_vec) / (norm(query_vec) * norm(entity_vec))
                results.append((entity, float(similarity)))
        
        # 按相似度排序
        results.sort(key=lambda x: x[1], reverse=True)
        return results[:top_k]
    
    def text_chunk_vector_search(
        self,
        query_embedding: List[float],
        top_k: int = 10
    ) -> List[Tuple[TextChunk, float]]:
        """基于向量的文本块搜索"""
        results = []
        query_vec = np.array(query_embedding)
        
        for chunk in self.text_chunks.values():
            if chunk.embedding:
                chunk_vec = np.array(chunk.embedding)
                similarity = np.dot(query_vec, chunk_vec) / (norm(query_vec) * norm(chunk_vec))
                results.append((chunk, float(similarity)))
        
        results.sort(key=lambda x: x[1], reverse=True)
        return results[:top_k]
    
    # ==================== 图算法 ====================
    
    def get_graph_stats(self) -> Dict[str, Any]:
        """获取图统计信息"""
        return {
            "num_nodes": self.graph.number_of_nodes(),
            "num_edges": self.graph.number_of_edges(),
            "num_entities": len(self.entities),
            "num_relations": len(self.relations),
            "num_text_chunks": len(self.text_chunks),
            "num_communities": len(self.communities),
            "is_directed": self.graph.is_directed(),
            "density": nx.density(self.graph),
        }
    
    def clear(self):
        """清空所有数据"""
        self.graph.clear()
        self.entities.clear()
        self.relations.clear()
        self.text_chunks.clear()
        self.communities.clear()
        self.entity_name_to_id.clear()
        logger.info("GraphStore cleared")
