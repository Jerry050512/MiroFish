"""
GraphRAG 查询引擎
实现 Local Search 和 Global Search 两种查询模式
"""

import json
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field

from ...utils.llm_client import LLMClient
from ...utils.logger import get_logger
from .models import Entity, Relation, Community, TextChunk, SearchResult
from .store import GraphStore

logger = get_logger('mirofish.graphrag.retriever')


@dataclass
class LocalSearchResult:
    """Local Search 结果"""
    query: str
    entities: List[Entity] = field(default_factory=list)
    relations: List[Relation] = field(default_factory=list)
    text_chunks: List[TextChunk] = field(default_factory=list)
    context_text: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "query": self.query,
            "entities": [e.to_dict() for e in self.entities],
            "relations": [r.to_dict() for r in self.relations],
            "text_chunks": [t.to_dict() for t in self.text_chunks],
            "context_text": self.context_text,
        }


@dataclass
class GlobalSearchResult:
    """Global Search 结果"""
    query: str
    communities: List[Community] = field(default_factory=list)
    context_text: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "query": self.query,
            "communities": [c.to_dict() for c in self.communities],
            "context_text": self.context_text,
        }


class LocalSearcher:
    """
    Local Search - 基于实体的局部搜索
    
    适用于回答关于特定实体的问题，如：
    - "张三是谁？"
    - "某公司的主要产品是什么？"
    """
    
    def __init__(
        self,
        graph_store: GraphStore,
        llm_client: Optional[LLMClient] = None
    ):
        self.graph_store = graph_store
        self.llm_client = llm_client or LLMClient()
    
    def search(
        self,
        query: str,
        top_k_entities: int = 10,
        neighbor_depth: int = 2,
        include_text_chunks: bool = True
    ) -> LocalSearchResult:
        """
        执行 Local Search
        
        Args:
            query: 查询文本
            top_k_entities: 返回的实体数量
            neighbor_depth: 邻居遍历深度
            include_text_chunks: 是否包含原始文本块
        
        Returns:
            Local Search 结果
        """
        logger.info(f"Local Search: {query}")
        
        # 1. 从查询中提取实体
        query_entities = self._extract_entities_from_query(query)
        
        # 2. 查找匹配的实体
        matched_entities = self._find_matching_entities(query_entities, top_k_entities)
        
        # 3. 获取相关关系
        related_relations = self._get_related_relations(matched_entities)
        
        # 4. 获取邻居实体
        all_entities_dict = {e.id: e for e in matched_entities}
        for entity in matched_entities:
            neighbors = self.graph_store.get_neighbors(entity.id, neighbor_depth)
            for depth, neighbor_list in neighbors.items():
                for neighbor in neighbor_list:
                    all_entities_dict[neighbor.id] = neighbor

        all_entities_list = list(all_entities_dict.values())
        
        # 5. 获取文本块
        text_chunks = []
        if include_text_chunks:
            text_chunks = self._get_related_text_chunks(all_entities_list, related_relations)
        
        # 6. 构建上下文文本
        context_text = self._build_context_text(
            all_entities_list,
            related_relations,
            text_chunks
        )
        
        result = LocalSearchResult(
            query=query,
            entities=all_entities_list,
            relations=related_relations,
            text_chunks=text_chunks,
            context_text=context_text
        )
        
        logger.info(f"Local Search completed: {len(result.entities)} entities, {len(result.relations)} relations")
        return result
    
    def _extract_entities_from_query(self, query: str) -> List[str]:
        """从查询中提取实体名称"""
        # 简单实现：查找图中与查询词匹配的实体
        query_lower = query.lower()
        found_entities = []
        
        for entity in self.graph_store.get_all_entities():
            if entity.name.lower() in query_lower or entity.type.lower() in query_lower:
                found_entities.append(entity.name)
        
        return found_entities
    
    def _find_matching_entities(
        self,
        entity_names: List[str],
        top_k: int
    ) -> List[Entity]:
        """查找匹配的实体"""
        matched = []
        
        # 按名称精确匹配
        for name in entity_names:
            entity = self.graph_store.get_entity_by_name(name)
            if entity and entity not in matched:
                matched.append(entity)
        
        # 如果不够，补充相似度匹配（如果有向量）
        if len(matched) < top_k:
            # TODO: 实现基于向量的相似度搜索
            pass
        
        return matched[:top_k]
    
    def _get_related_relations(self, entities: List[Entity]) -> List[Relation]:
        """获取与实体相关的关系"""
        entity_ids = {e.id for e in entities}
        relations = []
        
        for relation in self.graph_store.get_all_relations():
            if relation.source_id in entity_ids or relation.target_id in entity_ids:
                relations.append(relation)
        
        return relations
    
    def _get_related_text_chunks(
        self,
        entities: List[Entity],
        relations: List[Relation]
    ) -> List[TextChunk]:
        """获取相关的文本块"""
        chunk_ids = set()
        
        for entity in entities:
            chunk_ids.update(entity.source_ids)
        
        for relation in relations:
            chunk_ids.update(relation.source_ids)
        
        chunks = []
        for chunk_id in chunk_ids:
            chunk = self.graph_store.get_text_chunk(chunk_id)
            if chunk:
                chunks.append(chunk)
        
        return chunks[:10]  # 限制数量
    
    def _build_context_text(
        self,
        entities: List[Entity],
        relations: List[Relation],
        text_chunks: List[TextChunk]
    ) -> str:
        """构建上下文文本"""
        parts = []
        
        # 添加实体信息
        if entities:
            parts.append("## 相关实体")
            for entity in entities[:20]:  # 限制数量
                parts.append(f"- {entity.name} ({entity.type}): {entity.description}")
        
        # 添加关系信息
        if relations:
            parts.append("\n## 相关关系")
            for relation in relations[:20]:
                parts.append(
                    f"- {relation.source_name} --[{relation.relation_type}]--> {relation.target_name}: {relation.description}"
                )
        
        # 添加原始文本
        if text_chunks:
            parts.append("\n## 原始文本")
            for chunk in text_chunks[:5]:
                parts.append(f"[{chunk.document_id}] {chunk.text[:200]}...")
        
        return "\n".join(parts)


class GlobalSearcher:
    """
    Global Search - 基于社区摘要的全局搜索
    
    适用于回答关于整体主题的问题，如：
    - "这个文档集主要讲了什么？"
    - "总结所有关于某主题的观点"
    """
    
    def __init__(
        self,
        graph_store: GraphStore,
        llm_client: Optional[LLMClient] = None
    ):
        self.graph_store = graph_store
        self.llm_client = llm_client or LLMClient()
    
    def search(
        self,
        query: str,
        community_level: int = 0,
        top_k_communities: int = 10
    ) -> GlobalSearchResult:
        """
        执行 Global Search
        
        Args:
            query: 查询文本
            community_level: 社区层级
            top_k_communities: 返回的社区数量
        
        Returns:
            Global Search 结果
        """
        logger.info(f"Global Search: {query}")
        
        # 1. 获取指定层级的社区
        communities = self._get_relevant_communities(query, community_level, top_k_communities)
        
        # 2. 构建上下文文本
        context_text = self._build_context_text(communities)
        
        result = GlobalSearchResult(
            query=query,
            communities=communities,
            context_text=context_text
        )
        
        logger.info(f"Global Search completed: {len(result.communities)} communities")
        return result
    
    def _get_relevant_communities(
        self,
        query: str,
        level: int,
        top_k: int
    ) -> List[Community]:
        """获取相关的社区"""
        # 获取指定层级的社区
        communities = self.graph_store.get_communities_by_level(level)
        
        # 如果没有指定层级，获取所有社区
        if not communities:
            communities = self.graph_store.get_all_communities()
        
        if not communities:
            return []

        if query:
            query_lower = query.lower()
            # 简单评分：摘要或内容中包含关键词
            def score_community(c: Community):
                score = 0
                if query_lower in c.summary.lower():
                    score += 100
                if query_lower in c.full_content.lower():
                    score += 50
                # 加上实体数量作为微调
                score += len(c.entity_ids)
                return score

            communities.sort(key=score_community, reverse=True)
        else:
            # 按实体数量排序
            communities.sort(key=lambda c: len(c.entity_ids), reverse=True)
        
        return communities[:top_k]
    
    def _build_context_text(self, communities: List[Community]) -> str:
        """构建上下文文本"""
        parts = []
        
        parts.append("## 社区摘要")
        
        for i, community in enumerate(communities, 1):
            parts.append(f"\n### 社区 {i}")
            parts.append(f"**主题**: {community.summary}")
            parts.append(f"**详细内容**: {community.full_content}")
            parts.append(f"**包含实体数**: {len(community.entity_ids)}")
        
        return "\n".join(parts)


class GraphRetriever:
    """
    GraphRAG 检索器
    统一封装 Local Search 和 Global Search
    """
    
    def __init__(
        self,
        graph_store: GraphStore,
        llm_client: Optional[LLMClient] = None
    ):
        self.graph_store = graph_store
        self.llm_client = llm_client or LLMClient()
        
        self.local_searcher = LocalSearcher(graph_store, llm_client)
        self.global_searcher = GlobalSearcher(graph_store, llm_client)
    
    def local_search(
        self,
        query: str,
        top_k_entities: int = 10,
        neighbor_depth: int = 2
    ) -> LocalSearchResult:
        """
        执行 Local Search
        
        适用于回答关于特定实体的问题
        """
        return self.local_searcher.search(query, top_k_entities, neighbor_depth)
    
    def global_search(
        self,
        query: str,
        community_level: int = 0,
        top_k_communities: int = 10
    ) -> GlobalSearchResult:
        """
        执行 Global Search
        
        适用于回答关于整体主题的问题
        """
        return self.global_searcher.search(query, community_level, top_k_communities)
    
    def hybrid_search(
        self,
        query: str,
        use_local: bool = True,
        use_global: bool = True
    ) -> SearchResult:
        """
        混合搜索（Local + Global）
        
        Args:
            query: 查询文本
            use_local: 是否使用 Local Search
            use_global: 是否使用 Global Search
        
        Returns:
            综合搜索结果
        """
        entities = []
        relations = []
        communities = []
        context_parts = []
        
        if use_local:
            local_result = self.local_searcher.search(query)
            entities = local_result.entities
            relations = local_result.relations
            if local_result.context_text:
                context_parts.append("## 局部信息\n" + local_result.context_text)
        
        if use_global:
            global_result = self.global_searcher.search(query)
            communities = global_result.communities
            if global_result.context_text:
                context_parts.append("## 全局信息\n" + global_result.context_text)
        
        context_text = "\n\n".join(context_parts)
        
        return SearchResult(
            query=query,
            entities=entities,
            relations=relations,
            communities=communities,
            context_text=context_text
        )
    
    def get_entity_subgraph(
        self,
        entity_name: str,
        depth: int = 2
    ) -> Dict[str, Any]:
        """
        获取实体的子图
        
        Args:
            entity_name: 实体名称
            depth: 遍历深度
        
        Returns:
            子图数据
        """
        entity = self.graph_store.get_entity_by_name(entity_name)
        if not entity:
            return {"error": f"Entity '{entity_name}' not found"}
        
        # 获取邻居
        neighbors = self.graph_store.get_neighbors(entity.id, depth)
        
        # 构建子图
        all_entity_ids = {entity.id}
        for depth_entities in neighbors.values():
            for e in depth_entities:
                all_entity_ids.add(e.id)
        
        # 获取相关关系
        subgraph_relations = []
        for relation in self.graph_store.get_all_relations():
            if relation.source_id in all_entity_ids and relation.target_id in all_entity_ids:
                subgraph_relations.append(relation.to_dict())
        
        return {
            "center_entity": entity.to_dict(),
            "neighbors": {
                depth: [e.to_dict() for e in entities]
                for depth, entities in neighbors.items()
            },
            "relations": subgraph_relations
        }
