"""
GraphRAG 社区检测和摘要生成
使用 Leiden 算法进行社区检测，使用 LLM 生成社区摘要
"""

import json
from typing import List, Dict, Any, Optional, Set, Tuple
import networkx as nx

from ...utils.llm_client import LLMClient
from ...utils.logger import get_logger
from .models import Entity, Relation, Community, generate_id
from .store import GraphStore

logger = get_logger('mirofish.graphrag.community')


COMMUNITY_SUMMARY_PROMPT = """你是一个专业的信息分析助手。请根据以下社区内的实体和关系，生成一个简洁的社区摘要。

社区内的实体：
{entities}

社区内的关系：
{relations}

请生成以下内容：
1. 一句话总结这个社区的核心主题
2. 列出社区内的关键实体及其作用
3. 描述实体之间的主要关系

请按以下JSON格式输出：
{{
    "summary": "社区核心主题总结（一句话）",
    "full_content": "详细的社区描述，包括关键实体和关系"
}}

注意：
1. 只输出JSON格式，不要输出其他内容
2. 总结应该简洁但信息丰富
3. full_content 可以包含多段文字"""


class CommunityDetector:
    """社区检测器（使用 Leiden 算法）"""
    
    def __init__(self, graph_store: GraphStore):
        self.graph_store = graph_store
    
    def detect_communities(
        self,
        resolution: float = 1.0,
        levels: int = 2
    ) -> List[Community]:
        """
        检测社区
        
        Args:
            resolution: Leiden 算法分辨率参数
            levels: 社区层级数
        
        Returns:
            检测到的社区列表
        """
        try:
            import igraph as ig
            import leidenalg as la
        except ImportError:
            logger.warning("igraph or leidenalg not installed, using fallback community detection")
            return self._detect_communities_fallback(levels)
        
        # 转换为 igraph
        G = self.graph_store.graph
        if G.number_of_nodes() == 0:
            return []
        
        # 创建 igraph
        ig_graph = ig.Graph(directed=True)
        node_list = list(G.nodes())
        node_index = {node: i for i, node in enumerate(node_list)}
        
        ig_graph.add_vertices(len(node_list))
        
        edges = []
        weights = []
        for u, v, data in G.edges(data=True):
            edges.append((node_index[u], node_index[v]))
            weights.append(data.get('weight', 1.0))
        
        ig_graph.add_edges(edges)
        ig_graph.es['weight'] = weights
        
        communities = []
        
        # 多层级社区检测
        for level in range(levels):
            # 运行 Leiden 算法
            partition = la.find_partition(
                ig_graph,
                la.RBConfigurationVertexPartition,
                weights=ig_graph.es['weight'],
                resolution_parameter=resolution * (level + 1)
            )
            
            # 构建社区
            for comm_id, membership in enumerate(partition):
                entity_ids = [node_list[i] for i in membership]
                
                # 获取社区内的关系
                relation_ids = []
                for relation in self.graph_store.get_all_relations():
                    if relation.source_id in entity_ids and relation.target_id in entity_ids:
                        relation_ids.append(relation.id)
                
                community = Community(
                    id=generate_id(),
                    level=level,
                    entity_ids=entity_ids,
                    relation_ids=relation_ids,
                )
                communities.append(community)
        
        logger.info(f"Detected {len(communities)} communities across {levels} levels")
        return communities
    
    def _detect_communities_fallback(self, levels: int = 1) -> List[Community]:
        """备用社区检测（使用 NetworkX 的连通分量）"""
        G = self.graph_store.graph
        if G.number_of_nodes() == 0:
            return []
        
        communities = []
        
        # 将有向图转为无向图进行连通分量分析
        undirected = G.to_undirected()
        
        # 获取连通分量
        connected_components = list(nx.connected_components(undirected))
        
        for level in range(levels):
            for comm_id, component in enumerate(connected_components):
                entity_ids = list(component)
                
                # 获取社区内的关系
                relation_ids = []
                for relation in self.graph_store.get_all_relations():
                    if relation.source_id in entity_ids and relation.target_id in entity_ids:
                        relation_ids.append(relation.id)
                
                community = Community(
                    id=generate_id(),
                    level=level,
                    entity_ids=entity_ids,
                    relation_ids=relation_ids,
                )
                communities.append(community)
        
        logger.info(f"Detected {len(communities)} communities using fallback method")
        return communities


class CommunitySummarizer:
    """社区摘要生成器"""
    
    def __init__(self, graph_store: GraphStore, llm_client: Optional[LLMClient] = None):
        self.graph_store = graph_store
        self.llm_client = llm_client or LLMClient()
    
    def summarize(self, community: Community) -> Community:
        """
        为社区生成摘要
        
        Args:
            community: 社区对象
        
        Returns:
            更新后的社区对象
        """
        # 获取社区内的实体
        entities = []
        for entity_id in community.entity_ids:
            entity = self.graph_store.get_entity(entity_id)
            if entity:
                entities.append(f"- {entity.name} ({entity.type}): {entity.description}")
        
        # 获取社区内的关系
        relations = []
        for relation_id in community.relation_ids:
            relation = self.graph_store.get_relation(relation_id)
            if relation:
                relations.append(
                    f"- {relation.source_name} --[{relation.relation_type}]--> {relation.target_name}: {relation.description}"
                )
        
        if not entities:
            community.summary = "空社区"
            community.full_content = "该社区不包含任何实体"
            return community
        
        # 构建提示
        prompt = COMMUNITY_SUMMARY_PROMPT.format(
            entities="\n".join(entities[:20]),  # 限制数量避免过长
            relations="\n".join(relations[:20])
        )
        
        try:
            content = self.llm_client.chat(
                messages=[{"role": "user", "content": prompt}],
                temperature=0.3,
            )
            
            # 解析JSON
            content = content.strip()
            if content.startswith("```json"):
                content = content[7:]
            if content.endswith("```"):
                content = content[:-3]
            content = content.strip()
            
            data = json.loads(content)
            
            community.summary = data.get("summary", "")
            community.full_content = data.get("full_content", "")
            
            logger.info(f"Generated summary for community {community.id}")
            
        except Exception as e:
            logger.error(f"Community summarization failed: {e}")
            # 使用备用摘要
            entity_names = [e.split("(")[0].strip("- ") for e in entities[:5]]
            community.summary = f"包含实体: {', '.join(entity_names)}"
            community.full_content = f"该社区包含 {len(community.entity_ids)} 个实体和 {len(community.relation_ids)} 个关系"
        
        return community
    
    def summarize_all(self, communities: List[Community]) -> List[Community]:
        """为所有社区生成摘要"""
        summarized = []
        for community in communities:
            summarized.append(self.summarize(community))
        return summarized


class CommunityManager:
    """社区管理器"""
    
    def __init__(
        self,
        graph_store: GraphStore,
        llm_client: Optional[LLMClient] = None
    ):
        self.graph_store = graph_store
        self.detector = CommunityDetector(graph_store)
        self.summarizer = CommunitySummarizer(graph_store, llm_client)
    
    def build_communities(
        self,
        resolution: float = 1.0,
        levels: int = 2
    ) -> List[Community]:
        """
        构建社区（检测 + 摘要）
        
        Args:
            resolution: 社区检测分辨率
            levels: 社区层级数
        
        Returns:
            社区列表
        """
        # 检测社区
        communities = self.detector.detect_communities(resolution, levels)
        
        # 生成摘要
        communities = self.summarizer.summarize_all(communities)
        
        # 保存到存储
        for community in communities:
            self.graph_store.add_community(community)
        
        self.graph_store.save_data()
        
        logger.info(f"Built {len(communities)} communities")
        return communities
    
    def get_community_hierarchy(self) -> Dict[int, List[Community]]:
        """获取社区层级结构"""
        hierarchy = {}
        for community in self.graph_store.get_all_communities():
            if community.level not in hierarchy:
                hierarchy[community.level] = []
            hierarchy[community.level].append(community)
        return hierarchy
