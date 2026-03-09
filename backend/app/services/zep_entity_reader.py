"""
实体读取与过滤服务
支持 Zep Cloud 和本地 GraphRAG 两种模式
从图谱中读取节点，筛选出符合预定义实体类型的节点
"""

import time
from typing import Dict, Any, List, Optional, Set, Callable, TypeVar
from dataclasses import dataclass, field

from ..config import Config
from ..utils.logger import get_logger

logger = get_logger('mirofish.entity_reader')

# 用于泛型返回类型
T = TypeVar('T')


@dataclass
class EntityNode:
    """实体节点数据结构"""
    uuid: str
    name: str
    labels: List[str]
    summary: str
    attributes: Dict[str, Any]
    # 相关的边信息
    related_edges: List[Dict[str, Any]] = field(default_factory=list)
    # 相关的其他节点信息
    related_nodes: List[Dict[str, Any]] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "uuid": self.uuid,
            "name": self.name,
            "labels": self.labels,
            "summary": self.summary,
            "attributes": self.attributes,
            "related_edges": self.related_edges,
            "related_nodes": self.related_nodes,
        }
    
    def get_entity_type(self) -> Optional[str]:
        """获取实体类型（排除默认的Entity标签）"""
        for label in self.labels:
            if label not in ["Entity", "Node"]:
                return label
        return None


@dataclass
class FilteredEntities:
    """过滤后的实体集合"""
    entities: List[EntityNode]
    entity_types: Set[str]
    total_count: int
    filtered_count: int
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "entities": [e.to_dict() for e in self.entities],
            "entity_types": list(self.entity_types),
            "total_count": self.total_count,
            "filtered_count": self.filtered_count,
        }


class BaseEntityReader:
    """实体读取器基类"""
    
    def get_all_nodes(self, graph_id: str) -> List[Dict[str, Any]]:
        """获取图谱的所有节点"""
        raise NotImplementedError
    
    def get_all_edges(self, graph_id: str) -> List[Dict[str, Any]]:
        """获取图谱的所有边"""
        raise NotImplementedError
    
    def get_node_edges(self, node_uuid: str) -> List[Dict[str, Any]]:
        """获取指定节点的所有相关边"""
        raise NotImplementedError
    
    def filter_defined_entities(
        self, 
        graph_id: str,
        defined_entity_types: Optional[List[str]] = None,
        enrich_with_edges: bool = True
    ) -> FilteredEntities:
        """筛选出符合预定义实体类型的节点"""
        raise NotImplementedError


class ZepEntityReader(BaseEntityReader):
    """
    Zep Cloud 实体读取与过滤服务
    
    主要功能：
    1. 从Zep图谱读取所有节点
    2. 筛选出符合预定义实体类型的节点
    3. 获取每个实体的相关边和关联节点信息
    """
    
    def __init__(self, api_key: Optional[str] = None):
        from zep_cloud.client import Zep
        from ..utils.zep_paging import fetch_all_nodes, fetch_all_edges
        
        self.api_key = api_key or Config.ZEP_API_KEY
        if not self.api_key:
            raise ValueError("ZEP_API_KEY 未配置")
        
        self.client = Zep(api_key=self.api_key)
        self.fetch_all_nodes = fetch_all_nodes
        self.fetch_all_edges = fetch_all_edges
    
    def _call_with_retry(
        self, 
        func: Callable[[], T], 
        operation_name: str,
        max_retries: int = 3,
        initial_delay: float = 2.0
    ) -> T:
        """带重试机制的Zep API调用"""
        last_exception = None
        delay = initial_delay
        
        for attempt in range(max_retries):
            try:
                return func()
            except Exception as e:
                last_exception = e
                if attempt < max_retries - 1:
                    logger.warning(
                        f"Zep {operation_name} 第 {attempt + 1} 次尝试失败: {str(e)[:100]}, "
                        f"{delay:.1f}秒后重试..."
                    )
                    time.sleep(delay)
                    delay *= 2
                else:
                    logger.error(f"Zep {operation_name} 在 {max_retries} 次尝试后仍失败: {str(e)}")
        
        raise last_exception
    
    def get_all_nodes(self, graph_id: str) -> List[Dict[str, Any]]:
        """获取图谱的所有节点"""
        logger.info(f"获取图谱 {graph_id} 的所有节点...")
        
        nodes = self.fetch_all_nodes(self.client, graph_id)
        
        nodes_data = []
        for node in nodes:
            nodes_data.append({
                "uuid": getattr(node, 'uuid_', None) or getattr(node, 'uuid', ''),
                "name": node.name or "",
                "labels": node.labels or [],
                "summary": node.summary or "",
                "attributes": node.attributes or {},
            })
        
        logger.info(f"共获取 {len(nodes_data)} 个节点")
        return nodes_data
    
    def get_all_edges(self, graph_id: str) -> List[Dict[str, Any]]:
        """获取图谱的所有边"""
        logger.info(f"获取图谱 {graph_id} 的所有边...")
        
        edges = self.fetch_all_edges(self.client, graph_id)
        
        edges_data = []
        for edge in edges:
            edges_data.append({
                "uuid": getattr(edge, 'uuid_', None) or getattr(edge, 'uuid', ''),
                "name": edge.name or "",
                "fact": edge.fact or "",
                "source_node_uuid": edge.source_node_uuid,
                "target_node_uuid": edge.target_node_uuid,
                "attributes": edge.attributes or {},
            })
        
        logger.info(f"共获取 {len(edges_data)} 条边")
        return edges_data
    
    def get_node_edges(self, node_uuid: str) -> List[Dict[str, Any]]:
        """获取指定节点的所有相关边"""
        try:
            edges = self._call_with_retry(
                func=lambda: self.client.graph.node.get_entity_edges(node_uuid=node_uuid),
                operation_name=f"获取节点边(node={node_uuid[:8]}...)"
            )
            
            edges_data = []
            for edge in edges:
                edges_data.append({
                    "uuid": getattr(edge, 'uuid_', None) or getattr(edge, 'uuid', ''),
                    "name": edge.name or "",
                    "fact": edge.fact or "",
                    "source_node_uuid": edge.source_node_uuid,
                    "target_node_uuid": edge.target_node_uuid,
                    "attributes": edge.attributes or {},
                })
            
            return edges_data
        except Exception as e:
            logger.warning(f"获取节点 {node_uuid} 的边失败: {str(e)}")
            return []
    
    def filter_defined_entities(
        self, 
        graph_id: str,
        defined_entity_types: Optional[List[str]] = None,
        enrich_with_edges: bool = True
    ) -> FilteredEntities:
        """筛选出符合预定义实体类型的节点"""
        logger.info(f"开始筛选图谱 {graph_id} 的实体...")
        
        all_nodes = self.get_all_nodes(graph_id)
        total_count = len(all_nodes)
        all_edges = self.get_all_edges(graph_id) if enrich_with_edges else []
        
        return self._filter_entities(all_nodes, all_edges, defined_entity_types, enrich_with_edges)
    
    def _filter_entities(
        self,
        all_nodes: List[Dict[str, Any]],
        all_edges: List[Dict[str, Any]],
        defined_entity_types: Optional[List[str]],
        enrich_with_edges: bool
    ) -> FilteredEntities:
        """通用实体过滤逻辑"""
        node_map = {n["uuid"]: n for n in all_nodes}
        
        filtered_entities = []
        entity_types_found = set()
        
        for node in all_nodes:
            labels = node.get("labels", [])
            custom_labels = [l for l in labels if l not in ["Entity", "Node"]]
            
            if not custom_labels:
                continue
            
            if defined_entity_types:
                matching_labels = [l for l in custom_labels if l in defined_entity_types]
                if not matching_labels:
                    continue
                entity_type = matching_labels[0]
            else:
                entity_type = custom_labels[0]
            
            entity_types_found.add(entity_type)
            
            entity = EntityNode(
                uuid=node["uuid"],
                name=node["name"],
                labels=labels,
                summary=node.get("summary", ""),
                attributes=node.get("attributes", {}),
            )
            
            if enrich_with_edges:
                related_edges = []
                related_node_uuids = set()
                
                for edge in all_edges:
                    if edge["source_node_uuid"] == node["uuid"]:
                        related_edges.append({
                            "direction": "outgoing",
                            "edge_name": edge["name"],
                            "fact": edge["fact"],
                            "target_node_uuid": edge["target_node_uuid"],
                        })
                        related_node_uuids.add(edge["target_node_uuid"])
                    elif edge["target_node_uuid"] == node["uuid"]:
                        related_edges.append({
                            "direction": "incoming",
                            "edge_name": edge["name"],
                            "fact": edge["fact"],
                            "source_node_uuid": edge["source_node_uuid"],
                        })
                        related_node_uuids.add(edge["source_node_uuid"])
                
                entity.related_edges = related_edges
                
                related_nodes = []
                for related_uuid in related_node_uuids:
                    if related_uuid in node_map:
                        related_node = node_map[related_uuid]
                        related_nodes.append({
                            "uuid": related_node["uuid"],
                            "name": related_node["name"],
                            "labels": related_node["labels"],
                            "summary": related_node.get("summary", ""),
                        })
                
                entity.related_nodes = related_nodes
            
            filtered_entities.append(entity)
        
        logger.info(f"筛选完成: 总节点 {len(all_nodes)}, 符合条件 {len(filtered_entities)}, "
                   f"实体类型: {entity_types_found}")
        
        return FilteredEntities(
            entities=filtered_entities,
            entity_types=entity_types_found,
            total_count=len(all_nodes),
            filtered_count=len(filtered_entities),
        )


class LocalEntityReader(BaseEntityReader):
    """本地 GraphRAG 实体读取服务"""
    
    def __init__(self):
        from .local_graphrag import LocalMemoryService
        
        self.memory_service = LocalMemoryService(
            storage_dir=Config.GRAPHRAG_STORAGE_DIR
        )
    
    def get_all_nodes(self, graph_id: str) -> List[Dict[str, Any]]:
        """获取本地图谱的所有节点"""
        logger.info("获取本地图谱的所有节点...")
        
        entities = self.memory_service.get_all_entities()
        
        nodes_data = []
        for entity in entities:
            nodes_data.append({
                "uuid": entity['id'],
                "name": entity.get('name', ''),
                "labels": [entity.get('type', 'Entity')],
                "summary": entity.get('description', ''),
                "attributes": entity.get('attributes', {}),
            })
        
        logger.info(f"共获取 {len(nodes_data)} 个节点")
        return nodes_data
    
    def get_all_edges(self, graph_id: str) -> List[Dict[str, Any]]:
        """获取本地图谱的所有边"""
        logger.info("获取本地图谱的所有边...")
        
        relations = self.memory_service.get_all_relations()
        
        edges_data = []
        for relation in relations:
            edges_data.append({
                "uuid": relation['id'],
                "name": relation.get('relation_type', ''),
                "fact": relation.get('description', ''),
                "source_node_uuid": relation['source_id'],
                "target_node_uuid": relation['target_id'],
                "attributes": relation.get('attributes', {}),
            })
        
        logger.info(f"共获取 {len(edges_data)} 条边")
        return edges_data
    
    def get_node_edges(self, node_uuid: str) -> List[Dict[str, Any]]:
        """获取指定节点的所有相关边"""
        try:
            # 获取实体的子图
            subgraph = self.memory_service.get_entity_subgraph(node_uuid, depth=1)
            
            edges_data = []
            for edge in subgraph.get('edges', []):
                edges_data.append({
                    "uuid": edge.get('id', ''),
                    "name": edge.get('relation_type', ''),
                    "fact": edge.get('description', ''),
                    "source_node_uuid": edge.get('source_id', ''),
                    "target_node_uuid": edge.get('target_id', ''),
                    "attributes": edge.get('attributes', {}),
                })
            
            return edges_data
        except Exception as e:
            logger.warning(f"获取节点 {node_uuid} 的边失败: {str(e)}")
            return []
    
    def filter_defined_entities(
        self, 
        graph_id: str,
        defined_entity_types: Optional[List[str]] = None,
        enrich_with_edges: bool = True
    ) -> FilteredEntities:
        """筛选出符合预定义实体类型的节点"""
        logger.info("开始筛选本地图谱的实体...")
        
        all_nodes = self.get_all_nodes(graph_id)
        total_count = len(all_nodes)
        all_edges = self.get_all_edges(graph_id) if enrich_with_edges else []
        
        return self._filter_entities(all_nodes, all_edges, defined_entity_types, enrich_with_edges)
    
    def _filter_entities(
        self,
        all_nodes: List[Dict[str, Any]],
        all_edges: List[Dict[str, Any]],
        defined_entity_types: Optional[List[str]],
        enrich_with_edges: bool
    ) -> FilteredEntities:
        """通用实体过滤逻辑"""
        node_map = {n["uuid"]: n for n in all_nodes}
        
        filtered_entities = []
        entity_types_found = set()
        
        for node in all_nodes:
            labels = node.get("labels", [])
            custom_labels = [l for l in labels if l not in ["Entity", "Node"]]
            
            if not custom_labels:
                continue
            
            if defined_entity_types:
                matching_labels = [l for l in custom_labels if l in defined_entity_types]
                if not matching_labels:
                    continue
                entity_type = matching_labels[0]
            else:
                entity_type = custom_labels[0]
            
            entity_types_found.add(entity_type)
            
            entity = EntityNode(
                uuid=node["uuid"],
                name=node["name"],
                labels=labels,
                summary=node.get("summary", ""),
                attributes=node.get("attributes", {}),
            )
            
            if enrich_with_edges:
                related_edges = []
                related_node_uuids = set()
                
                for edge in all_edges:
                    if edge["source_node_uuid"] == node["uuid"]:
                        related_edges.append({
                            "direction": "outgoing",
                            "edge_name": edge["name"],
                            "fact": edge["fact"],
                            "target_node_uuid": edge["target_node_uuid"],
                        })
                        related_node_uuids.add(edge["target_node_uuid"])
                    elif edge["target_node_uuid"] == node["uuid"]:
                        related_edges.append({
                            "direction": "incoming",
                            "edge_name": edge["name"],
                            "fact": edge["fact"],
                            "source_node_uuid": edge["source_node_uuid"],
                        })
                        related_node_uuids.add(edge["source_node_uuid"])
                
                entity.related_edges = related_edges
                
                related_nodes = []
                for related_uuid in related_node_uuids:
                    if related_uuid in node_map:
                        related_node = node_map[related_uuid]
                        related_nodes.append({
                            "uuid": related_node["uuid"],
                            "name": related_node["name"],
                            "labels": related_node["labels"],
                            "summary": related_node.get("summary", ""),
                        })
                
                entity.related_nodes = related_nodes
            
            filtered_entities.append(entity)
        
        logger.info(f"筛选完成: 总节点 {len(all_nodes)}, 符合条件 {len(filtered_entities)}, "
                   f"实体类型: {entity_types_found}")
        
        return FilteredEntities(
            entities=filtered_entities,
            entity_types=entity_types_found,
            total_count=len(all_nodes),
            filtered_count=len(filtered_entities),
        )


def get_entity_reader() -> BaseEntityReader:
    """
    获取实体读取器
    根据配置自动选择 Zep Cloud 或本地 GraphRAG
    """
    if Config.USE_LOCAL_GRAPHRAG:
        logger.info("使用本地 GraphRAG 实体读取器")
        return LocalEntityReader()
    else:
        logger.info("使用 Zep Cloud 实体读取器")
        return ZepEntityReader()
