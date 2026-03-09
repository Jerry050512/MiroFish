"""
GraphRAG 数据模型
定义实体、关系、社区等核心数据结构
"""

from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional, Set
from datetime import datetime
import uuid


@dataclass
class Entity:
    """实体节点"""
    id: str
    name: str
    type: str
    description: str = ""
    source_ids: List[str] = field(default_factory=list)  # 来源文本块ID
    embedding: Optional[List[float]] = None
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "type": self.type,
            "description": self.description,
            "source_ids": self.source_ids,
            "created_at": self.created_at,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Entity":
        return cls(
            id=data["id"],
            name=data["name"],
            type=data["type"],
            description=data.get("description", ""),
            source_ids=data.get("source_ids", []),
            embedding=data.get("embedding"),
            created_at=data.get("created_at", datetime.now().isoformat()),
        )


@dataclass
class Relation:
    """关系边"""
    id: str
    source_id: str  # 源实体ID
    target_id: str  # 目标实体ID
    source_name: str = ""  # 源实体名称
    target_name: str = ""  # 目标实体名称
    relation_type: str = ""  # 关系类型
    description: str = ""  # 关系描述
    weight: float = 1.0  # 关系权重
    source_ids: List[str] = field(default_factory=list)  # 来源文本块ID
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "source_id": self.source_id,
            "target_id": self.target_id,
            "source_name": self.source_name,
            "target_name": self.target_name,
            "relation_type": self.relation_type,
            "description": self.description,
            "weight": self.weight,
            "source_ids": self.source_ids,
            "created_at": self.created_at,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Relation":
        return cls(
            id=data["id"],
            source_id=data["source_id"],
            target_id=data["target_id"],
            source_name=data.get("source_name", ""),
            target_name=data.get("target_name", ""),
            relation_type=data.get("relation_type", ""),
            description=data.get("description", ""),
            weight=data.get("weight", 1.0),
            source_ids=data.get("source_ids", []),
            created_at=data.get("created_at", datetime.now().isoformat()),
        )


@dataclass
class TextChunk:
    """文本块"""
    id: str
    text: str
    index: int  # 在文档中的顺序
    document_id: str = ""  # 所属文档ID
    embedding: Optional[List[float]] = None
    entities: List[str] = field(default_factory=list)  # 包含的实体ID
    relations: List[str] = field(default_factory=list)  # 包含的关系ID
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "text": self.text,
            "index": self.index,
            "document_id": self.document_id,
            "entities": self.entities,
            "relations": self.relations,
            "created_at": self.created_at,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TextChunk":
        return cls(
            id=data["id"],
            text=data["text"],
            index=data["index"],
            document_id=data.get("document_id", ""),
            embedding=data.get("embedding"),
            entities=data.get("entities", []),
            relations=data.get("relations", []),
            created_at=data.get("created_at", datetime.now().isoformat()),
        )


@dataclass
class Community:
    """社区（由社区检测算法生成）"""
    id: str
    level: int  # 社区层级
    entity_ids: List[str] = field(default_factory=list)  # 社区包含的实体
    relation_ids: List[str] = field(default_factory=list)  # 社区包含的关系
    summary: str = ""  # 社区摘要
    full_content: str = ""  # 完整内容
    embedding: Optional[List[float]] = None
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "level": self.level,
            "entity_ids": self.entity_ids,
            "relation_ids": self.relation_ids,
            "summary": self.summary,
            "full_content": self.full_content,
            "created_at": self.created_at,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Community":
        return cls(
            id=data["id"],
            level=data["level"],
            entity_ids=data.get("entity_ids", []),
            relation_ids=data.get("relation_ids", []),
            summary=data.get("summary", ""),
            full_content=data.get("full_content", ""),
            embedding=data.get("embedding"),
            created_at=data.get("created_at", datetime.now().isoformat()),
        )


@dataclass
class SearchResult:
    """搜索结果"""
    query: str
    entities: List[Entity] = field(default_factory=list)
    relations: List[Relation] = field(default_factory=list)
    communities: List[Community] = field(default_factory=list)
    text_chunks: List[TextChunk] = field(default_factory=list)
    
    # 上下文文本（用于LLM）
    context_text: str = ""
    
    # 统计信息
    total_entities: int = 0
    total_relations: int = 0
    total_communities: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "query": self.query,
            "entities": [e.to_dict() for e in self.entities],
            "relations": [r.to_dict() for r in self.relations],
            "communities": [c.to_dict() for c in self.communities],
            "text_chunks": [t.to_dict() for t in self.text_chunks],
            "context_text": self.context_text,
            "total_entities": len(self.entities),
            "total_relations": len(self.relations),
            "total_communities": len(self.communities),
        }


@dataclass
class ExtractedEntity:
    """提取的实体（从文本中提取的原始结果）"""
    name: str
    type: str
    description: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "type": self.type,
            "description": self.description,
        }


@dataclass
class ExtractedRelation:
    """提取的关系（从文本中提取的原始结果）"""
    source_name: str
    target_name: str
    relation_type: str
    description: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "source_name": self.source_name,
            "target_name": self.target_name,
            "relation_type": self.relation_type,
            "description": self.description,
        }


def generate_id() -> str:
    """生成唯一ID"""
    return str(uuid.uuid4())
