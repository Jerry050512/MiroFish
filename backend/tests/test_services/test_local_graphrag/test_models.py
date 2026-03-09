"""
GraphRAG 数据模型单元测试
"""

import pytest
from datetime import datetime

from app.services.local_graphrag.models import (
    Entity, Relation, TextChunk, Community,
    ExtractedEntity, ExtractedRelation, generate_id
)


class TestEntity:
    """测试 Entity 模型"""
    
    def test_entity_creation(self):
        """测试实体创建"""
        entity = Entity(
            id="test-id",
            name="张三",
            type="人物",
            description="一名软件工程师"
        )
        
        assert entity.id == "test-id"
        assert entity.name == "张三"
        assert entity.type == "人物"
        assert entity.description == "一名软件工程师"
        assert entity.source_ids == []
    
    def test_entity_to_dict(self):
        """测试实体序列化"""
        entity = Entity(
            id="test-id",
            name="张三",
            type="人物",
            description="一名软件工程师",
            source_ids=["chunk-1"]
        )
        
        data = entity.to_dict()
        
        assert data["id"] == "test-id"
        assert data["name"] == "张三"
        assert data["type"] == "人物"
        assert data["description"] == "一名软件工程师"
        assert data["source_ids"] == ["chunk-1"]
    
    def test_entity_from_dict(self):
        """测试实体反序列化"""
        data = {
            "id": "test-id",
            "name": "张三",
            "type": "人物",
            "description": "一名软件工程师",
            "source_ids": ["chunk-1"],
            "created_at": "2024-01-01T00:00:00"
        }
        
        entity = Entity.from_dict(data)
        
        assert entity.id == "test-id"
        assert entity.name == "张三"
        assert entity.type == "人物"
        assert entity.source_ids == ["chunk-1"]


class TestRelation:
    """测试 Relation 模型"""
    
    def test_relation_creation(self):
        """测试关系创建"""
        relation = Relation(
            id="rel-id",
            source_id="entity-1",
            target_id="entity-2",
            source_name="张三",
            target_name="李四",
            relation_type="同事",
            description="在同一公司工作"
        )
        
        assert relation.id == "rel-id"
        assert relation.source_id == "entity-1"
        assert relation.target_id == "entity-2"
        assert relation.relation_type == "同事"
        assert relation.weight == 1.0
    
    def test_relation_to_dict(self):
        """测试关系序列化"""
        relation = Relation(
            id="rel-id",
            source_id="entity-1",
            target_id="entity-2",
            source_name="张三",
            target_name="李四",
            relation_type="同事",
            weight=2.0
        )
        
        data = relation.to_dict()
        
        assert data["id"] == "rel-id"
        assert data["source_name"] == "张三"
        assert data["target_name"] == "李四"
        assert data["weight"] == 2.0


class TestTextChunk:
    """测试 TextChunk 模型"""
    
    def test_chunk_creation(self):
        """测试文本块创建"""
        chunk = TextChunk(
            id="chunk-1",
            text="这是一段测试文本",
            index=0,
            document_id="doc-1"
        )
        
        assert chunk.id == "chunk-1"
        assert chunk.text == "这是一段测试文本"
        assert chunk.index == 0
        assert chunk.document_id == "doc-1"
    
    def test_chunk_with_entities(self):
        """测试包含实体的文本块"""
        chunk = TextChunk(
            id="chunk-1",
            text="张三和李四是同事",
            index=0,
            entities=["entity-1", "entity-2"]
        )
        
        assert len(chunk.entities) == 2
        assert "entity-1" in chunk.entities


class TestCommunity:
    """测试 Community 模型"""
    
    def test_community_creation(self):
        """测试社区创建"""
        community = Community(
            id="comm-1",
            level=0,
            entity_ids=["e1", "e2", "e3"],
            summary="这是一个测试社区"
        )
        
        assert community.id == "comm-1"
        assert community.level == 0
        assert len(community.entity_ids) == 3
        assert community.summary == "这是一个测试社区"


class TestExtractedModels:
    """测试提取模型"""
    
    def test_extracted_entity(self):
        """测试提取的实体"""
        entity = ExtractedEntity(
            name="阿里巴巴",
            type="公司",
            description="一家科技公司"
        )
        
        assert entity.name == "阿里巴巴"
        assert entity.type == "公司"
    
    def test_extracted_relation(self):
        """测试提取的关系"""
        relation = ExtractedRelation(
            source_name="张三",
            target_name="阿里巴巴",
            relation_type="工作于",
            description="张三在阿里巴巴工作"
        )
        
        assert relation.source_name == "张三"
        assert relation.target_name == "阿里巴巴"
        assert relation.relation_type == "工作于"


class TestGenerateId:
    """测试 ID 生成"""
    
    def test_generate_id(self):
        """测试 ID 生成"""
        id1 = generate_id()
        id2 = generate_id()
        
        assert isinstance(id1, str)
        assert len(id1) > 0
        assert id1 != id2  # 确保唯一性
