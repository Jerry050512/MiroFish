"""
GraphRAG 存储层单元测试
"""

import pytest
import tempfile
import shutil
from pathlib import Path

from app.services.local_graphrag.store import GraphStore
from app.services.local_graphrag.models import Entity, Relation, TextChunk, generate_id


class TestGraphStore:
    """测试 GraphStore"""
    
    @pytest.fixture
    def temp_storage(self):
        """创建临时存储目录"""
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        shutil.rmtree(temp_dir)
    
    @pytest.fixture
    def graph_store(self, temp_storage):
        """创建 GraphStore 实例"""
        return GraphStore(temp_storage)
    
    def test_init(self, temp_storage):
        """测试初始化"""
        store = GraphStore(temp_storage)
        assert store.storage_dir == Path(temp_storage)
        assert store.graph.number_of_nodes() == 0
        assert store.graph.number_of_edges() == 0
    
    def test_add_entity(self, graph_store):
        """测试添加实体"""
        entity = Entity(
            id=generate_id(),
            name="张三",
            type="人物",
            description="一名工程师"
        )
        
        entity_id = graph_store.add_entity(entity)
        
        assert entity_id == entity.id
        assert entity_id in graph_store.entities
        assert "张三" in graph_store.entity_name_to_id
        assert graph_store.graph.number_of_nodes() == 1
    
    def test_add_duplicate_entity(self, graph_store):
        """测试添加重复实体"""
        entity1 = Entity(
            id=generate_id(),
            name="张三",
            type="人物",
            description="描述1"
        )
        entity2 = Entity(
            id=generate_id(),
            name="张三",
            type="人物",
            description="描述2"
        )
        
        id1 = graph_store.add_entity(entity1)
        id2 = graph_store.add_entity(entity2)
        
        # 应该返回已存在的实体ID
        assert id1 == id2
        assert graph_store.graph.number_of_nodes() == 1
    
    def test_get_entity(self, graph_store):
        """测试获取实体"""
        entity = Entity(
            id="test-id",
            name="张三",
            type="人物",
            description="一名工程师"
        )
        graph_store.add_entity(entity)
        
        retrieved = graph_store.get_entity("test-id")
        
        assert retrieved is not None
        assert retrieved.name == "张三"
        assert retrieved.type == "人物"
    
    def test_get_entity_by_name(self, graph_store):
        """测试通过名称获取实体"""
        entity = Entity(
            id="test-id",
            name="张三",
            type="人物",
            description="一名工程师"
        )
        graph_store.add_entity(entity)
        
        retrieved = graph_store.get_entity_by_name("张三")
        
        assert retrieved is not None
        assert retrieved.id == "test-id"
    
    def test_add_relation(self, graph_store):
        """测试添加关系"""
        # 先添加两个实体
        entity1 = Entity(id="e1", name="张三", type="人物", description="")
        entity2 = Entity(id="e2", name="李四", type="人物", description="")
        graph_store.add_entity(entity1)
        graph_store.add_entity(entity2)
        
        relation = Relation(
            id="r1",
            source_id="e1",
            target_id="e2",
            source_name="张三",
            target_name="李四",
            relation_type="同事"
        )
        
        relation_id = graph_store.add_relation(relation)
        
        assert relation_id == "r1"
        assert relation_id in graph_store.relations
        assert graph_store.graph.number_of_edges() == 1
    
    def test_get_relations_by_entity(self, graph_store):
        """测试获取实体的关系"""
        # 添加实体和关系
        entity1 = Entity(id="e1", name="张三", type="人物", description="")
        entity2 = Entity(id="e2", name="李四", type="人物", description="")
        graph_store.add_entity(entity1)
        graph_store.add_entity(entity2)
        
        relation = Relation(
            id="r1",
            source_id="e1",
            target_id="e2",
            source_name="张三",
            target_name="李四",
            relation_type="同事"
        )
        graph_store.add_relation(relation)
        
        relations = graph_store.get_relations_by_entity("e1")
        
        assert len(relations) == 1
        assert relations[0].relation_type == "同事"
    
    def test_get_neighbors(self, graph_store):
        """测试获取邻居"""
        # 创建简单的图结构
        entities = [
            Entity(id="e1", name="中心", type="人物", description=""),
            Entity(id="e2", name="邻居1", type="人物", description=""),
            Entity(id="e3", name="邻居2", type="人物", description=""),
            Entity(id="e4", name="远处", type="人物", description=""),
        ]
        for e in entities:
            graph_store.add_entity(e)
        
        relations = [
            Relation(id="r1", source_id="e1", target_id="e2", relation_type="认识"),
            Relation(id="r2", source_id="e1", target_id="e3", relation_type="认识"),
            Relation(id="r3", source_id="e2", target_id="e4", relation_type="认识"),
        ]
        for r in relations:
            graph_store.add_relation(r)
        
        neighbors = graph_store.get_neighbors("e1", depth=1)
        
        assert 1 in neighbors
        assert len(neighbors[1]) == 2  # e2 和 e3
    
    def test_add_text_chunk(self, graph_store):
        """测试添加文本块"""
        chunk = TextChunk(
            id="chunk-1",
            text="测试文本",
            index=0,
            document_id="doc-1"
        )
        
        chunk_id = graph_store.add_text_chunk(chunk)
        
        assert chunk_id == "chunk-1"
        assert chunk_id in graph_store.text_chunks
    
    def test_save_and_load(self, temp_storage):
        """测试数据保存和加载"""
        # 创建存储并添加数据
        store1 = GraphStore(temp_storage)
        entity = Entity(id="e1", name="张三", type="人物", description="一名工程师")
        store1.add_entity(entity)
        store1.save_data()
        
        # 创建新的存储实例，应该加载已有数据
        store2 = GraphStore(temp_storage)
        
        assert "e1" in store2.entities
        assert store2.get_entity_by_name("张三") is not None
    
    def test_get_graph_stats(self, graph_store):
        """测试获取图统计信息"""
        # 添加一些数据
        entity1 = Entity(id="e1", name="张三", type="人物", description="")
        entity2 = Entity(id="e2", name="李四", type="人物", description="")
        graph_store.add_entity(entity1)
        graph_store.add_entity(entity2)
        
        relation = Relation(id="r1", source_id="e1", target_id="e2", relation_type="同事")
        graph_store.add_relation(relation)
        
        stats = graph_store.get_graph_stats()
        
        assert stats["num_nodes"] == 2
        assert stats["num_edges"] == 1
        assert stats["num_entities"] == 2
        assert stats["num_relations"] == 1
    
    def test_clear(self, graph_store):
        """测试清空数据"""
        entity = Entity(id="e1", name="张三", type="人物", description="")
        graph_store.add_entity(entity)
        
        graph_store.clear()
        
        assert len(graph_store.entities) == 0
        assert len(graph_store.entity_name_to_id) == 0
        assert graph_store.graph.number_of_nodes() == 0
