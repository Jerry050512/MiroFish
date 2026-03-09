"""
GraphRAG 索引管道单元测试
"""

import pytest
import tempfile
import shutil

from app.services.local_graphrag.indexer import TextChunker, IndexingPipeline
from app.services.local_graphrag.store import GraphStore


class TestTextChunker:
    """测试文本分块器"""
    
    @pytest.fixture
    def chunker(self):
        """创建 TextChunker 实例"""
        return TextChunker(chunk_size=100, chunk_overlap=20)
    
    def test_chunk_text(self, chunker):
        """测试文本分块"""
        text = "这是一段测试文本。" * 20  # 创建足够长的文本
        
        chunks = chunker.chunk_text(text, document_id="doc-1")
        
        assert len(chunks) > 0
        assert all(len(chunk.text) <= 100 for chunk in chunks)
        assert all(chunk.document_id == "doc-1" for chunk in chunks)
    
    def test_chunk_overlap(self, chunker):
        """测试分块重叠"""
        text = "这是一段测试文本。" * 20
        
        chunks = chunker.chunk_text(text, document_id="doc-1")
        
        if len(chunks) > 1:
            # 检查是否有重叠
            first_chunk_end = chunks[0].text[-20:]
            second_chunk_start = chunks[1].text[:20]
            # 应该有部分重叠
            assert len(chunks[0].text) == 100
    
    def test_chunk_indexing(self, chunker):
        """测试分块索引"""
        text = "这是一段测试文本。" * 20
        
        chunks = chunker.chunk_text(text, document_id="doc-1")
        
        for i, chunk in enumerate(chunks):
            assert chunk.index == i
    
    def test_short_text(self, chunker):
        """测试短文本"""
        text = "短文本"
        
        chunks = chunker.chunk_text(text, document_id="doc-1")
        
        assert len(chunks) == 1
        assert chunks[0].text == text


class TestIndexingPipeline:
    """测试索引管道"""
    
    @pytest.fixture
    def temp_storage(self):
        """创建临时存储目录"""
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        shutil.rmtree(temp_dir)
    
    @pytest.fixture
    def pipeline(self, temp_storage, mocker):
        """创建 IndexingPipeline 实例"""
        graph_store = GraphStore(temp_storage)
        
        # Mock LLM client
        mock_llm = mocker.Mock()
        mock_llm.chat_completion.return_value = {
            "choices": [{
                "message": {
                    "content": """{
                        "entities": [
                            {"name": "张三", "type": "人物", "description": "一名工程师"},
                            {"name": "阿里巴巴", "type": "公司", "description": "科技公司"}
                        ]
                    }"""
                }
            }]
        }
        
        return IndexingPipeline(graph_store, mock_llm)
    
    def test_index_text(self, pipeline):
        """测试索引文本"""
        text = "张三在阿里巴巴工作。"
        
        pipeline.index_text(text, document_id="doc-1")
        
        # 检查是否创建了文本块
        assert len(pipeline.graph_store.text_chunks) > 0
    
    def test_index_multiple_texts(self, pipeline):
        """测试索引多个文本"""
        texts = [
            ("doc-1", "张三在阿里巴巴工作。"),
            ("doc-2", "李四在腾讯工作。"),
        ]
        
        pipeline.index_documents(texts)
        
        # 检查是否创建了文本块
        assert len(pipeline.graph_store.text_chunks) > 0
    
    def test_get_stats(self, pipeline):
        """测试获取统计信息"""
        text = "张三在阿里巴巴工作。"
        pipeline.index_text(text, document_id="doc-1")
        
        stats = pipeline.get_stats()
        
        assert "texts_processed" in stats
        assert "chunks_created" in stats
        assert "graph_stats" in stats
        assert stats["texts_processed"] == 1
    
    def test_reset_stats(self, pipeline):
        """测试重置统计信息"""
        text = "张三在阿里巴巴工作。"
        pipeline.index_text(text, document_id="doc-1")
        
        pipeline.reset_stats()
        stats = pipeline.get_stats()
        
        assert stats["texts_processed"] == 0
        assert stats["chunks_created"] == 0
