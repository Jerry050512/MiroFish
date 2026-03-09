"""
GraphRAG 本地记忆服务
提供与 Zep 兼容的统一接口
"""

import os
import threading
import time
from typing import Dict, Any, List, Optional, Callable
from queue import Queue, Empty
from dataclasses import dataclass
from datetime import datetime

from ...config import Config
from ...utils.logger import get_logger
from ...utils.llm_client import LLMClient
from .models import Entity, Relation, generate_id
from .store import GraphStore
from .indexer import IndexingPipeline
from .retriever import GraphRetriever, LocalSearchResult, GlobalSearchResult

logger = get_logger('mirofish.graphrag.memory_service')


@dataclass
class AgentActivity:
    """Agent活动记录"""
    platform: str
    agent_id: int
    agent_name: str
    action_type: str
    action_args: Dict[str, Any]
    round_num: int
    timestamp: str
    
    def to_text(self) -> str:
        """转换为文本描述"""
        action_descriptions = {
            "CREATE_POST": self._describe_create_post,
            "LIKE_POST": self._describe_like_post,
            "DISLIKE_POST": self._describe_dislike_post,
            "REPOST": self._describe_repost,
            "QUOTE_POST": self._describe_quote_post,
            "FOLLOW": self._describe_follow,
            "CREATE_COMMENT": self._describe_create_comment,
            "LIKE_COMMENT": self._describe_like_comment,
            "DISLIKE_COMMENT": self._describe_dislike_comment,
            "SEARCH_POSTS": self._describe_search,
            "SEARCH_USER": self._describe_search_user,
            "MUTE": self._describe_mute,
        }
        
        describe_func = action_descriptions.get(self.action_type, self._describe_generic)
        description = describe_func()
        
        return f"{self.agent_name}: {description}"
    
    def _describe_create_post(self) -> str:
        content = self.action_args.get("content", "")
        return f"发布了一条帖子：「{content}」" if content else "发布了一条帖子"
    
    def _describe_like_post(self) -> str:
        post_content = self.action_args.get("post_content", "")
        post_author = self.action_args.get("post_author_name", "")
        if post_content and post_author:
            return f"点赞了{post_author}的帖子：「{post_content}」"
        return "点赞了一条帖子"
    
    def _describe_dislike_post(self) -> str:
        post_content = self.action_args.get("post_content", "")
        post_author = self.action_args.get("post_author_name", "")
        if post_content and post_author:
            return f"踩了{post_author}的帖子：「{post_content}」"
        return "踩了一条帖子"
    
    def _describe_repost(self) -> str:
        original_content = self.action_args.get("original_content", "")
        original_author = self.action_args.get("original_author_name", "")
        if original_content and original_author:
            return f"转发了{original_author}的帖子：「{original_content}」"
        return "转发了一条帖子"
    
    def _describe_quote_post(self) -> str:
        original_content = self.action_args.get("original_content", "")
        original_author = self.action_args.get("original_author_name", "")
        quote_content = self.action_args.get("quote_content", "")
        base = f"引用了{original_author}的帖子" if original_author else "引用了一条帖子"
        if quote_content:
            base += f"，并评论道：「{quote_content}」"
        return base
    
    def _describe_follow(self) -> str:
        target_user_name = self.action_args.get("target_user_name", "")
        return f"关注了用户「{target_user_name}」" if target_user_name else "关注了一个用户"
    
    def _describe_create_comment(self) -> str:
        content = self.action_args.get("content", "")
        return f"评论道：「{content}」" if content else "发表了评论"
    
    def _describe_like_comment(self) -> str:
        return "点赞了一条评论"
    
    def _describe_dislike_comment(self) -> str:
        return "踩了一条评论"
    
    def _describe_search(self) -> str:
        query = self.action_args.get("query", "")
        return f"搜索了「{query}」" if query else "进行了搜索"
    
    def _describe_search_user(self) -> str:
        return "搜索了用户"
    
    def _describe_mute(self) -> str:
        target_user_name = self.action_args.get("target_user_name", "")
        return f"屏蔽了用户「{target_user_name}」" if target_user_name else "屏蔽了一个用户"
    
    def _describe_generic(self) -> str:
        return f"执行了{self.action_type}操作"


class LocalMemoryService:
    """
    本地 GraphRAG 记忆服务
    
    提供与 Zep 兼容的接口，支持：
    - 添加/更新记忆（文本）
    - 搜索记忆（Local Search / Global Search）
    - 实体查询
    - 关系查询
    """
    
    def __init__(
        self,
        storage_dir: Optional[str] = None,
        llm_client: Optional[LLMClient] = None
    ):
        """
        初始化本地记忆服务
        
        Args:
            storage_dir: 存储目录（默认使用项目根目录下的 data/graphrag）
            llm_client: LLM客户端
        """
        if storage_dir is None:
            # 默认存储在项目根目录
            project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
            storage_dir = os.path.join(project_root, "data", "graphrag")
        
        self.storage_dir = storage_dir
        self.llm_client = llm_client or LLMClient()
        
        # 初始化组件
        self.graph_store = GraphStore(storage_dir)
        self.indexer = IndexingPipeline(self.graph_store, llm_client)
        self.retriever = GraphRetriever(self.graph_store, llm_client)
        
        logger.info(f"LocalMemoryService initialized: {storage_dir}")
    
    # ==================== 记忆管理 ====================
    
    def add_memory(
        self,
        text: str,
        memory_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        添加记忆
        
        Args:
            text: 记忆文本
            memory_id: 记忆ID（可选）
            metadata: 元数据（可选）
        
        Returns:
            记忆ID
        """
        if memory_id is None:
            memory_id = generate_id()
        
        logger.info(f"Adding memory: {memory_id}")
        
        # 使用索引管道处理文本
        self.indexer.index_text(text, document_id=memory_id)
        
        return memory_id
    
    def add_memories(
        self,
        texts: List[str],
        memory_ids: Optional[List[str]] = None
    ) -> List[str]:
        """
        批量添加记忆
        
        Args:
            texts: 文本列表
            memory_ids: 记忆ID列表（可选）
        
        Returns:
            记忆ID列表
        """
        if memory_ids is None:
            memory_ids = [generate_id() for _ in texts]
        
        documents = list(zip(memory_ids, texts))
        self.indexer.index_documents(documents)
        
        return memory_ids
    
    def search(
        self,
        query: str,
        search_type: str = "hybrid",
        top_k: int = 10
    ) -> Dict[str, Any]:
        """
        搜索记忆
        
        Args:
            query: 查询文本
            search_type: 搜索类型 ("local", "global", "hybrid")
            top_k: 返回结果数量
        
        Returns:
            搜索结果
        """
        logger.info(f"Searching: {query} (type={search_type})")
        
        if search_type == "local":
            result = self.retriever.local_search(query, top_k_entities=top_k)
            return result.to_dict()
        
        elif search_type == "global":
            result = self.retriever.global_search(query, top_k_communities=top_k)
            return result.to_dict()
        
        else:  # hybrid
            result = self.retriever.hybrid_search(query)
            return result.to_dict()
    
    def get_entity(self, entity_name: str) -> Optional[Dict[str, Any]]:
        """
        获取实体信息
        
        Args:
            entity_name: 实体名称
        
        Returns:
            实体信息
        """
        entity = self.graph_store.get_entity_by_name(entity_name)
        if entity:
            return entity.to_dict()
        return None
    
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
        return self.retriever.get_entity_subgraph(entity_name, depth)
    
    def get_all_entities(self) -> List[Dict[str, Any]]:
        """获取所有实体"""
        return [e.to_dict() for e in self.graph_store.get_all_entities()]
    
    def get_all_relations(self) -> List[Dict[str, Any]]:
        """获取所有关系"""
        return [r.to_dict() for r in self.graph_store.get_all_relations()]
    
    def get_graph_stats(self) -> Dict[str, Any]:
        """获取图谱统计信息"""
        return self.graph_store.get_graph_stats()
    
    # ==================== 模拟记忆更新（兼容 Zep） ====================
    
    def add_agent_activity(self, activity: AgentActivity):
        """
        添加 Agent 活动（用于模拟场景）
        
        Args:
            activity: Agent 活动
        """
        text = activity.to_text()
        document_id = f"{activity.platform}_{activity.agent_id}_{activity.round_num}"
        
        self.add_memory(text, memory_id=document_id)
    
    def build_index(self):
        """构建索引（社区检测等）"""
        self.indexer.build_communities()
        logger.info("Index built successfully")
    
    def clear(self):
        """清空所有记忆"""
        self.graph_store.clear()
        self.graph_store.save_data()
        logger.info("All memories cleared")


class GraphMemoryUpdater:
    """
    GraphRAG 记忆更新器（兼容 ZepGraphMemoryUpdater 接口）
    
    用于在模拟过程中实时更新记忆
    """
    
    BATCH_SIZE = 5
    SEND_INTERVAL = 0.5
    
    def __init__(self, memory_service: LocalMemoryService):
        self.memory_service = memory_service
        
        # 活动队列
        self._activity_queue: Queue = Queue()
        
        # 缓冲区
        self._buffer: List[AgentActivity] = []
        self._buffer_lock = threading.Lock()
        
        # 控制标志
        self._running = False
        self._worker_thread: Optional[threading.Thread] = None
        
        # 统计
        self._total_activities = 0
        self._total_sent = 0
    
    def start(self):
        """启动后台工作线程"""
        if self._running:
            return
        
        self._running = True
        self._worker_thread = threading.Thread(
            target=self._worker_loop,
            daemon=True,
            name="GraphMemoryUpdater"
        )
        self._worker_thread.start()
        logger.info("GraphMemoryUpdater started")
    
    def stop(self):
        """停止后台工作线程"""
        self._running = False
        
        # 发送剩余的活动
        self._flush_buffer()
        
        if self._worker_thread and self._worker_thread.is_alive():
            self._worker_thread.join(timeout=10)
        
        logger.info(f"GraphMemoryUpdater stopped: total_activities={self._total_activities}")
    
    def _worker_loop(self):
        """工作线程循环"""
        while self._running:
            try:
                # 从队列获取活动
                activity = self._activity_queue.get(timeout=1)
                
                with self._buffer_lock:
                    self._buffer.append(activity)
                    self._total_activities += 1
                    
                    # 批量发送
                    if len(self._buffer) >= self.BATCH_SIZE:
                        self._flush_buffer()
                
            except Empty:
                # 超时，检查是否需要发送剩余数据
                with self._buffer_lock:
                    if self._buffer:
                        self._flush_buffer()
    
    def _flush_buffer(self):
        """发送缓冲区中的活动"""
        if not self._buffer:
            return
        
        activities = self._buffer[:]
        self._buffer = []
        
        # 合并文本并添加
        texts = [activity.to_text() for activity in activities]
        combined_text = "\n".join(texts)
        
        try:
            document_id = f"batch_{datetime.now().isoformat()}"
            self.memory_service.add_memory(combined_text, memory_id=document_id)
            self._total_sent += len(activities)
            
            logger.debug(f"Sent {len(activities)} activities to memory")
            
        except Exception as e:
            logger.error(f"Failed to send activities: {e}")
    
    def add_activity(self, activity: AgentActivity):
        """添加活动到队列"""
        self._activity_queue.put(activity)
    
    def get_stats(self) -> Dict[str, int]:
        """获取统计信息"""
        return {
            "total_activities": self._total_activities,
            "total_sent": self._total_sent,
            "buffer_size": len(self._buffer),
        }
