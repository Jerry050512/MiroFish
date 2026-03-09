"""
图谱记忆更新服务
支持 Zep Cloud 和本地 GraphRAG 两种模式
将模拟中的Agent活动动态更新到图谱中
"""

import os
import time
import threading
import json
from typing import Dict, Any, List, Optional, Callable
from dataclasses import dataclass
from datetime import datetime
from queue import Queue, Empty

from ..config import Config
from ..utils.logger import get_logger

logger = get_logger('mirofish.graph_memory_updater')


@dataclass
class AgentActivity:
    """Agent活动记录"""
    platform: str           # twitter / reddit
    agent_id: int
    agent_name: str
    action_type: str        # CREATE_POST, LIKE_POST, etc.
    action_args: Dict[str, Any]
    round_num: int
    timestamp: str
    
    def to_episode_text(self) -> str:
        """
        将活动转换为可以发送给图谱的文本描述
        
        采用自然语言描述格式，让系统能够从中提取实体和关系
        """
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
        if content:
            return f"发布了一条帖子：「{content}」"
        return "发布了一条帖子"
    
    def _describe_like_post(self) -> str:
        """点赞帖子 - 包含帖子原文和作者信息"""
        post_content = self.action_args.get("post_content", "")
        post_author = self.action_args.get("post_author_name", "")
        
        if post_content and post_author:
            return f"点赞了{post_author}的帖子：「{post_content}」"
        elif post_content:
            return f"点赞了一条帖子：「{post_content}」"
        elif post_author:
            return f"点赞了{post_author}的一条帖子"
        return "点赞了一条帖子"
    
    def _describe_dislike_post(self) -> str:
        """踩帖子 - 包含帖子原文和作者信息"""
        post_content = self.action_args.get("post_content", "")
        post_author = self.action_args.get("post_author_name", "")
        
        if post_content and post_author:
            return f"踩了{post_author}的帖子：「{post_content}」"
        elif post_content:
            return f"踩了一条帖子：「{post_content}」"
        elif post_author:
            return f"踩了{post_author}的一条帖子"
        return "踩了一条帖子"
    
    def _describe_repost(self) -> str:
        """转发帖子 - 包含原帖内容和作者信息"""
        original_content = self.action_args.get("original_content", "")
        original_author = self.action_args.get("original_author_name", "")
        
        if original_content and original_author:
            return f"转发了{original_author}的帖子：「{original_content}」"
        elif original_content:
            return f"转发了一条帖子：「{original_content}」"
        elif original_author:
            return f"转发了{original_author}的一条帖子"
        return "转发了一条帖子"
    
    def _describe_quote_post(self) -> str:
        """引用帖子 - 包含原帖内容、作者信息和引用评论"""
        original_content = self.action_args.get("original_content", "")
        original_author = self.action_args.get("original_author_name", "")
        quote_content = self.action_args.get("quote_content", "")
        
        base = f"引用了{original_author}的帖子" if original_author else "引用了一条帖子"
        if original_content:
            base += f"：「{original_content}」"
        if quote_content:
            base += f"，并评论道：「{quote_content}」"
        return base
    
    def _describe_follow(self) -> str:
        """关注用户 - 包含目标用户信息"""
        target_user_name = self.action_args.get("target_user_name", "")
        if target_user_name:
            return f"关注了用户「{target_user_name}」"
        return "关注了一个用户"
    
    def _describe_create_comment(self) -> str:
        """创建评论 - 包含评论内容"""
        content = self.action_args.get("content", "")
        if content:
            return f"评论道：「{content}」"
        return "发表了评论"
    
    def _describe_like_comment(self) -> str:
        """点赞评论 - 包含评论内容和作者信息"""
        comment_content = self.action_args.get("comment_content", "")
        comment_author = self.action_args.get("comment_author_name", "")
        
        if comment_content and comment_author:
            return f"点赞了{comment_author}的评论：「{comment_content}」"
        elif comment_content:
            return f"点赞了一条评论：「{comment_content}」"
        elif comment_author:
            return f"点赞了{comment_author}的一条评论"
        return "点赞了一条评论"
    
    def _describe_dislike_comment(self) -> str:
        """踩评论 - 包含评论内容和作者信息"""
        comment_content = self.action_args.get("comment_content", "")
        comment_author = self.action_args.get("comment_author_name", "")
        
        if comment_content and comment_author:
            return f"踩了{comment_author}的评论：「{comment_content}」"
        elif comment_content:
            return f"踩了一条评论：「{comment_content}」"
        elif comment_author:
            return f"踩了{comment_author}的一条评论"
        return "踩了一条评论"
    
    def _describe_search(self) -> str:
        """搜索帖子 - 包含搜索关键词"""
        keywords = self.action_args.get("keywords", "")
        if keywords:
            return f"搜索了关键词「{keywords}」"
        return "进行了搜索"
    
    def _describe_search_user(self) -> str:
        """搜索用户 - 包含搜索的用户名"""
        username = self.action_args.get("username", "")
        if username:
            return f"搜索了用户「{username}」"
        return "搜索了用户"
    
    def _describe_mute(self) -> str:
        """屏蔽用户 - 包含目标用户信息"""
        target_user_name = self.action_args.get("target_user_name", "")
        if target_user_name:
            return f"屏蔽了用户「{target_user_name}」"
        return "屏蔽了一个用户"
    
    def _describe_generic(self) -> str:
        """通用描述"""
        return f"执行了动作「{self.action_type}」"


class BaseGraphMemoryUpdater:
    """图谱记忆更新器基类"""
    
    # 批量发送大小（每个平台累积多少条后发送）
    BATCH_SIZE = 5
    
    # 发送间隔（秒），避免请求过快
    SEND_INTERVAL = 0.5
    
    # 重试配置
    MAX_RETRIES = 3
    RETRY_DELAY = 2  # 秒
    
    # 平台名称映射（用于控制台显示）
    PLATFORM_DISPLAY_NAMES = {
        'twitter': '世界1',
        'reddit': '世界2',
    }
    
    def __init__(self, graph_id: str):
        self.graph_id = graph_id
        
        # 活动队列
        self._activity_queue: Queue = Queue()
        
        # 按平台分组的活动缓冲区
        self._platform_buffers: Dict[str, List[AgentActivity]] = {
            'twitter': [],
            'reddit': [],
        }
        self._buffer_lock = threading.Lock()
        
        # 控制标志
        self._running = False
        self._worker_thread: Optional[threading.Thread] = None
        
        # 统计
        self._total_activities = 0
        self._total_sent = 0
        self._total_items_sent = 0
        self._failed_count = 0
        self._skipped_count = 0
        
        logger.info(f"GraphMemoryUpdater 初始化完成: graph_id={graph_id}, batch_size={self.BATCH_SIZE}")
    
    def _get_platform_display_name(self, platform: str) -> str:
        """获取平台的显示名称"""
        return self.PLATFORM_DISPLAY_NAMES.get(platform.lower(), platform)
    
    def start(self):
        """启动后台工作线程"""
        if self._running:
            return
        
        self._running = True
        self._worker_thread = threading.Thread(
            target=self._worker_loop,
            daemon=True,
            name=f"GraphMemoryUpdater-{self.graph_id[:8]}"
        )
        self._worker_thread.start()
        logger.info(f"GraphMemoryUpdater 已启动: graph_id={self.graph_id}")
    
    def stop(self):
        """停止后台工作线程"""
        self._running = False
        
        # 发送剩余的活动
        self._flush_remaining()
        
        if self._worker_thread and self._worker_thread.is_alive():
            self._worker_thread.join(timeout=10)
        
        logger.info(f"GraphMemoryUpdater 已停止: graph_id={self.graph_id}, "
                   f"total_activities={self._total_activities}, "
                   f"items_sent={self._total_items_sent}")
    
    def _worker_loop(self):
        """工作线程循环"""
        while self._running:
            try:
                # 从队列获取活动
                activity = self._activity_queue.get(timeout=1)
                
                # 过滤掉 DO_NOTHING 和 TREND 动作
                if activity.action_type in ['DO_NOTHING', 'TREND', 'REFRESH']:
                    self._skipped_count += 1
                    continue
                
                platform = activity.platform.lower()
                
                with self._buffer_lock:
                    if platform not in self._platform_buffers:
                        self._platform_buffers[platform] = []
                    
                    self._platform_buffers[platform].append(activity)
                    self._total_activities += 1
                    
                    # 检查是否达到批量发送条件
                    if len(self._platform_buffers[platform]) >= self.BATCH_SIZE:
                        activities_to_send = self._platform_buffers[platform][:]
                        self._platform_buffers[platform] = []
                        
                        # 在锁外发送，避免阻塞
                        self._send_batch_activities(activities_to_send, platform)
                
            except Empty:
                # 超时，检查是否需要发送剩余数据
                with self._buffer_lock:
                    for platform, buffer in self._platform_buffers.items():
                        if buffer and len(buffer) >= self.BATCH_SIZE:
                            activities_to_send = buffer[:]
                            self._platform_buffers[platform] = []
                            self._send_batch_activities(activities_to_send, platform)
    
    def _send_batch_activities(self, activities: List[AgentActivity], platform: str):
        """发送一批活动到图谱（子类实现）"""
        raise NotImplementedError
    
    def _flush_remaining(self):
        """发送队列和缓冲区中剩余的活动"""
        # 首先处理队列中剩余的活动，添加到缓冲区
        while not self._activity_queue.empty():
            try:
                activity = self._activity_queue.get_nowait()
                platform = activity.platform.lower()
                with self._buffer_lock:
                    if platform not in self._platform_buffers:
                        self._platform_buffers[platform] = []
                    self._platform_buffers[platform].append(activity)
            except Empty:
                break
        
        # 然后发送各平台缓冲区中剩余的活动
        with self._buffer_lock:
            for platform, buffer in self._platform_buffers.items():
                if buffer:
                    display_name = self._get_platform_display_name(platform)
                    logger.info(f"发送{display_name}平台剩余的 {len(buffer)} 条活动")
                    self._send_batch_activities(buffer, platform)
            # 清空所有缓冲区
            for platform in self._platform_buffers:
                self._platform_buffers[platform] = []
    
    def add_activity(self, activity: AgentActivity):
        """添加活动到队列"""
        self._activity_queue.put(activity)
    
    def add_activity_from_dict(self, data: Dict[str, Any], platform: str):
        """
        从字典添加活动

        Args:
            data: 动作数据字典
            platform: 平台名称
        """
        try:
            activity = AgentActivity(
                platform=platform,
                agent_id=data.get("agent_id", 0),
                agent_name=data.get("agent_name", ""),
                action_type=data.get("action_type", ""),
                action_args=data.get("action_args", {}),
                round_num=data.get("round", 0),
                timestamp=data.get("timestamp", datetime.now().isoformat())
            )
            self.add_activity(activity)
        except Exception as e:
            logger.error(f"解析活动数据失败: {e}")

    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        with self._buffer_lock:
            buffer_sizes = {p: len(b) for p, b in self._platform_buffers.items()}
        
        return {
            "graph_id": self.graph_id,
            "batch_size": self.BATCH_SIZE,
            "total_activities": self._total_activities,
            "batches_sent": self._total_sent,
            "items_sent": self._total_items_sent,
            "failed_count": self._failed_count,
            "skipped_count": self._skipped_count,
            "queue_size": self._activity_queue.qsize(),
            "buffer_sizes": buffer_sizes,
            "running": self._running,
        }


class ZepGraphMemoryUpdater(BaseGraphMemoryUpdater):
    """Zep Cloud 图谱记忆更新器"""
    
    def __init__(self, graph_id: str, api_key: Optional[str] = None):
        super().__init__(graph_id)
        
        from zep_cloud.client import Zep
        
        self.api_key = api_key or Config.ZEP_API_KEY
        
        if not self.api_key:
            raise ValueError("ZEP_API_KEY未配置")
        
        self.client = Zep(api_key=self.api_key)
    
    def _send_batch_activities(self, activities: List[AgentActivity], platform: str):
        """发送一批活动到 Zep 图谱"""
        if not activities:
            return
        
        # 将多条活动合并为一条文本
        episode_texts = [activity.to_episode_text() for activity in activities]
        combined_text = "\n".join(episode_texts)
        
        # 带重试的发送
        for attempt in range(self.MAX_RETRIES):
            try:
                self.client.graph.add(
                    graph_id=self.graph_id,
                    type="text",
                    data=combined_text
                )
                
                self._total_sent += 1
                self._total_items_sent += len(activities)
                display_name = self._get_platform_display_name(platform)
                logger.info(f"成功批量发送 {len(activities)} 条{display_name}活动到 Zep 图谱 {self.graph_id}")
                return
                
            except Exception as e:
                if attempt < self.MAX_RETRIES - 1:
                    logger.warning(f"批量发送到Zep失败 (尝试 {attempt + 1}/{self.MAX_RETRIES}): {e}")
                    time.sleep(self.RETRY_DELAY * (attempt + 1))
                else:
                    logger.error(f"批量发送到Zep失败，已重试{self.MAX_RETRIES}次: {e}")
                    self._failed_count += 1


class LocalGraphMemoryUpdater(BaseGraphMemoryUpdater):
    """本地 GraphRAG 图谱记忆更新器"""
    
    def __init__(self, graph_id: str):
        super().__init__(graph_id)
        
        from .local_graphrag import LocalMemoryService
        
        self.memory_service = LocalMemoryService(
            storage_dir=Config.GRAPHRAG_STORAGE_DIR
        )
    
    def _send_batch_activities(self, activities: List[AgentActivity], platform: str):
        """发送一批活动到本地图谱"""
        if not activities:
            return
        
        # 将多条活动合并为一条文本
        episode_texts = [activity.to_episode_text() for activity in activities]
        combined_text = "\n".join(episode_texts)
        
        try:
            # 添加到本地 GraphRAG
            document_id = f"{self.graph_id}_{platform}_{int(time.time())}"
            self.memory_service.add_memory(combined_text, memory_id=document_id)
            
            self._total_sent += 1
            self._total_items_sent += len(activities)
            display_name = self._get_platform_display_name(platform)
            logger.info(f"成功批量发送 {len(activities)} 条{display_name}活动到本地图谱")
            
        except Exception as e:
            logger.error(f"批量发送到本地图谱失败: {e}")
            self._failed_count += 1


class ZepGraphMemoryManager:
    """
    管理多个模拟的图谱记忆更新器
    根据配置自动选择 Zep Cloud 或本地 GraphRAG
    """
    
    _updaters: Dict[str, BaseGraphMemoryUpdater] = {}
    _lock = threading.Lock()
    _stop_all_done = False
    
    @classmethod
    def create_updater(cls, simulation_id: str, graph_id: str) -> BaseGraphMemoryUpdater:
        """
        为模拟创建图谱记忆更新器
        
        Args:
            simulation_id: 模拟ID
            graph_id: 图谱ID
            
        Returns:
            GraphMemoryUpdater实例
        """
        with cls._lock:
            # 如果已存在，先停止旧的
            if simulation_id in cls._updaters:
                cls._updaters[simulation_id].stop()
            
            # 根据配置选择使用哪种更新器
            if Config.USE_LOCAL_GRAPHRAG:
                updater = LocalGraphMemoryUpdater(graph_id)
                logger.info(f"创建本地 GraphRAG 记忆更新器: simulation_id={simulation_id}, graph_id={graph_id}")
            else:
                updater = ZepGraphMemoryUpdater(graph_id)
                logger.info(f"创建 Zep Cloud 记忆更新器: simulation_id={simulation_id}, graph_id={graph_id}")
            
            updater.start()
            cls._updaters[simulation_id] = updater
            
            return updater
    
    @classmethod
    def get_updater(cls, simulation_id: str) -> Optional[BaseGraphMemoryUpdater]:
        """获取模拟的更新器"""
        return cls._updaters.get(simulation_id)
    
    @classmethod
    def stop_updater(cls, simulation_id: str):
        """停止并移除模拟的更新器"""
        with cls._lock:
            if simulation_id in cls._updaters:
                cls._updaters[simulation_id].stop()
                del cls._updaters[simulation_id]
                logger.info(f"已停止图谱记忆更新器: simulation_id={simulation_id}")
    
    @classmethod
    def stop_all(cls):
        """停止所有更新器"""
        # 防止重复调用
        if cls._stop_all_done:
            return
        cls._stop_all_done = True
        
        with cls._lock:
            if cls._updaters:
                for simulation_id, updater in list(cls._updaters.items()):
                    try:
                        updater.stop()
                    except Exception as e:
                        logger.error(f"停止更新器失败: simulation_id={simulation_id}, error={e}")
                cls._updaters.clear()
            logger.info("已停止所有图谱记忆更新器")
    
    @classmethod
    def get_all_stats(cls) -> Dict[str, Dict[str, Any]]:
        """获取所有更新器的统计信息"""
        return {
            sim_id: updater.get_stats() 
            for sim_id, updater in cls._updaters.items()
        }
