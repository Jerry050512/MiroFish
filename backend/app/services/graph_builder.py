"""
图谱构建服务
支持 Zep Cloud 和本地 GraphRAG 两种模式
"""

import os
import uuid
import time
import threading
from typing import Dict, Any, List, Optional, Callable
from dataclasses import dataclass

from ..config import Config
from ..models.task import TaskManager, TaskStatus
from .text_processor import TextProcessor

# 延迟导入，根据运行时配置决定
LocalMemoryService = None
Zep = None
EpisodeData = None
EntityEdgeSourceTarget = None
fetch_all_nodes = None
fetch_all_edges = None


def _ensure_imports():
    """确保导入所需的模块"""
    global LocalMemoryService, Zep, EpisodeData, EntityEdgeSourceTarget, fetch_all_nodes, fetch_all_edges
    
    if Config.USE_LOCAL_GRAPHRAG:
        if LocalMemoryService is None:
            from .local_graphrag import LocalMemoryService as _LocalMemoryService
            LocalMemoryService = _LocalMemoryService
    else:
        if Zep is None:
            from zep_cloud.client import Zep as _Zep
            from zep_cloud import EpisodeData as _EpisodeData, EntityEdgeSourceTarget as _EntityEdgeSourceTarget
            from ..utils.zep_paging import fetch_all_nodes as _fetch_all_nodes, fetch_all_edges as _fetch_all_edges
            Zep = _Zep
            EpisodeData = _EpisodeData
            EntityEdgeSourceTarget = _EntityEdgeSourceTarget
            fetch_all_nodes = _fetch_all_nodes
            fetch_all_edges = _fetch_all_edges


@dataclass
class GraphInfo:
    """图谱信息"""
    graph_id: str
    node_count: int
    edge_count: int
    entity_types: List[str]
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "graph_id": self.graph_id,
            "node_count": self.node_count,
            "edge_count": self.edge_count,
            "entity_types": self.entity_types,
        }


class GraphBuilderService:
    """
    图谱构建服务
    支持 Zep Cloud 和本地 GraphRAG 两种模式
    """
    
    def __init__(self, api_key: Optional[str] = None):
        # 确保导入所需的模块
        _ensure_imports()
        
        self.use_local = Config.USE_LOCAL_GRAPHRAG
        
        if self.use_local:
            # 使用本地 GraphRAG
            self.local_memory = LocalMemoryService(
                storage_dir=Config.GRAPHRAG_STORAGE_DIR
            )
            self.client = None
        else:
            # 使用 Zep Cloud
            self.api_key = api_key or Config.ZEP_API_KEY
            if not self.api_key:
                raise ValueError("ZEP_API_KEY 未配置")
            self.client = Zep(api_key=self.api_key)
            self.local_memory = None
        
        self.task_manager = TaskManager()
    
    def build_graph_async(
        self,
        text: str,
        ontology: Dict[str, Any],
        graph_name: str = "MiroFish Graph",
        chunk_size: int = 500,
        chunk_overlap: int = 50,
        batch_size: int = 3
    ) -> str:
        """
        异步构建图谱
        
        Args:
            text: 输入文本
            ontology: 本体定义（来自接口1的输出）
            graph_name: 图谱名称
            chunk_size: 文本块大小
            chunk_overlap: 块重叠大小
            batch_size: 每批发送的块数量
            
        Returns:
            任务ID
        """
        # 创建任务
        task_id = self.task_manager.create_task(
            task_type="graph_build",
            metadata={
                "graph_name": graph_name,
                "chunk_size": chunk_size,
                "text_length": len(text),
                "use_local_graphrag": self.use_local,
            }
        )
        
        # 在后台线程中执行构建
        thread = threading.Thread(
            target=self._build_graph_worker,
            args=(task_id, text, ontology, graph_name, chunk_size, chunk_overlap, batch_size)
        )
        thread.daemon = True
        thread.start()
        
        return task_id
    
    def _build_graph_worker(
        self,
        task_id: str,
        text: str,
        ontology: Dict[str, Any],
        graph_name: str,
        chunk_size: int,
        chunk_overlap: int,
        batch_size: int
    ):
        """图谱构建工作线程"""
        try:
            self.task_manager.update_task(
                task_id,
                status=TaskStatus.PROCESSING,
                progress=5,
                message="开始构建图谱..."
            )
            
            if self.use_local:
                # 使用本地 GraphRAG 构建
                self._build_with_local_graphrag(
                    task_id, text, ontology, graph_name, chunk_size, chunk_overlap
                )
            else:
                # 使用 Zep Cloud 构建
                self._build_with_zep(
                    task_id, text, ontology, graph_name, chunk_size, chunk_overlap, batch_size
                )
            
        except Exception as e:
            import traceback
            error_msg = f"{str(e)}\n{traceback.format_exc()}"
            self.task_manager.fail_task(task_id, error_msg)
    
    def _build_with_local_graphrag(
        self,
        task_id: str,
        text: str,
        ontology: Dict[str, Any],
        graph_name: str,
        chunk_size: int,
        chunk_overlap: int
    ):
        """使用本地 GraphRAG 构建图谱"""
        from .local_graphrag import IndexingPipeline
        
        # 1. 创建图谱ID
        graph_id = f"local_{uuid.uuid4().hex[:16]}"
        self.task_manager.update_task(
            task_id,
            progress=10,
            message=f"本地图谱已创建: {graph_id}"
        )
        
        # 2. 设置本体（存储在 metadata 中）
        self.task_manager.update_task(
            task_id,
            progress=15,
            message="本体已设置"
        )
        
        # 3. 文本分块
        chunks = TextProcessor.split_text(text, chunk_size, chunk_overlap)
        total_chunks = len(chunks)
        self.task_manager.update_task(
            task_id,
            progress=20,
            message=f"文本已分割为 {total_chunks} 个块"
        )
        
        # 4. 使用索引管道处理文本
        def progress_callback(stage, current, total):
            progress = 20 + int((current / total) * 60) if total > 0 else 20
            self.task_manager.update_task(
                task_id,
                progress=progress,
                message=f"{stage}: {current}/{total}"
            )
        
        # 创建索引管道
        indexer = IndexingPipeline(
            self.local_memory.graph_store,
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap
        )
        
        # 处理文本
        indexer.index_text(text, document_id=graph_id, progress_callback=progress_callback)
        
        # 5. 构建社区
        self.task_manager.update_task(
            task_id,
            progress=80,
            message="构建社区..."
        )
        
        indexer.build_communities(
            levels=Config.GRAPHRAG_COMMUNITY_LEVELS,
            progress_callback=progress_callback
        )
        
        # 6. 获取图谱信息
        self.task_manager.update_task(
            task_id,
            progress=90,
            message="获取图谱信息..."
        )
        
        graph_info = self._get_local_graph_info()
        
        # 完成
        self.task_manager.complete_task(task_id, {
            "graph_id": graph_id,
            "graph_info": graph_info.to_dict(),
            "chunks_processed": total_chunks,
            "use_local_graphrag": True,
        })
    
    def _build_with_zep(
        self,
        task_id: str,
        text: str,
        ontology: Dict[str, Any],
        graph_name: str,
        chunk_size: int,
        chunk_overlap: int,
        batch_size: int
    ):
        """使用 Zep Cloud 构建图谱"""
        # 1. 创建图谱
        graph_id = self.create_graph(graph_name)
        self.task_manager.update_task(
            task_id,
            progress=10,
            message=f"图谱已创建: {graph_id}"
        )
        
        # 2. 设置本体
        self.set_ontology(graph_id, ontology)
        self.task_manager.update_task(
            task_id,
            progress=15,
            message="本体已设置"
        )
        
        # 3. 文本分块
        chunks = TextProcessor.split_text(text, chunk_size, chunk_overlap)
        total_chunks = len(chunks)
        self.task_manager.update_task(
            task_id,
            progress=20,
            message=f"文本已分割为 {total_chunks} 个块"
        )
        
        # 4. 分批发送数据
        episode_uuids = self.add_text_batches(
            graph_id, chunks, batch_size,
            lambda msg, prog: self.task_manager.update_task(
                task_id,
                progress=20 + int(prog * 0.4),  # 20-60%
                message=msg
            )
        )
        
        # 5. 等待Zep处理完成
        self.task_manager.update_task(
            task_id,
            progress=60,
            message="等待Zep处理数据..."
        )
        
        self._wait_for_episodes(
            episode_uuids,
            lambda msg, prog: self.task_manager.update_task(
                task_id,
                progress=60 + int(prog * 0.3),  # 60-90%
                message=msg
            )
        )
        
        # 6. 获取图谱信息
        self.task_manager.update_task(
            task_id,
            progress=90,
            message="获取图谱信息..."
        )
        
        graph_info = self._get_graph_info(graph_id)
        
        # 完成
        self.task_manager.complete_task(task_id, {
            "graph_id": graph_id,
            "graph_info": graph_info.to_dict(),
            "chunks_processed": total_chunks,
            "use_local_graphrag": False,
        })
    
    def create_graph(self, name: str) -> str:
        """创建图谱"""
        if self.use_local:
            graph_id = f"local_{uuid.uuid4().hex[:16]}"
            return graph_id
        else:
            graph_id = f"mirofish_{uuid.uuid4().hex[:16]}"
            self.client.graph.create(
                graph_id=graph_id,
                name=name,
                description="MiroFish Social Simulation Graph"
            )
            return graph_id
    
    def set_ontology(self, graph_id: str, ontology: Dict[str, Any]):
        """设置图谱本体"""
        if self.use_local:
            # 本地模式：存储在 extractor 中
            self.local_memory.indexer.extractor.set_ontology(ontology)
        else:
            # Zep Cloud 模式
            self._set_zep_ontology(graph_id, ontology)
    
    def _set_zep_ontology(self, graph_id: str, ontology: Dict[str, Any]):
        """设置 Zep 本体（原逻辑）"""
        import warnings
        from typing import Optional
        from pydantic import Field
        from zep_cloud.external_clients.ontology import EntityModel, EntityText, EdgeModel
        
        warnings.filterwarnings('ignore', category=UserWarning, module='pydantic')
        
        RESERVED_NAMES = {'uuid', 'name', 'group_id', 'name_embedding', 'summary', 'created_at'}
        
        def safe_attr_name(attr_name: str) -> str:
            if attr_name.lower() in RESERVED_NAMES:
                return f"entity_{attr_name}"
            return attr_name
        
        entity_types = {}
        for entity_def in ontology.get("entity_types", []):
            name = entity_def["name"]
            description = entity_def.get("description", f"A {name} entity.")
            
            attrs = {"__doc__": description}
            annotations = {}
            
            for attr_def in entity_def.get("attributes", []):
                attr_name = safe_attr_name(attr_def["name"])
                attr_desc = attr_def.get("description", attr_name)
                attrs[attr_name] = Field(description=attr_desc, default=None)
                annotations[attr_name] = Optional[EntityText]
            
            attrs["__annotations__"] = annotations
            
            entity_class = type(name, (EntityModel,), attrs)
            entity_class.__doc__ = description
            entity_types[name] = entity_class
        
        edge_definitions = {}
        for edge_def in ontology.get("edge_types", []):
            name = edge_def["name"]
            description = edge_def.get("description", f"A {name} relationship.")
            
            attrs = {"__doc__": description}
            annotations = {}
            
            for attr_def in edge_def.get("attributes", []):
                attr_name = safe_attr_name(attr_def["name"])
                attr_desc = attr_def.get("description", attr_name)
                attrs[attr_name] = Field(description=attr_desc, default=None)
                annotations[attr_name] = Optional[str]
            
            attrs["__annotations__"] = annotations
            
            class_name = ''.join(word.capitalize() for word in name.split('_'))
            edge_class = type(class_name, (EdgeModel,), attrs)
            edge_class.__doc__ = description
            
            source_targets = []
            for st in edge_def.get("source_targets", []):
                source_targets.append(
                    EntityEdgeSourceTarget(
                        source=st.get("source", "Entity"),
                        target=st.get("target", "Entity")
                    )
                )
            
            if source_targets:
                edge_definitions[name] = (edge_class, source_targets)
        
        if entity_types or edge_definitions:
            self.client.graph.set_ontology(
                graph_ids=[graph_id],
                entities=entity_types if entity_types else None,
                edges=edge_definitions if edge_definitions else None,
            )
    
    def add_text_batches(
        self,
        graph_id: str,
        chunks: List[str],
        batch_size: int = 3,
        progress_callback: Optional[Callable] = None
    ) -> List[str]:
        """分批添加文本到图谱"""
        if self.use_local:
            # 本地模式：直接通过索引管道处理
            total_chunks = len(chunks)
            for i, chunk in enumerate(chunks):
                if progress_callback:
                    progress = (i + 1) / total_chunks
                    progress_callback(f"正在处理本地块 {i+1}/{total_chunks}...", progress)

                # 直接使用 local_memory 的 indexer 处理
                self.local_memory.indexer._process_chunk_from_text(chunk, document_id=graph_id, index=i)

            # 保存数据
            self.local_memory.graph_store.save_data()
            return [f"local_chunk_{i}" for i in range(total_chunks)]
        else:
            # Zep Cloud 模式
            return self._add_zep_text_batches(graph_id, chunks, batch_size, progress_callback)
    
    def _add_zep_text_batches(
        self,
        graph_id: str,
        chunks: List[str],
        batch_size: int,
        progress_callback: Optional[Callable]
    ) -> List[str]:
        """Zep 分批添加文本"""
        episode_uuids = []
        total_chunks = len(chunks)
        
        for i in range(0, total_chunks, batch_size):
            batch_chunks = chunks[i:i + batch_size]
            batch_num = i // batch_size + 1
            total_batches = (total_chunks + batch_size - 1) // batch_size
            
            if progress_callback:
                progress = (i + len(batch_chunks)) / total_chunks
                progress_callback(
                    f"发送第 {batch_num}/{total_batches} 批数据 ({len(batch_chunks)} 块)...",
                    progress
                )
            
            episodes = [
                EpisodeData(data=chunk, type="text")
                for chunk in batch_chunks
            ]
            
            try:
                batch_result = self.client.graph.add_batch(
                    graph_id=graph_id,
                    episodes=episodes
                )
                
                if batch_result and isinstance(batch_result, list):
                    for ep in batch_result:
                        ep_uuid = getattr(ep, 'uuid_', None) or getattr(ep, 'uuid', None)
                        if ep_uuid:
                            episode_uuids.append(ep_uuid)
                
                time.sleep(1)
                
            except Exception as e:
                if progress_callback:
                    progress_callback(f"批次 {batch_num} 发送失败: {str(e)}", 0)
                raise
        
        return episode_uuids
    
    def _wait_for_episodes(
        self,
        episode_uuids: List[str],
        progress_callback: Optional[Callable] = None,
        timeout: int = 600
    ):
        """等待所有 episode 处理完成"""
        if self.use_local:
            # 本地模式：在此处执行社区构建，因为 Zep 模式下这是自动或后续步骤
            if progress_callback:
                progress_callback("正在构建本地社区...", 0.5)

            self.local_memory.indexer.build_communities(
                levels=Config.GRAPHRAG_COMMUNITY_LEVELS
            )

            if progress_callback:
                progress_callback("本地社区构建完成", 1.0)
            return

        if not episode_uuids:
            if progress_callback:
                progress_callback("无需等待", 1.0)
            return
        
        start_time = time.time()
        pending_episodes = set(episode_uuids)
        completed_count = 0
        total_episodes = len(episode_uuids)
        
        if progress_callback:
            progress_callback(f"开始等待 {total_episodes} 个文本块处理...", 0)
        
        while pending_episodes:
            if time.time() - start_time > timeout:
                if progress_callback:
                    progress_callback(
                        f"部分文本块超时，已完成 {completed_count}/{total_episodes}",
                        completed_count / total_episodes
                    )
                break
            
            for ep_uuid in list(pending_episodes):
                try:
                    episode = self.client.graph.episode.get(uuid_=ep_uuid)
                    is_processed = getattr(episode, 'processed', False)
                    
                    if is_processed:
                        pending_episodes.remove(ep_uuid)
                        completed_count += 1
                        
                except Exception:
                    pass
            
            elapsed = int(time.time() - start_time)
            if progress_callback:
                progress_callback(
                    f"Zep处理中... {completed_count}/{total_episodes} 完成, {len(pending_episodes)} 待处理 ({elapsed}秒)",
                    completed_count / total_episodes if total_episodes > 0 else 0
                )
            
            if pending_episodes:
                time.sleep(3)
        
        if progress_callback:
            progress_callback(f"处理完成: {completed_count}/{total_episodes}", 1.0)
    
    def _get_graph_info(self, graph_id: str) -> GraphInfo:
        """获取 Zep 图谱信息"""
        nodes = fetch_all_nodes(self.client, graph_id)
        edges = fetch_all_edges(self.client, graph_id)
        
        entity_types = set()
        for node in nodes:
            if node.labels:
                for label in node.labels:
                    if label not in ["Entity", "Node"]:
                        entity_types.add(label)
        
        return GraphInfo(
            graph_id=graph_id,
            node_count=len(nodes),
            edge_count=len(edges),
            entity_types=list(entity_types)
        )
    
    def _get_local_graph_info(self) -> GraphInfo:
        """获取本地图谱信息"""
        stats = self.local_memory.get_graph_stats()
        
        # 获取所有实体类型
        entity_types = set()
        for entity in self.local_memory.get_all_entities():
            entity_types.add(entity.get('type', 'Unknown'))
        
        return GraphInfo(
            graph_id="local_graph",
            node_count=stats.get('num_entities', 0),
            edge_count=stats.get('num_relations', 0),
            entity_types=list(entity_types)
        )
    
    def get_graph_data(self, graph_id: str) -> Dict[str, Any]:
        """获取完整图谱数据"""
        if self.use_local:
            return self._get_local_graph_data()
        else:
            return self._get_zep_graph_data(graph_id)
    
    def _get_local_graph_data(self) -> Dict[str, Any]:
        """获取本地图谱数据"""
        entities = self.local_memory.get_all_entities()
        relations = self.local_memory.get_all_relations()
        
        # 创建实体名称映射
        entity_map = {}
        for entity in entities:
            entity_map[entity['id']] = entity.get('name', '')
        
        nodes_data = []
        for entity in entities:
            nodes_data.append({
                "uuid": entity['id'],
                "name": entity.get('name', ''),
                "labels": [entity.get('type', 'Entity')],
                "summary": entity.get('description', ''),
                "attributes": {},
                "created_at": entity.get('created_at'),
            })
        
        edges_data = []
        for relation in relations:
            edges_data.append({
                "uuid": relation['id'],
                "name": relation.get('relation_type', ''),
                "fact": relation.get('description', ''),
                "fact_type": relation.get('relation_type', ''),
                "source_node_uuid": relation['source_id'],
                "target_node_uuid": relation['target_id'],
                "source_node_name": entity_map.get(relation['source_id'], ''),
                "target_node_name": entity_map.get(relation['target_id'], ''),
                "attributes": {},
                "created_at": relation.get('created_at'),
                "valid_at": None,
                "invalid_at": None,
                "expired_at": None,
                "episodes": relation.get('source_ids', []),
            })
        
        return {
            "graph_id": "local_graph",
            "nodes": nodes_data,
            "edges": edges_data,
            "node_count": len(nodes_data),
            "edge_count": len(edges_data),
        }
    
    def _get_zep_graph_data(self, graph_id: str) -> Dict[str, Any]:
        """获取 Zep 图谱数据"""
        nodes = fetch_all_nodes(self.client, graph_id)
        edges = fetch_all_edges(self.client, graph_id)
        
        node_map = {}
        for node in nodes:
            node_map[node.uuid_] = node.name or ""
        
        nodes_data = []
        for node in nodes:
            created_at = getattr(node, 'created_at', None)
            if created_at:
                created_at = str(created_at)
            
            nodes_data.append({
                "uuid": node.uuid_,
                "name": node.name,
                "labels": node.labels or [],
                "summary": node.summary or "",
                "attributes": node.attributes or {},
                "created_at": created_at,
            })
        
        edges_data = []
        for edge in edges:
            created_at = getattr(edge, 'created_at', None)
            valid_at = getattr(edge, 'valid_at', None)
            invalid_at = getattr(edge, 'invalid_at', None)
            expired_at = getattr(edge, 'expired_at', None)
            
            episodes = getattr(edge, 'episodes', None) or getattr(edge, 'episode_ids', None)
            if episodes and not isinstance(episodes, list):
                episodes = [str(episodes)]
            elif episodes:
                episodes = [str(e) for e in episodes]
            
            fact_type = getattr(edge, 'fact_type', None) or edge.name or ""
            
            edges_data.append({
                "uuid": edge.uuid_,
                "name": edge.name or "",
                "fact": edge.fact or "",
                "fact_type": fact_type,
                "source_node_uuid": edge.source_node_uuid,
                "target_node_uuid": edge.target_node_uuid,
                "source_node_name": node_map.get(edge.source_node_uuid, ""),
                "target_node_name": node_map.get(edge.target_node_uuid, ""),
                "attributes": edge.attributes or {},
                "created_at": str(created_at) if created_at else None,
                "valid_at": str(valid_at) if valid_at else None,
                "invalid_at": str(invalid_at) if invalid_at else None,
                "expired_at": str(expired_at) if expired_at else None,
                "episodes": episodes or [],
            })
        
        return {
            "graph_id": graph_id,
            "nodes": nodes_data,
            "edges": edges_data,
            "node_count": len(nodes_data),
            "edge_count": len(edges_data),
        }
    
    def delete_graph(self, graph_id: str):
        """删除图谱"""
        if self.use_local:
            # 本地模式：清空存储
            self.local_memory.clear()
        else:
            # Zep Cloud 模式
            self.client.graph.delete(graph_id=graph_id)
