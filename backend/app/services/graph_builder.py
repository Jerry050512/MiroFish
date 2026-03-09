"""
图谱构建服务
支持 Zep Cloud 与本地图谱两种模式
"""

import json
import os
import re
import time
import uuid
import threading
from dataclasses import dataclass
from typing import Dict, Any, List, Optional, Callable

from ..config import Config
from ..models.project import ProjectManager
from ..models.task import TaskManager, TaskStatus
from ..utils.llm_client import LLMClient
from ..utils.logger import get_logger
from ..utils.zep_paging import fetch_all_nodes, fetch_all_edges
from .text_processor import TextProcessor

try:
    from zep_cloud.client import Zep
    from zep_cloud import EpisodeData, EntityEdgeSourceTarget
except Exception:
    Zep = None
    EpisodeData = None
    EntityEdgeSourceTarget = None

logger = get_logger('mirofish.graph_builder')


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


class LocalGraphStore:
    """本地图谱存储"""

    @staticmethod
    def _graphs_dir() -> str:
        path = os.path.join(Config.UPLOAD_FOLDER, 'graphs')
        os.makedirs(path, exist_ok=True)
        return path

    @classmethod
    def _graph_path(cls, graph_id: str) -> str:
        return os.path.join(cls._graphs_dir(), f'{graph_id}.json')

    @classmethod
    def save_graph(cls, graph_data: Dict[str, Any]) -> str:
        graph_id = graph_data['graph_id']
        path = cls._graph_path(graph_id)
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(graph_data, f, ensure_ascii=False, indent=2)
        logger.info(f"本地图谱已保存: {graph_id} -> {path}")
        return path

    @classmethod
    def load_graph(cls, graph_id: str) -> Dict[str, Any]:
        path = cls._graph_path(graph_id)
        if not os.path.exists(path):
            raise FileNotFoundError(f"本地图谱不存在: {graph_id}")
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)

    @classmethod
    def delete_graph(cls, graph_id: str):
        path = cls._graph_path(graph_id)
        if os.path.exists(path):
            os.remove(path)
            logger.info(f"本地图谱已删除: {graph_id}")


LOCAL_EXTRACTION_SYSTEM_PROMPT = """你是一个知识图谱抽取助手。请根据给定文本块和允许的本体定义，抽取明确出现的实体与关系。

输出要求：
1. 仅输出合法 JSON，不要输出任何其他内容
2. 只抽取文本中明确提到或高度确定可归纳的实体/关系
3. 实体 type 必须从给定 entity_types 中选择
4. 关系 type 必须从给定 edge_types 中选择
5. 实体和关系尽量使用原文中的具体名称
6. 如果关系两端实体不明确，不要编造

JSON 格式：
{
  "entities": [
    {
      "name": "实体名称",
      "type": "实体类型",
      "summary": "一句话概述",
      "attributes": {"key": "value"}
    }
  ],
  "relations": [
    {
      "source_name": "源实体名称",
      "target_name": "目标实体名称",
      "type": "关系类型",
      "fact": "关系事实描述",
      "attributes": {"key": "value"}
    }
  ]
}
"""


class LocalGraphBuilder:
    """本地图谱构建器"""

    def __init__(self):
        self.llm_client = LLMClient()

    @staticmethod
    def _normalize_name(name: str) -> str:
        normalized = re.sub(r'\s+', ' ', (name or '').strip())
        return normalized.lower()

    def create_graph(self, name: str) -> str:
        return f"localgraph_{uuid.uuid4().hex[:16]}"

    def set_ontology(self, graph_id: str, ontology: Dict[str, Any]):
        return None

    def add_text_batches(
        self,
        graph_id: str,
        chunks: List[str],
        batch_size: int = 3,
        progress_callback: Optional[Callable] = None
    ) -> List[str]:
        return []

    def _build_extraction_prompt(self, chunk: str, ontology: Dict[str, Any]) -> str:
        entity_types = [item.get('name') for item in ontology.get('entity_types', []) if item.get('name')]
        edge_types = [item.get('name') for item in ontology.get('edge_types', []) if item.get('name')]
        return (
            f"允许的实体类型: {json.dumps(entity_types, ensure_ascii=False)}\n"
            f"允许的关系类型: {json.dumps(edge_types, ensure_ascii=False)}\n"
            f"本体定义: {json.dumps(ontology, ensure_ascii=False)}\n\n"
            f"文本块:\n{chunk}"
        )

    def build_local_graph(
        self,
        graph_id: str,
        graph_name: str,
        chunks: List[str],
        ontology: Dict[str, Any],
        progress_callback: Optional[Callable] = None
    ) -> Dict[str, Any]:
        entity_index: Dict[str, Dict[str, Any]] = {}
        relation_index: Dict[str, Dict[str, Any]] = {}
        total_chunks = max(len(chunks), 1)

        for idx, chunk in enumerate(chunks, start=1):
            if progress_callback:
                progress_callback(
                    f"抽取第 {idx}/{total_chunks} 个文本块中的实体关系...",
                    idx / total_chunks * 0.7
                )

            prompt = self._build_extraction_prompt(chunk, ontology)
            result = self.llm_client.chat_json(
                messages=[
                    {"role": "system", "content": LOCAL_EXTRACTION_SYSTEM_PROMPT},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
                max_tokens=4096
            )

            chunk_entities = result.get('entities', []) or []
            chunk_relations = result.get('relations', []) or []
            logger.info(
                f"本地抽取完成: chunk={idx}/{total_chunks}, entities={len(chunk_entities)}, relations={len(chunk_relations)}"
            )

            for entity in chunk_entities:
                name = (entity.get('name') or '').strip()
                entity_type = (entity.get('type') or '').strip()
                if not name or not entity_type:
                    continue
                key = f"{entity_type}::{self._normalize_name(name)}"
                existing = entity_index.get(key)
                if existing:
                    if not existing.get('summary') and entity.get('summary'):
                        existing['summary'] = entity.get('summary')
                    existing['attributes'].update(entity.get('attributes') or {})
                else:
                    entity_index[key] = {
                        'uuid': f"node_{uuid.uuid4().hex[:12]}",
                        'name': name,
                        'labels': ['Entity', entity_type],
                        'summary': entity.get('summary') or '',
                        'attributes': entity.get('attributes') or {},
                        'created_at': None,
                        '_type': entity_type,
                        '_norm_name': self._normalize_name(name),
                    }

            for relation in chunk_relations:
                source_name = (relation.get('source_name') or '').strip()
                target_name = (relation.get('target_name') or '').strip()
                relation_type = (relation.get('type') or '').strip()
                if not source_name or not target_name or not relation_type:
                    continue
                fact = (relation.get('fact') or '').strip()
                key = f"{self._normalize_name(source_name)}::{relation_type}::{self._normalize_name(target_name)}::{fact}"
                relation_index[key] = {
                    'source_name': source_name,
                    'target_name': target_name,
                    'type': relation_type,
                    'fact': fact,
                    'attributes': relation.get('attributes') or {},
                }

        if progress_callback:
            progress_callback("合并实体与关系...", 0.82)

        nodes = list(entity_index.values())
        node_name_candidates: Dict[str, List[Dict[str, Any]]] = {}
        for node in nodes:
            node_name_candidates.setdefault(node['_norm_name'], []).append(node)

        def resolve_node(name: str) -> Optional[Dict[str, Any]]:
            matches = node_name_candidates.get(self._normalize_name(name), [])
            return matches[0] if matches else None

        edges = []
        for relation in relation_index.values():
            source_node = resolve_node(relation['source_name'])
            target_node = resolve_node(relation['target_name'])
            if not source_node or not target_node:
                continue
            edges.append({
                'uuid': f"edge_{uuid.uuid4().hex[:12]}",
                'name': relation['type'],
                'fact': relation['fact'],
                'fact_type': relation['type'],
                'source_node_uuid': source_node['uuid'],
                'target_node_uuid': target_node['uuid'],
                'source_node_name': source_node['name'],
                'target_node_name': target_node['name'],
                'attributes': relation['attributes'],
                'created_at': None,
                'valid_at': None,
                'invalid_at': None,
                'expired_at': None,
                'episodes': [],
            })

        for node in nodes:
            node.pop('_type', None)
            node.pop('_norm_name', None)

        graph_data = {
            'graph_id': graph_id,
            'name': graph_name,
            'provider': 'local',
            'nodes': nodes,
            'edges': edges,
            'node_count': len(nodes),
            'edge_count': len(edges),
            'ontology': ontology,
        }

        if len(nodes) == 0:
            raise ValueError('本地图谱构建完成，但未抽取到任何实体，请检查文本内容或模型输出')

        if progress_callback:
            progress_callback("持久化本地图谱...", 0.95)

        LocalGraphStore.save_graph(graph_data)
        return graph_data

    def get_graph_data(self, graph_id: str) -> Dict[str, Any]:
        return LocalGraphStore.load_graph(graph_id)

    def delete_graph(self, graph_id: str):
        LocalGraphStore.delete_graph(graph_id)


class ZepGraphBuilder:
    """Zep 图谱构建器"""

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or Config.ZEP_API_KEY
        if not self.api_key:
            raise ValueError("ZEP_API_KEY 未配置")
        if Zep is None:
            raise ValueError("zep_cloud 未安装，无法使用 Zep 模式")
        self.client = Zep(api_key=self.api_key)
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
        task_id = self.task_manager.create_task(
            task_type="graph_build",
            metadata={
                "graph_name": graph_name,
                "chunk_size": chunk_size,
                "text_length": len(text),
            }
        )

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
        try:
            self.task_manager.update_task(
                task_id,
                status=TaskStatus.PROCESSING,
                progress=5,
                message="开始构建图谱..."
            )

            graph_id = self.create_graph(graph_name)
            self.task_manager.update_task(
                task_id,
                progress=10,
                message=f"图谱已创建: {graph_id}"
            )

            self.set_ontology(graph_id, ontology)
            self.task_manager.update_task(
                task_id,
                progress=15,
                message="本体已设置"
            )

            chunks = TextProcessor.split_text(text, chunk_size, chunk_overlap)
            total_chunks = len(chunks)
            self.task_manager.update_task(
                task_id,
                progress=20,
                message=f"文本已分割为 {total_chunks} 个块"
            )

            episode_uuids = self.add_text_batches(
                graph_id, chunks, batch_size,
                lambda msg, prog: self.task_manager.update_task(
                    task_id,
                    progress=20 + int(prog * 0.4),
                    message=msg
                )
            )

            self.task_manager.update_task(
                task_id,
                progress=60,
                message="等待Zep处理数据..."
            )

            self._wait_for_episodes(
                episode_uuids,
                lambda msg, prog: self.task_manager.update_task(
                    task_id,
                    progress=60 + int(prog * 0.3),
                    message=msg
                )
            )

            self.task_manager.update_task(
                task_id,
                progress=90,
                message="获取图谱信息..."
            )

            graph_info = self._get_graph_info(graph_id)
            self.task_manager.complete_task(task_id, {
                "graph_id": graph_id,
                "graph_info": graph_info.to_dict(),
                "chunks_processed": total_chunks,
            })

        except Exception as e:
            import traceback
            error_msg = f"{str(e)}\n{traceback.format_exc()}"
            self.task_manager.fail_task(task_id, error_msg)

    def create_graph(self, name: str) -> str:
        graph_id = f"mirofish_{uuid.uuid4().hex[:16]}"
        self.client.graph.create(
            graph_id=graph_id,
            name=name,
            description="MiroFish Social Simulation Graph"
        )
        return graph_id

    def set_ontology(self, graph_id: str, ontology: Dict[str, Any]):
        import warnings
        from typing import Optional
        from pydantic import Field
        from zep_cloud.external_clients.ontology import EntityModel, EntityText, EdgeModel

        warnings.filterwarnings('ignore', category=UserWarning, module='pydantic')
        reserved_names = {'uuid', 'name', 'group_id', 'name_embedding', 'summary', 'created_at'}

        def safe_attr_name(attr_name: str) -> str:
            if attr_name.lower() in reserved_names:
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
            episodes = [EpisodeData(data=chunk, type="text") for chunk in batch_chunks]
            try:
                batch_result = self.client.graph.add_batch(graph_id=graph_id, episodes=episodes)
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
        if not episode_uuids:
            if progress_callback:
                progress_callback("无需等待（没有 episode）", 1.0)
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

    def get_graph_data(self, graph_id: str) -> Dict[str, Any]:
        nodes = fetch_all_nodes(self.client, graph_id)
        edges = fetch_all_edges(self.client, graph_id)
        node_map = {node.uuid_: node.name or "" for node in nodes}

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
        self.client.graph.delete(graph_id=graph_id)


class GraphBuilderService:
    """图谱构建服务门面"""

    def __init__(self, api_key: Optional[str] = None):
        self.use_local = Config.USE_LOCAL_GRAPHRAG
        if self.use_local:
            self.provider = LocalGraphBuilder()
        else:
            self.provider = ZepGraphBuilder(api_key=api_key)

    def create_graph(self, name: str) -> str:
        return self.provider.create_graph(name)

    def set_ontology(self, graph_id: str, ontology: Dict[str, Any]):
        return self.provider.set_ontology(graph_id, ontology)

    def add_text_batches(self, graph_id: str, chunks: List[str], batch_size: int = 3, progress_callback: Optional[Callable] = None):
        return self.provider.add_text_batches(graph_id, chunks, batch_size=batch_size, progress_callback=progress_callback)

    def _wait_for_episodes(self, episode_uuids: List[str], progress_callback: Optional[Callable] = None, timeout: int = 600):
        if hasattr(self.provider, '_wait_for_episodes'):
            return self.provider._wait_for_episodes(episode_uuids, progress_callback, timeout)
        return None

    def get_graph_data(self, graph_id: str) -> Dict[str, Any]:
        return self.provider.get_graph_data(graph_id)

    def delete_graph(self, graph_id: str):
        return self.provider.delete_graph(graph_id)

    def build_local_graph(self, project_id: str, graph_id: str, graph_name: str, chunks: List[str], ontology: Dict[str, Any], progress_callback: Optional[Callable] = None) -> Dict[str, Any]:
        if not self.use_local:
            raise ValueError('当前不是本地 GraphRAG 模式')
        return self.provider.build_local_graph(graph_id, graph_name, chunks, ontology, progress_callback)
