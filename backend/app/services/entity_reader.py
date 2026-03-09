"""
图谱实体读取服务
支持本地与 Zep 两种图谱来源
"""

from typing import Optional, List

from ..config import Config
from .graph_builder import LocalGraphStore
from .zep_entity_reader import ZepEntityReader, EntityNode, FilteredEntities


class LocalEntityReader:
    """本地图谱实体读取器"""

    def get_all_nodes(self, graph_id: str):
        graph = LocalGraphStore.load_graph(graph_id)
        return graph.get('nodes', [])

    def get_all_edges(self, graph_id: str):
        graph = LocalGraphStore.load_graph(graph_id)
        return graph.get('edges', [])

    def filter_defined_entities(
        self,
        graph_id: str,
        defined_entity_types: Optional[List[str]] = None,
        enrich_with_edges: bool = True
    ) -> FilteredEntities:
        all_nodes = self.get_all_nodes(graph_id)
        all_edges = self.get_all_edges(graph_id) if enrich_with_edges else []
        node_map = {n['uuid']: n for n in all_nodes}

        entities = []
        entity_types = set()
        for node in all_nodes:
            labels = node.get('labels', [])
            custom_labels = [l for l in labels if l not in ['Entity', 'Node']]
            if not custom_labels:
                continue
            if defined_entity_types:
                matching = [l for l in custom_labels if l in defined_entity_types]
                if not matching:
                    continue
                entity_type = matching[0]
            else:
                entity_type = custom_labels[0]
            entity_types.add(entity_type)

            entity = EntityNode(
                uuid=node.get('uuid', ''),
                name=node.get('name', ''),
                labels=labels,
                summary=node.get('summary', ''),
                attributes=node.get('attributes', {}),
            )

            if enrich_with_edges:
                related_edges = []
                related_node_uuids = set()
                for edge in all_edges:
                    if edge.get('source_node_uuid') == entity.uuid:
                        related_edges.append({
                            'direction': 'outgoing',
                            'edge_name': edge.get('name', ''),
                            'fact': edge.get('fact', ''),
                            'target_node_uuid': edge.get('target_node_uuid')
                        })
                        related_node_uuids.add(edge.get('target_node_uuid'))
                    elif edge.get('target_node_uuid') == entity.uuid:
                        related_edges.append({
                            'direction': 'incoming',
                            'edge_name': edge.get('name', ''),
                            'fact': edge.get('fact', ''),
                            'source_node_uuid': edge.get('source_node_uuid')
                        })
                        related_node_uuids.add(edge.get('source_node_uuid'))

                entity.related_edges = related_edges
                entity.related_nodes = [
                    {
                        'uuid': node_map[related_uuid].get('uuid', ''),
                        'name': node_map[related_uuid].get('name', ''),
                        'labels': node_map[related_uuid].get('labels', []),
                        'summary': node_map[related_uuid].get('summary', ''),
                    }
                    for related_uuid in related_node_uuids
                    if related_uuid in node_map
                ]

            entities.append(entity)

        return FilteredEntities(
            entities=entities,
            entity_types=entity_types,
            total_count=len(all_nodes),
            filtered_count=len(entities),
        )

    def get_entity_with_context(self, graph_id: str, entity_uuid: str):
        result = self.filter_defined_entities(graph_id=graph_id, enrich_with_edges=True)
        for entity in result.entities:
            if entity.uuid == entity_uuid:
                return entity
        return None

    def get_entities_by_type(self, graph_id: str, entity_type: str, enrich_with_edges: bool = True):
        result = self.filter_defined_entities(
            graph_id=graph_id,
            defined_entity_types=[entity_type],
            enrich_with_edges=enrich_with_edges
        )
        return result.entities


class GraphEntityReader:
    """根据配置选择实体读取器"""

    def __init__(self):
        self.reader = LocalEntityReader() if Config.USE_LOCAL_GRAPHRAG else ZepEntityReader()

    def __getattr__(self, item):
        return getattr(self.reader, item)
