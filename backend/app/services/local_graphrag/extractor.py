"""
GraphRAG 实体和关系提取器
使用 LLM 从文本中提取实体和关系
"""

import json
from typing import List, Dict, Any, Optional
from dataclasses import dataclass

from ...utils.llm_client import LLMClient
from ...utils.logger import get_logger
from .models import ExtractedEntity, ExtractedRelation

logger = get_logger('mirofish.graphrag.extractor')


ENTITY_EXTRACTION_PROMPT = """你是一个专业的信息提取助手。请从以下文本中提取所有重要的实体。

{ontology_prompt}

文本内容：
{text}

请按以下JSON格式输出提取的实体：
{{
    "entities": [
        {{
            "name": "实体名称",
            "type": "实体类型",
            "description": "实体的详细描述"
        }}
    ]
}}

注意：
1. 只输出JSON格式，不要输出其他内容
2. 提取所有重要的、有实际意义的实体
3. 实体名称应该是文本中实际出现的名称
4. 如果文本中没有重要实体，返回空数组"""


RELATION_EXTRACTION_PROMPT = """你是一个专业的关系提取助手。请从以下文本中提取实体之间的关系。

{ontology_prompt}

文本内容：
{text}

已识别的实体：
{entities}

请按以下JSON格式输出提取的关系：
{{
    "relations": [
        {{
            "source_name": "源实体名称",
            "target_name": "目标实体名称",
            "relation_type": "关系类型",
            "description": "关系的详细描述"
        }}
    ]
}}

注意：
1. 只输出JSON格式，不要输出其他内容
2. 提取所有明确或隐含的关系
3. 关系应该连接已识别的实体
4. 如果文本中没有明确关系，返回空数组"""


@dataclass
class ExtractionResult:
    """提取结果"""
    entities: List[ExtractedEntity]
    relations: List[ExtractedRelation]
    text_chunk_id: str = ""


class EntityExtractor:
    """实体提取器"""
    
    def __init__(self, llm_client: Optional[LLMClient] = None):
        self.llm_client = llm_client or LLMClient()
        self.ontology: Optional[Dict[str, Any]] = None
    
    def set_ontology(self, ontology: Dict[str, Any]):
        """设置本体"""
        self.ontology = ontology

    def extract(self, text: str) -> List[ExtractedEntity]:
        """
        从文本中提取实体
        
        Args:
            text: 输入文本
        
        Returns:
            提取的实体列表
        """
        ontology_prompt = ""
        if self.ontology:
            entity_types = [f"{et['name']} ({et.get('description', '')})" for et in self.ontology.get("entity_types", [])]
            ontology_prompt = "参考以下本体定义的实体类型：\n" + "\n".join(entity_types)
        else:
            ontology_prompt = "可选实体类型包括：人物、组织、地点、概念、事件、产品等。"

        prompt = ENTITY_EXTRACTION_PROMPT.format(text=text, ontology_prompt=ontology_prompt)
        
        try:
            content = self.llm_client.chat(
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1,
            )
            
            # 解析JSON
            content = content.strip()
            if content.startswith("```json"):
                content = content[7:]
            if content.endswith("```"):
                content = content[:-3]
            content = content.strip()
            
            data = json.loads(content)
            entities = []
            
            for entity_data in data.get("entities", []):
                entity = ExtractedEntity(
                    name=entity_data["name"],
                    type=entity_data["type"],
                    description=entity_data.get("description", "")
                )
                entities.append(entity)
            
            logger.info(f"Extracted {len(entities)} entities from text")
            return entities
            
        except Exception as e:
            logger.error(f"Entity extraction failed: {e}")
            return []


class RelationExtractor:
    """关系提取器"""
    
    def __init__(self, llm_client: Optional[LLMClient] = None):
        self.llm_client = llm_client or LLMClient()
        self.ontology: Optional[Dict[str, Any]] = None
    
    def set_ontology(self, ontology: Dict[str, Any]):
        """设置本体"""
        self.ontology = ontology

    def extract(
        self,
        text: str,
        entities: List[ExtractedEntity]
    ) -> List[ExtractedRelation]:
        """
        从文本中提取关系
        
        Args:
            text: 输入文本
            entities: 已提取的实体列表
        
        Returns:
            提取的关系列表
        """
        # 构建实体描述
        entity_descriptions = []
        for entity in entities:
            entity_descriptions.append(f"- {entity.name} ({entity.type})")
        
        entities_text = "\n".join(entity_descriptions) if entity_descriptions else "无"
        
        ontology_prompt = ""
        if self.ontology:
            edge_types = [f"{et['name']} ({et.get('description', '')})" for et in self.ontology.get("edge_types", [])]
            ontology_prompt = "参考以下本体定义的关系统：\n" + "\n".join(edge_types)
        else:
            ontology_prompt = "可选关系类型包括：属于、创建、使用、影响、参与、位于等。"

        prompt = RELATION_EXTRACTION_PROMPT.format(
            text=text,
            entities=entities_text,
            ontology_prompt=ontology_prompt
        )
        
        try:
            content = self.llm_client.chat(
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1,
            )
            
            # 解析JSON
            content = content.strip()
            if content.startswith("```json"):
                content = content[7:]
            if content.endswith("```"):
                content = content[:-3]
            content = content.strip()
            
            data = json.loads(content)
            relations = []
            
            for relation_data in data.get("relations", []):
                relation = ExtractedRelation(
                    source_name=relation_data["source_name"],
                    target_name=relation_data["target_name"],
                    relation_type=relation_data["relation_type"],
                    description=relation_data.get("description", "")
                )
                relations.append(relation)
            
            logger.info(f"Extracted {len(relations)} relations from text")
            return relations
            
        except Exception as e:
            logger.error(f"Relation extraction failed: {e}")
            return []


class GraphExtractor:
    """图提取器（组合实体和关系提取）"""
    
    def __init__(self, llm_client: Optional[LLMClient] = None):
        self.entity_extractor = EntityExtractor(llm_client)
        self.relation_extractor = RelationExtractor(llm_client)
    
    def set_ontology(self, ontology: Dict[str, Any]):
        """设置本体"""
        self.entity_extractor.set_ontology(ontology)
        self.relation_extractor.set_ontology(ontology)

    def extract(self, text: str, text_chunk_id: str = "") -> ExtractionResult:
        """
        从文本中提取实体和关系
        
        Args:
            text: 输入文本
            text_chunk_id: 文本块ID
        
        Returns:
            提取结果
        """
        # 提取实体
        entities = self.entity_extractor.extract(text)
        
        # 提取关系
        relations = self.relation_extractor.extract(text, entities)
        
        return ExtractionResult(
            entities=entities,
            relations=relations,
            text_chunk_id=text_chunk_id
        )
