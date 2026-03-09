"""
本地 GraphRAG 集成测试脚本
测试完整的索引和查询流程
"""

import os
import sys
import tempfile
import shutil

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from app.services.local_graphrag import (
    LocalMemoryService,
    GraphMemoryUpdater,
    AgentActivity
)


def test_basic_workflow():
    """测试基本工作流程"""
    print("=" * 60)
    print("测试本地 GraphRAG 基本工作流程")
    print("=" * 60)
    
    # 创建临时存储目录
    temp_dir = tempfile.mkdtemp()
    print(f"\n1. 创建临时存储目录: {temp_dir}")
    
    try:
        # 初始化服务
        print("\n2. 初始化 LocalMemoryService...")
        memory = LocalMemoryService(storage_dir=temp_dir)
        
        # 添加记忆
        print("\n3. 添加记忆...")
        texts = [
            "张三是一名软件工程师，在阿里巴巴工作。他擅长Python和机器学习。",
            "李四是一名产品经理，在腾讯工作。他负责微信的产品设计。",
            "王五是一名数据科学家，在字节跳动工作。他专注于推荐算法。",
            "张三和李四是大学同学，他们都毕业于清华大学计算机系。",
            "阿里巴巴和腾讯是中国最大的两家互联网公司。",
        ]
        
        for i, text in enumerate(texts, 1):
            print(f"   添加记忆 {i}/{len(texts)}: {text[:30]}...")
            memory.add_memory(text, memory_id=f"doc-{i}")
        
        # 获取图谱统计
        print("\n4. 图谱统计:")
        stats = memory.get_graph_stats()
        for key, value in stats.items():
            print(f"   {key}: {value}")
        
        # 搜索记忆
        print("\n5. 搜索记忆 (Local Search)...")
        results = memory.search("张三在哪里工作？", search_type="local")
        print(f"   找到 {results.get('total_entities', 0)} 个实体")
        print(f"   找到 {results.get('total_relations', 0)} 个关系")
        
        # 获取实体信息
        print("\n6. 获取实体信息:")
        entity = memory.get_entity("张三")
        if entity:
            print(f"   实体: {entity['name']}")
            print(f"   类型: {entity['type']}")
            print(f"   描述: {entity.get('description', 'N/A')}")
        else:
            print("   未找到实体 '张三'")
        
        # 获取所有实体
        print("\n7. 所有实体列表:")
        all_entities = memory.get_all_entities()
        for entity in all_entities[:10]:  # 只显示前10个
            print(f"   - {entity['name']} ({entity['type']})")
        
        print(f"\n   共 {len(all_entities)} 个实体")
        
        print("\n8. 测试 Agent 活动记录...")
        activity = AgentActivity(
            platform="twitter",
            agent_id=1,
            agent_name="用户A",
            action_type="CREATE_POST",
            action_args={"content": "今天学习了GraphRAG，很有趣！"},
            round_num=1,
            timestamp="2024-01-01T00:00:00"
        )
        memory.add_agent_activity(activity)
        print(f"   活动文本: {activity.to_text()}")
        
        print("\n" + "=" * 60)
        print("测试完成！")
        print("=" * 60)
        
    finally:
        # 清理临时目录
        shutil.rmtree(temp_dir)
        print(f"\n清理临时目录: {temp_dir}")


def test_memory_updater():
    """测试记忆更新器"""
    print("\n" + "=" * 60)
    print("测试 GraphMemoryUpdater")
    print("=" * 60)
    
    temp_dir = tempfile.mkdtemp()
    
    try:
        memory = LocalMemoryService(storage_dir=temp_dir)
        updater = GraphMemoryUpdater(memory)
        
        print("\n1. 启动更新器...")
        updater.start()
        
        print("\n2. 添加活动...")
        activities = [
            AgentActivity(
                platform="twitter",
                agent_id=i,
                agent_name=f"用户{i}",
                action_type="CREATE_POST",
                action_args={"content": f"这是第{i}条测试帖子"},
                round_num=1,
                timestamp="2024-01-01T00:00:00"
            )
            for i in range(1, 6)
        ]
        
        for activity in activities:
            updater.add_activity(activity)
            print(f"   添加: {activity.to_text()[:40]}...")
        
        # 等待处理
        import time
        time.sleep(1)
        
        print("\n3. 停止更新器...")
        updater.stop()
        
        stats = updater.get_stats()
        print(f"\n4. 统计信息:")
        print(f"   总活动数: {stats['total_activities']}")
        print(f"   已发送: {stats['total_sent']}")
        
        print("\n测试完成！")
        
    finally:
        shutil.rmtree(temp_dir)


if __name__ == "__main__":
    print("\n本地 GraphRAG 集成测试")
    print("注意: 此测试需要配置 LLM_API_KEY\n")
    
    # 检查环境变量
    from app.config import Config
    
    if not Config.LLM_API_KEY:
        print("错误: 未配置 LLM_API_KEY")
        print("请在 .env 文件中配置 LLM_API_KEY")
        sys.exit(1)
    
    try:
        test_basic_workflow()
        test_memory_updater()
        
        print("\n" + "=" * 60)
        print("所有测试通过！")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n测试失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
