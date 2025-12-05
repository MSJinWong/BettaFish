#!/usr/bin/env python3
"""
测试小红书数据读取和分析流程

这个脚本会：
1. 从数据库读取指定关键词的小红书数据
2. 显示数据统计
3. 可选：运行 InsightEngine 分析
4. 可选：运行 ReportEngine 生成报告

用法:
    # 只测试数据库连接和数据读取
    python test_xhs_pipeline.py --keyword "雪花秀"
    
    # 运行完整流程（分析 + 报告）
    python test_xhs_pipeline.py --keyword "雪花秀" --full
"""

import argparse
import sys
import asyncio
from pathlib import Path
from loguru import logger

# 添加项目路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))


async def test_database_connection():
    """测试数据库连接"""
    logger.info("测试数据库连接...")
    
    try:
        from InsightEngine.utils.db import fetch_all
        
        # 简单查询测试
        result = await fetch_all("SELECT 1 as test")
        if result and result[0]['test'] == 1:
            logger.success("✓ 数据库连接成功")
            return True
        else:
            logger.error("✗ 数据库查询返回异常")
            return False
    except Exception as e:
        logger.error(f"✗ 数据库连接失败: {e}")
        logger.info("请检查 .env 文件中的数据库配置:")
        logger.info("  DB_HOST, DB_USER, DB_PASSWORD, DB_NAME, DB_PORT")
        return False


async def query_xhs_data(keyword: str):
    """查询小红书数据"""
    logger.info(f"\n查询关键词「{keyword}」的小红书数据...")
    
    from InsightEngine.utils.db import fetch_all
    
    # 查询笔记数量
    note_count_query = """
        SELECT COUNT(*) as count 
        FROM xhs_note 
        WHERE source_keyword LIKE :keyword
    """
    note_count = await fetch_all(note_count_query, {"keyword": f"%{keyword}%"})
    total_notes = note_count[0]['count'] if note_count else 0
    
    logger.info(f"  找到 {total_notes} 条笔记")
    
    if total_notes == 0:
        logger.warning(f"  未找到关键词「{keyword}」的数据")
        logger.info("  提示：检查数据库中 xhs_note 表的 source_keyword 字段")
        return None
    
    # 查询创作者数量
    creator_count_query = """
        SELECT COUNT(DISTINCT user_id) as count 
        FROM xhs_note 
        WHERE source_keyword LIKE :keyword
    """
    creator_count = await fetch_all(creator_count_query, {"keyword": f"%{keyword}%"})
    total_creators = creator_count[0]['count'] if creator_count else 0
    
    logger.info(f"  涉及 {total_creators} 个创作者")
    
    # 查询互动数据统计
    engagement_query = """
        SELECT 
            SUM(CAST(liked_count AS UNSIGNED)) as total_likes,
            SUM(CAST(collected_count AS UNSIGNED)) as total_collects,
            SUM(CAST(comment_count AS UNSIGNED)) as total_comments,
            SUM(CAST(share_count AS UNSIGNED)) as total_shares
        FROM xhs_note 
        WHERE source_keyword LIKE :keyword
    """
    engagement = await fetch_all(engagement_query, {"keyword": f"%{keyword}%"})
    
    if engagement:
        stats = engagement[0]
        logger.info(f"  互动统计:")
        logger.info(f"    点赞: {stats.get('total_likes', 0):,}")
        logger.info(f"    收藏: {stats.get('total_collects', 0):,}")
        logger.info(f"    评论: {stats.get('total_comments', 0):,}")
        logger.info(f"    分享: {stats.get('total_shares', 0):,}")
    
    # 查询最近的几条笔记
    recent_notes_query = """
        SELECT note_id, title, nickname, liked_count, comment_count
        FROM xhs_note 
        WHERE source_keyword LIKE :keyword
        ORDER BY time DESC
        LIMIT 5
    """
    recent_notes = await fetch_all(recent_notes_query, {"keyword": f"%{keyword}%"})
    
    if recent_notes:
        logger.info(f"\n  最近的 {len(recent_notes)} 条笔记:")
        for i, note in enumerate(recent_notes, 1):
            title = note.get('title', '无标题')[:30]
            logger.info(f"    {i}. {title}... (作者: {note.get('nickname', '未知')}, 点赞: {note.get('liked_count', 0)})")
    
    return {
        'total_notes': total_notes,
        'total_creators': total_creators,
        'engagement': engagement[0] if engagement else {}
    }


def run_insight_analysis(keyword: str):
    """运行 InsightEngine 分析"""
    logger.info(f"\n{'='*60}")
    logger.info("运行 InsightEngine 分析")
    logger.info(f"{'='*60}\n")
    
    try:
        from InsightEngine import create_agent
        
        agent = create_agent()
        # 聚焦品牌在小红书上的内容/选题/爆款分析
        query = (
            f"分析小红书平台上关于「{keyword}」的品牌内容表现，"
            f"重点输出：热点选题方向、热词云、爆款产品与爆款笔记特征，"
            f"以及后续内容投放和选题策略建议。"
        )
        
        logger.info("开始分析，这可能需要几分钟...")
        report_content = agent.research(query=query, save_report=True)
        
        # 找到生成的报告
        output_dir = Path(agent.config.OUTPUT_DIR)
        report_files = sorted(output_dir.glob("*.md"), key=lambda p: p.stat().st_mtime, reverse=True)
        
        if report_files:
            latest_report = report_files[0]
            logger.success(f"✓ 报告已生成: {latest_report}")
            return str(latest_report)
        else:
            logger.error("✗ 未找到生成的报告文件")
            return None
            
    except Exception as e:
        logger.exception(f"InsightEngine 分析失败: {e}")
        return None


async def main():
    parser = argparse.ArgumentParser(description="测试小红书数据读取和分析流程")
    parser.add_argument("--keyword", "-k", required=True, help="搜索关键词")
    parser.add_argument("--full", action="store_true", help="运行完整流程（包括 InsightEngine 分析）")
    
    args = parser.parse_args()
    
    logger.info(f"\n{'='*60}")
    logger.info("小红书数据分析流程测试")
    logger.info(f"{'='*60}\n")
    
    # 步骤 1: 测试数据库连接
    if not await test_database_connection():
        logger.error("数据库连接失败，退出")
        sys.exit(1)
    
    # 步骤 2: 查询数据
    data_stats = await query_xhs_data(args.keyword)
    
    if not data_stats:
        logger.error("未找到数据，退出")
        sys.exit(1)
    
    # 步骤 3: 可选 - 运行分析
    if args.full:
        insight_report = run_insight_analysis(args.keyword)
        if insight_report:
            logger.success(f"\n{'='*60}")
            logger.success("✓ 完整流程测试成功！")
            logger.success(f"{'='*60}\n")
        else:
            logger.error("分析失败")
            sys.exit(1)
    else:
        logger.success(f"\n{'='*60}")
        logger.success("✓ 数据库连接和数据查询测试成功！")
        logger.info("使用 --full 参数运行完整分析流程")
        logger.success(f"{'='*60}\n")


if __name__ == "__main__":
    asyncio.run(main())

