#!/usr/bin/env python3
"""
从数据库读取小红书数据 → InsightEngine 分析 → ReportEngine 生成报告

用法:
    python generate_xhs_report.py --keyword "雪花秀"
    python generate_xhs_report.py --keyword "雪花秀" --limit 100
    python generate_xhs_report.py --keyword "雪花秀" --skip-insight  # 只用已有的 insight 报告生成最终报告
"""

import argparse
import sys
import os
from pathlib import Path
from datetime import datetime
from loguru import logger

# 添加项目路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))


def check_environment():
    """检查必要的环境配置"""
    logger.info("检查环境配置...")
    
    # 检查 InsightEngine
    try:
        from InsightEngine import DeepSearchAgent, create_agent
        logger.success("✓ InsightEngine 可用")
    except ImportError as e:
        logger.error(f"✗ InsightEngine 导入失败: {e}")
        return False
    
    # 检查 ReportEngine
    try:
        from ReportEngine import ReportAgent, create_agent as create_report_agent
        logger.success("✓ ReportEngine 可用")
    except ImportError as e:
        logger.error(f"✗ ReportEngine 导入失败: {e}")
        return False
    
    # 检查数据库配置
    try:
        from MindSpider.DeepSentimentCrawling.MediaCrawler.database.db_session import get_session
        logger.success("✓ 数据库配置可用")
    except ImportError as e:
        logger.error(f"✗ 数据库配置导入失败: {e}")
        return False
    
    return True


def run_insight_analysis(keyword: str, limit: int = 50) -> str:
    """
    运行 InsightEngine 分析
    
    Args:
        keyword: 搜索关键词（对应 source_keyword）
        limit: 限制分析的笔记数量
    
    Returns:
        生成的 Markdown 报告路径
    """
    logger.info(f"\n{'='*60}")
    logger.info(f"步骤 1: 运行 InsightEngine 分析")
    logger.info(f"关键词: {keyword}")
    logger.info(f"{'='*60}\n")
    
    from InsightEngine import create_agent
    
    # 创建 agent
    agent = create_agent()
    
    # 构造查询语句（聚焦品牌内容/选题/爆款分析）
    query = (
        f"分析小红书平台上关于「{keyword}」的品牌内容表现，"
        f"重点输出：热点选题方向、热词云、爆款产品与爆款笔记特征，"
        f"以及后续内容投放和选题策略建议。"
    )
    
    # 执行分析（会自动从数据库读取数据）
    logger.info(f"开始分析，这可能需要几分钟...")
    report_content = agent.research(query=query, save_report=True)
    
    # 找到生成的报告文件
    output_dir = Path(agent.config.OUTPUT_DIR)
    report_files = sorted(output_dir.glob("*.md"), key=lambda p: p.stat().st_mtime, reverse=True)
    
    if report_files:
        latest_report = report_files[0]
        logger.success(f"✓ InsightEngine 报告已生成: {latest_report}")
        return str(latest_report)
    else:
        logger.error("✗ 未找到生成的报告文件")
        return None


def run_report_generation(insight_report_path: str, keyword: str) -> dict:
    """
    运行 ReportEngine 生成最终报告
    
    Args:
        insight_report_path: InsightEngine 生成的报告路径
        keyword: 关键词（用于报告标题）
    
    Returns:
        包含 html_content 和文件路径的字典
    """
    logger.info(f"\n{'='*60}")
    logger.info(f"步骤 2: 运行 ReportEngine 生成最终报告")
    logger.info(f"{'='*60}\n")
    
    from ReportEngine import create_agent as create_report_agent
    
    # 创建 agent
    agent = create_report_agent()
    
    # 读取 InsightEngine 报告
    with open(insight_report_path, 'r', encoding='utf-8') as f:
        insight_content = f.read()
    
    # 构造查询（用于 ReportEngine 报告标题）
    query = f"小红书「{keyword}」品牌内容与爆款分析报告"
    
    # 生成报告
    logger.info(f"开始生成最终报告，这可能需要几分钟...")
    result = agent.generate_report(
        query=query,
        reports=[insight_content],  # 传入 InsightEngine 的报告
        forum_logs="",  # 不使用论坛日志
        custom_template="",  # 使用自动模板选择
        save_report=True,  # 自动保存
        stream_handler=None  # 命令行模式不需要流式输出
    )
    
    logger.success(f"✓ ReportEngine 报告已生成")
    if 'html_path' in result:
        logger.info(f"  HTML 报告: {result['html_path']}")
    if 'ir_path' in result:
        logger.info(f"  IR 文件: {result['ir_path']}")
    
    return result


def main():
    parser = argparse.ArgumentParser(
        description="从数据库读取小红书数据并生成分析报告"
    )
    parser.add_argument(
        "--keyword", "-k",
        required=True,
        help="搜索关键词（对应数据库中的 source_keyword）"
    )
    parser.add_argument(
        "--limit", "-l",
        type=int,
        default=50,
        help="限制分析的笔记数量（默认 50）"
    )
    parser.add_argument(
        "--skip-insight",
        action="store_true",
        help="跳过 InsightEngine 分析，直接使用已有报告生成最终报告"
    )
    parser.add_argument(
        "--insight-report",
        help="指定已有的 InsightEngine 报告路径（配合 --skip-insight 使用）"
    )
    
    args = parser.parse_args()
    
    # 检查环境
    if not check_environment():
        logger.error("环境检查失败，请确保已正确安装 InsightEngine 和 ReportEngine")
        sys.exit(1)
    
    try:
        # 步骤 1: InsightEngine 分析
        if args.skip_insight:
            if not args.insight_report:
                logger.error("使用 --skip-insight 时必须指定 --insight-report")
                sys.exit(1)
            insight_report_path = args.insight_report
            logger.info(f"跳过 InsightEngine 分析，使用已有报告: {insight_report_path}")
        else:
            insight_report_path = run_insight_analysis(args.keyword, args.limit)
            if not insight_report_path:
                logger.error("InsightEngine 分析失败")
                sys.exit(1)
        
        # 步骤 2: ReportEngine 生成最终报告
        result = run_report_generation(insight_report_path, args.keyword)
        
        logger.success(f"\n{'='*60}")
        logger.success("✓ 完整报告生成流程完成！")
        logger.success(f"{'='*60}\n")
        
    except KeyboardInterrupt:
        logger.warning("\n用户中断")
        sys.exit(1)
    except Exception as e:
        logger.exception(f"发生错误: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

