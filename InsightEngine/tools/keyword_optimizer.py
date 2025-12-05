"""
关键词优化中间件
使用 Qwen AI 将 Agent 生成的搜索词优化为更适合「小红书品牌内容/选题分析」场景的关键词
"""

from openai import OpenAI
import json
import sys
import os
from typing import List, Dict, Any
from dataclasses import dataclass

# 添加项目根目录到Python路径以导入config
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from config import settings
from loguru import logger

# 添加utils目录到Python路径
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(os.path.dirname(current_dir))
utils_dir = os.path.join(root_dir, 'utils')
if utils_dir not in sys.path:
    sys.path.append(utils_dir)

from retry_helper import with_graceful_retry, SEARCH_API_RETRY_CONFIG

@dataclass
class KeywordOptimizationResponse:
    """关键词优化响应"""
    original_query: str
    optimized_keywords: List[str]
    reasoning: str
    success: bool
    error_message: str = ""

class KeywordOptimizer:
    """关键词优化器

    使用硅基流动的 Qwen3 模型，将 Agent 生成的搜索词优化为更贴近
    「小红书品牌内容/选题分析」场景的检索关键词。
    """
    
    def __init__(self, api_key: str = None, base_url: str = None, model_name: str = None):
        """
        初始化关键词优化器
        
        Args:
            api_key: 硅基流动API密钥，如果不提供则从配置文件读取
            base_url: 接口基础地址，默认使用配置文件提供的SiliconFlow地址
        """
        self.api_key = api_key or settings.KEYWORD_OPTIMIZER_API_KEY

        if not self.api_key:
            raise ValueError("未找到硅基流动API密钥，请在config.py中设置KEYWORD_OPTIMIZER_API_KEY")

        self.base_url = base_url or settings.KEYWORD_OPTIMIZER_BASE_URL

        self.client = OpenAI(
            api_key=self.api_key,
            base_url=self.base_url
        )
        self.model = model_name or settings.KEYWORD_OPTIMIZER_MODEL_NAME
    
    def optimize_keywords(self, original_query: str, context: str = "") -> KeywordOptimizationResponse:
        """
        优化搜索关键词
        
        Args:
            original_query: Agent生成的原始搜索查询
            context: 额外的上下文信息（如段落标题、内容描述等）
            
        Returns:
            KeywordOptimizationResponse: 优化后的关键词列表
        """
        logger.info(f"🔍 关键词优化中间件: 处理查询 '{original_query}'")
        
        try:
            # 构建优化prompt
            system_prompt = self._build_system_prompt()
            user_prompt = self._build_user_prompt(original_query, context)
            
            # 调用Qwen API
            response = self._call_qwen_api(system_prompt, user_prompt)
            
            if response["success"]:
                # 解析响应
                content = response["content"]
                try:
                    # 尝试解析JSON格式的响应
                    if content.strip().startswith('{'):
                        parsed = json.loads(content)
                        keywords = parsed.get("keywords", [])
                        reasoning = parsed.get("reasoning", "")
                    else:
                        # 如果不是JSON格式，尝试从文本中提取关键词
                        keywords = self._extract_keywords_from_text(content)
                        reasoning = content
                    
                    # 验证关键词质量
                    validated_keywords = self._validate_keywords(keywords)
                    
                    logger.info(
                        f"✅ 优化成功: {len(validated_keywords)}个关键词" +
                        ("" if not validated_keywords else "\n" +
                         "\n".join([f"   {i}. '{k}'" for i, k in enumerate(validated_keywords, 1)]))
                    )
                        
                    
                    
                    return KeywordOptimizationResponse(
                        original_query=original_query,
                        optimized_keywords=validated_keywords,
                        reasoning=reasoning,
                        success=True
                    )
                
                except Exception as e:
                    logger.exception(f"⚠️ 解析响应失败，使用备用方案: {str(e)}")
                    # 备用方案：从原始查询中提取关键词
                    fallback_keywords = self._fallback_keyword_extraction(original_query)
                    return KeywordOptimizationResponse(
                        original_query=original_query,
                        optimized_keywords=fallback_keywords,
                        reasoning="API响应解析失败，使用备用关键词提取",
                        success=True
                    )
            else:
                logger.error(f"❌ API调用失败: {response['error']}")
                # 使用备用方案
                fallback_keywords = self._fallback_keyword_extraction(original_query)
                return KeywordOptimizationResponse(
                    original_query=original_query,
                    optimized_keywords=fallback_keywords,
                    reasoning="API调用失败，使用备用关键词提取",
                    success=True,
                    error_message=response['error']
                )
                
        except Exception as e:
            logger.error(f"❌ 关键词优化失败: {str(e)}")
            # 最终备用方案
            fallback_keywords = self._fallback_keyword_extraction(original_query)
            return KeywordOptimizationResponse(
                original_query=original_query,
                optimized_keywords=fallback_keywords,
                reasoning="系统错误，使用备用关键词提取",
                success=False,
                error_message=str(e)
            )
    
    def _build_system_prompt(self) -> str:
        """构建系统 prompt（品牌/小红书场景）"""
        return """你是一位专业的「小红书品牌内容与选题分析」数据挖掘专家。
你的任务是将用户提供的搜索查询优化为更适合在小红书品牌内容数据库中查找的关键词。

**核心原则**：
1. **贴近小红书用户语言**：使用普通博主/用户在小红书标题、标签、搜索栏中会真实使用的词汇
2. **避免官方/学术术语**：不使用「舆情」「传播」「倾向」「展望」「发展趋势」等研究型词汇
3. **简洁具体**：每个关键词要简洁明了，便于在标题/标签/搜索中直接使用
4. **围绕品牌/品类场景**：结合品牌名/产品名 + 场景/功效/人群/价格带等维度
5. **数量控制**：最少提供 8 个关键词，最多提供 20 个关键词
6. **避免跑题**：不要脱离初始查询的品牌、品类或核心需求

**重要提醒**：
- 每个关键词都必须是一个不可分割的独立词条，严禁在词条内部包含空格；
- 例如，应使用「雪花秀润燥精华」「秋冬干皮粉底」而不是「雪花秀 润燥 精华」。

**输出格式**：
请以 JSON 格式返回结果：
{
    "keywords": ["关键词1", "关键词2", "关键词3"],
    "reasoning": "选择这些关键词的理由（用中文简要说明）"
}

**示例（仅示意，不要照搬具体词）**：
输入："雪花秀精华 学生党 干皮 适合吗"
输出：
{
    "keywords": [
        "雪花秀精华",
        "雪花秀润燥精华",
        "雪花秀精华干皮",
        "雪花秀精华学生党",
        "干皮精华推荐",
        "学生党护肤",
        "秋冬干皮精华",
        "韩系精华推荐"
    ],
    "reasoning": "优先保留品牌和明星单品名称，同时加入典型人群（学生党）、肤质（干皮）、季节（秋冬）等维度，便于在小红书中覆盖更多真实搜索场景。"
}"""

    def _build_user_prompt(self, original_query: str, context: str) -> str:
        """构建用户prompt"""
        prompt = (
            "请将以下搜索查询优化为适合 '小红书品牌内容/选题分析' 场景的关键词："
            f"\n\n原始查询：{original_query}"
        )
        
        if context:
            prompt += f"\n\n上下文信息：{context}"
        
        prompt += "\n\n请记住：要使用小红书用户在标题/标签/搜索中真实会用的词汇，避免官方术语和学术化表达。"
        
        return prompt
    
    @with_graceful_retry(SEARCH_API_RETRY_CONFIG, default_return={"success": False, "error": "关键词优化服务暂时不可用"})
    def _call_qwen_api(self, system_prompt: str, user_prompt: str) -> Dict[str, Any]:
        """调用Qwen API"""
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.7,
            )

            if response.choices:
                content = response.choices[0].message.content
                return {"success": True, "content": content}
            else:
                return {"success": False, "error": "API返回格式异常"}
        except Exception as e:
            return {"success": False, "error": f"API调用异常: {str(e)}"}
    
    def _extract_keywords_from_text(self, text: str) -> List[str]:
        """从文本中提取关键词（当JSON解析失败时使用）"""
        # 简单的关键词提取逻辑
        lines = text.split('\n')
        keywords = []
        
        for line in lines:
            line = line.strip()
            # 查找可能的关键词
            if '：' in line or ':' in line:
                parts = line.split('：') if '：' in line else line.split(':')
                if len(parts) > 1:
                    potential_keywords = parts[1].strip()
                    # 尝试分割关键词
                    if '、' in potential_keywords:
                        keywords.extend([k.strip() for k in potential_keywords.split('、')])
                    elif ',' in potential_keywords:
                        keywords.extend([k.strip() for k in potential_keywords.split(',')])
                    else:
                        keywords.append(potential_keywords)
        
        # 如果没有找到，尝试其他方法
        if not keywords:
            # 查找引号中的内容
            import re
            quoted_content = re.findall(r'["""\'](.*?)["""\']', text)
            keywords.extend(quoted_content)
        
        # 清理和验证关键词
        cleaned_keywords = []
        for keyword in keywords[:20]:  # 最多20个
            keyword = keyword.strip().strip('"\'""''')
            if keyword and len(keyword) <= 20:  # 合理长度
                cleaned_keywords.append(keyword)
        
        return cleaned_keywords[:20]
    
    def _validate_keywords(self, keywords: List[str]) -> List[str]:
        """验证和清理关键词"""
        validated = []
        
        # 不良关键词（过于专业或官方）
        bad_keywords = {
            '态度分析', '公众反应', '情绪倾向',
            '未来展望', '发展趋势', '战略规划', '政策导向', '管理机制'
        }
        
        for keyword in keywords:
            if isinstance(keyword, str):
                keyword = keyword.strip().strip('"\'""''')
                
                # 基本验证
                if (keyword and 
                    len(keyword) <= 20 and 
                    len(keyword) >= 1 and
                    not any(bad_word in keyword for bad_word in bad_keywords)):
                    validated.append(keyword)
        
        return validated[:20]  # 最多返回20个关键词
    
    def _fallback_keyword_extraction(self, original_query: str) -> List[str]:
        """备用关键词提取方案"""
        # 简单的关键词提取逻辑
        # 移除常见的无用词汇
        stop_words = {'、'}
        
        # 分割查询
        import re
        # 按空格、标点分割
        tokens = re.split(r'[\s，。！？；：、]+', original_query)
        
        keywords = []
        for token in tokens:
            token = token.strip()
            if token and token not in stop_words and len(token) >= 2:
                keywords.append(token)
        
        # 如果没有有效关键词，使用原始查询的第一个词
        if not keywords:
            first_word = original_query.split()[0] if original_query.split() else original_query
            keywords = [first_word] if first_word else ["热门"]
        
        return keywords[:20]

# 全局实例
keyword_optimizer = KeywordOptimizer()
