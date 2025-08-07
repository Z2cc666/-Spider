#!/Users/z2cc/miniconda3/bin/python
# -*- coding: UTF-8 -*-
import os
import re
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urlparse
import json
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import lru_cache

# 关键词 - 扩大范围以获取更多相关文章，包括药监局等监管部门
KEYWORDS = [
    "贪污", "腐败", "受贿", "处罚", "双开", "调查", "起诉", "纪检", "廉洁", "投案", "被查", "违规", "违纪", "处分", "免职", "撤职",
    "医院", "医生", "院长", "主任", "医疗", "药品", "回扣", "红包", "贿赂", "违法", "犯罪", "立案", "逮捕", "拘留", "判刑",
    "医保", "医保基金", "骗保", "套保", "虚开", "发票", "税务", "逃税", "漏税", "偷税", "补税", "罚款", "没收", "追缴",
    "主动投案", "被查", "被诉", "被逮捕", "被拘留", "被判刑", "被开除", "被免职", "被处分", "被双开", "被调查", "被立案",
    "副院长", "副主任", "委员", "科长", "副科长", "护士长", "药师", "院长助理", "主任医师", "副主任医师", "主治医师", "护士", "护师",
    "医疗腐败", "医药腐败", "医疗回扣", "药品回扣", "医疗贿赂", "药品贿赂", "医疗违法", "药品违法", "医疗犯罪", "药品犯罪",
    # 新增监管部门和药品相关关键词
    "药监局", "国家药监局", "药品监督", "批件", "药品批件", "药品审批", "药品注册", "药品生产", "药品销售", "药企", "制药",
    "食药监", "市场监管", "卫健委", "卫生健康委", "纪委监委", "巡视组", "审计", "监察", "通报", "新事"
]

# 健识局公众号配置
GZH_LIST = [
    {
        "name": "健识局",
        "token": "840103440",
        "cookie": "RK=DC2Uq4Wf9P; ptcz=c9f4dcf0c0fb279d2316b228ce1d2d7a6b107f591ae8bbce0eac0ce98bc9de36; wxuin=51340895845860; mm_lang=zh_CN; _hp2_id.1405110977=%7B%22userId%22%3A%226801447023479475%22%2C%22pageviewId%22%3A%228306667787246811%22%2C%22sessionId%22%3A%224504468753015668%22%2C%22identity%22%3Anull%2C%22trackerVersion%22%3A%224.0%22%7D; ua_id=mxGDXOVuOo8d0D2hAAAAACdqUxp53FqemlDjGf2eSLM=; rewardsn=; wxtokenkey=777; poc_sid=HBg3iGijlmGc_2ocHEPN26JgrEcR59UETkMwwy7P; _clck=3911425635|1|fy3|0; uuid=0f23747e8a4ce4803ac4c2e81813d9c3; rand_info=CAESICRG+nL2+PnQWtbjYd6JuRPOT89alJ8x3l0VMgP1oYnS; slave_bizuin=3911425635; data_bizuin=3911425635; bizuin=3911425635; data_ticket=0s37cmjlBOpA+6yyl1Vmc2ZL5TY2yMPaZb8t5y2aenlcBIOvu0qMjhWGtWAn5OiS; slave_sid=eFlBWmtDMTdWZUJGMFRpUUNyRGp6TFEyd2czYTRSa1NQY1RiWWtxQVY2dExTMDl4QVhGNHZGamZwUzJ1djJlelpnTjlTcmdsNGVFaXBWSXNnVDJvcEdQUGJoMWFQTzU1MzVlbnpXRXdLQWk5a2FQWWpwNXkyMk1sb0NDMGhPTkpjanF0N2dsVlR3b2prWkts; slave_user=gh_b3cdf815ccbf; xid=e437bfb7c69a0dd7d1b3c2fa393774e4; _clsk=1vy8tkg|1754033023118|3|1|mp.weixin.qq.com/weheat-agent/payload/record"
    },
]

# 创建全局会话对象，复用连接
SESSION = requests.Session()
SESSION.headers.update({
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36'
})

# 性能配置 - 提升并行处理速度
PERFORMANCE_CONFIG = {
    'max_workers': 12,  # 并发线程数，大幅提升（原来5 -> 12）
    'timeout': 15,      # 请求超时时间（秒），稍微增加应对并发
    'page_delay': 0.3,  # 页面间延迟（秒），减少延迟提速
    'cache_size': 256   # LRU缓存大小，增加缓存提升性能
}

# 预编译正则表达式以提高效率
COMPILED_REGEXES = {
    'date': re.compile(r"(\d{4}[年/-]\d{1,2}[月/-]\d{1,2}日?)|(\d{1,2}/\d{1,2}/\d{4})|(\d{4}-\d{1,2}-\d{1,2})"),
    'province': re.compile(r"(北京|天津|上海|重庆|河北|山西|辽宁|吉林|黑龙江|江苏|浙江|安徽|福建|江西|山东|河南|湖北|湖南|广东|海南|四川|贵州|云南|陕西|甘肃|青海|台湾|内蒙古|广西|西藏|宁夏|新疆|香港|澳门)"),
    'position': re.compile(r"(董事长|总裁|总经理|首席执行官|CEO|副总裁|副总经理|董事|副董事长|董事会成员|总监|副总监|经理|副经理|主管|主任医师|副主任医师|主治医师|主管医师|住院医师|党委书记|纪委书记|院长助理|主任委员|副主任委员|副院长|院长|副主任|主任|副科长|科长|副处长|处长|副局长|局长|厅长|副厅长|护士长|书记|委员|医生|药师|护士|护师|工程师|会计师|助理|秘书|专员|主办|干事|职员|工作人员)")
}

# 保持兼容性的别名（使用预编译版本）
DATE_RE = COMPILED_REGEXES['date']
PROVINCE_RE = COMPILED_REGEXES['province'] 
POSITION_RE = COMPILED_REGEXES['position']
HOSPITAL_RE = re.compile(r"[\u4e00-\u9fa5]{2,}(医院|卫生院|卫生服务中心|中医院|医科大学附属医院|人民医院|中心医院|第一医院|第二医院|第三医院)")
NAME_RE = re.compile(r"[\u4e00-\u9fa5]{2,4}")

@lru_cache(maxsize=PERFORMANCE_CONFIG['cache_size'])
def quick_title_filter(title):
    """快速标题过滤 - 使用缓存提高效率"""
    return any(keyword in title for keyword in KEYWORDS)

def normalize_date_format(date_str):
    """标准化日期格式为 xxxx/xx/xx"""
    if not date_str:
        return ""
    
    # 如果已经是正确格式，直接返回
    if re.match(r'^\d{4}/\d{2}/\d{2}$', str(date_str)):
        return str(date_str)
    
    # 处理各种可能的日期格式
    date_patterns = [
        (r'(\d{4})[年/-](\d{1,2})[月/-](\d{1,2})日?', r'\1/\2/\3'),
        (r'(\d{4})-(\d{1,2})-(\d{1,2})', r'\1/\2/\3'),
        (r'(\d{1,2})/(\d{1,2})/(\d{4})', r'\3/\1/\2'),  # MM/dd/yyyy -> yyyy/MM/dd
    ]
    
    for pattern, replacement in date_patterns:
        match = re.search(pattern, str(date_str))
        if match:
            # 确保月份和日期是两位数
            year, month, day = match.groups()
            return f"{year}/{month.zfill(2)}/{day.zfill(2)}"
    
    return str(date_str)  # 如果无法解析，返回原始字符串

def fetch_articles_batch(gzh, start_page=0, batch_size=20, page_size=10):
    """批量抓取公众号文章列表 - 优化版本使用会话复用"""
    all_articles = []
    consecutive_empty_pages = 0
    max_consecutive_empty = 3
    
    end_page = start_page + batch_size
    
    for page in range(start_page, end_page):
        params = {
            'sub': 'list',
            'search_field': 'null',
            "begin": page * page_size,
            "count": page_size,
            'query': '',
            'fakeid': gzh['fakeid'],
            'type': '101_1',
            'free_publish_type': '1',
            'sub_action': 'list_ex',
            'fingerprint': '3ac89b4e2b10b8054438ff4808dccd28',
            'token': gzh['token'],
            'lang': 'zh_CN',
            'f': 'json',
            'ajax': '1',
        }
        headers = {
            'cookie': gzh['cookie'],
            'referer': f'https://mp.weixin.qq.com/cgi-bin/appmsg?t=media/appmsg_edit_v2&action=edit&isNew=1&type=77&createType=0&token={gzh["token"]}&lang=zh_CN&timestamp=1754033020095',
            'accept': '*/*',
            'accept-language': 'zh-CN,zh;q=0.9',
            'accept-encoding': 'gzip, deflate, br, zstd',
            'sec-ch-ua': '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'x-requested-with': 'XMLHttpRequest',
            'priority': 'u=1, i'
        }
        try:
            # 使用全局会话复用连接
            resp = SESSION.get("https://mp.weixin.qq.com/cgi-bin/appmsgpublish", 
                             params=params, headers=headers, timeout=PERFORMANCE_CONFIG['timeout'])
            print(f"第{page+1}页请求状态码: {resp.status_code}")
            if resp.status_code != 200:
                print(f"请求失败，状态码: {resp.status_code}")
                consecutive_empty_pages += 1
                if consecutive_empty_pages >= max_consecutive_empty:
                    break
                continue
                
            result = resp.json()
            if result.get("base_resp", {}).get("ret") != 0:
                print(f"API返回错误: {result.get('base_resp', {})}")
                consecutive_empty_pages += 1
                if consecutive_empty_pages >= max_consecutive_empty:
                    break
                continue
                
            publish_page = result.get("publish_page")
            if not publish_page:
                print("未找到publish_page数据")
                consecutive_empty_pages += 1
                if consecutive_empty_pages >= max_consecutive_empty:
                    print(f"连续{max_consecutive_empty}页无数据，停止本批次")
                    break
                continue
                
            articleList = json.loads(publish_page)
            articles = articleList.get("publish_list", [])
            if not articles:
                print(f"第{page+1}页没有文章")
                consecutive_empty_pages += 1
                if consecutive_empty_pages >= max_consecutive_empty:
                    print(f"连续{max_consecutive_empty}页无文章，停止本批次")
                    break
                continue
            else:
                consecutive_empty_pages = 0
            
            print(f"第{page+1}页获取到{len(articles)}篇文章")
            all_articles.extend(articles)
            
            # 使用配置的页面间延迟
            time.sleep(PERFORMANCE_CONFIG['page_delay'])
                
        except Exception as e:
            print(f"{gzh['name']} 第{page+1}页抓取失败: {e}")
            consecutive_empty_pages += 1
            if consecutive_empty_pages >= max_consecutive_empty:
                print(f"连续{max_consecutive_empty}次失败，停止本批次")
                break
            time.sleep(2)  # 失败时延迟减少
            continue
    
    success = len(all_articles) > 0
    print(f"批次获取完成，共获取到 {len(all_articles)} 篇文章")
    return all_articles, success

def get_article_content(link, gzh_name, session=None):
    """获取文章正文 - 优化版本使用会话复用"""
    if session is None:
        session = SESSION
        
    try:
        headers = {
            'referer': 'https://mp.weixin.qq.com/cgi-bin/appmsg?t=media/appmsg_edit_v2&action=edit&isNew=1&type=77&createType=0&token=840103440&lang=zh_CN&timestamp=1754033020095',
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'accept-language': 'zh-CN,zh;q=0.9',
            'sec-ch-ua': '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin'
        }
        resp = session.get(link, headers=headers, timeout=PERFORMANCE_CONFIG['timeout'])
        
        if resp.status_code != 200:
            return ''
            
        soup = BeautifulSoup(resp.text, 'html.parser')
        
        # 尝试多种方式获取内容
        content_div = soup.find('div', {'id': 'js_content'}) or \
                     soup.find('div', class_='rich_media_content') or \
                     soup.find('div', class_='content') or \
                     soup.find('article')
        
        if content_div:
            # 移除脚本和样式标签
            for script in content_div(["script", "style"]):
                script.decompose()
            
            text = content_div.get_text(separator='\n', strip=True)
            return text
        else:
            # 如果找不到特定区域，尝试获取整个body
            body = soup.find('body')
            if body:
                text = body.get_text(separator='\n', strip=True)
                return text
            return ''
            
    except Exception as e:
        # 减少错误输出，避免影响性能
        return ''

def process_single_article(article_data):
    """处理单篇文章的函数 - 用于并发处理"""
    try:
        article, gzh_name, create_time = article_data
        title = article.get("title", "")
        link = article.get("link", "")
        
        # 快速标题预筛选
        if not quick_title_filter(title):
            return None
            
        # 获取文章内容
        content = get_article_content(link, gzh_name)
        
        # 内容关键词筛选
        if not any(k in content for k in KEYWORDS):
            return None
            
        return {
            'title': title,
            'link': link,
            'content': content,
            'create_time': create_time
        }
    except Exception as e:
        return None

def extract_first(pattern, text):
    """改进的正则提取第一个匹配"""
    if not text:
        return ''
    
    matches = pattern.findall(text)
    if matches:
        if isinstance(matches[0], tuple):
            for match in matches:
                for group in match:
                    if group and len(group) > 1:
                        return group
        else:
            return max(matches, key=len)
    
    return ''

def is_incomplete_name(name):
    """检查是否为不完整的姓名"""
    if len(name) < 2:
        return True
    
    # 检查是否为常见的不完整姓名模式
    incomplete_patterns = [
        r'^[王李张刘陈杨赵黄周吴]生$',  # X生结尾的
        r'^[王李张刘陈杨赵黄周吴]医$',  # X医结尾的
        r'^[王李张刘陈杨赵黄周吴]院$',  # X院结尾的
        r'^[王李张刘陈杨赵黄周吴]长$',  # X长结尾的
        r'^[王李张刘陈杨赵黄周吴]任$',  # X任结尾的
        r'^[王李张刘陈杨赵黄周吴]主$',  # X主结尾的
        r'^[王李张刘陈杨赵黄周吴]委$',  # X委结尾的
        r'^[王李张刘陈杨赵黄周吴]局$',  # X局结尾的
    ]
    
    for pattern in incomplete_patterns:
        if re.match(pattern, name):
            return True
    
    # 检查是否为单独的常见字（可能是职位的一部分）
    common_single_chars = ['生', '医', '院', '长', '任', '主', '委', '局', '处', '科', '护', '师', '员']
    if len(name) == 2 and name[1] in common_single_chars:
        return True
        
    return False

def extract_name(text):
    """平衡的姓名提取逻辑 - 在准确性和召回率之间取得平衡"""
    names = []
    
    # 扩展的中文姓氏列表（恢复更多姓氏以提高召回率）
    common_surnames = ['王', '李', '张', '刘', '陈', '杨', '赵', '黄', '周', '吴', '徐', '孙', '胡', '朱', '高', '林', '何', '郭', '马', '罗', '梁', '宋', '郑', '谢', '韩', '唐', '冯', '于', '董', '萧', '程', '曹', '袁', '邓', '许', '傅', '沈', '曾', '彭', '吕', '苏', '卢', '蒋', '蔡', '贾', '丁', '魏', '薛', '叶', '阎', '余', '潘', '杜', '戴', '夏', '钟', '汪', '田', '任', '姜', '范', '方', '石', '姚', '谭', '廖', '邹', '熊', '金', '陆', '郝', '孔', '白', '崔', '康', '毛', '邱', '秦', '江', '史', '顾', '侯', '邵', '孟', '龙', '万', '段', '雷', '钱', '汤', '尹', '黎', '易', '常', '武', '乔', '贺', '赖', '龚', '文', '阚', '骆', '邢', '严', '孟', '吴']
    
    # 违法关键词 - 这些词不应该出现在姓名中
    violation_suffix = ['被', '涉', '遭', '因', '于', '已', '正', '将', '曾', '等', '与', '及', '接', '收', '获', '取', '给', '送', '让', '向', '从', '在', '到', '对', '为', '是', '有', '无', '不', '未', '再', '还', '都', '也', '却', '但', '而', '则', '即', '就', '既']
    
    # 精简的无效词汇列表（只保留最明确的误识别词汇）
    invalid_words = [
        # 基础无效词
        '健识局', '赛柏蓝', '医院', '主任医师', '副主任医师', '主治医师', '护师', '董事长', '副总裁',
        # 明确的错误识别词汇（从实际数据中观察到的）
        '方结果', '康方生物', '生物医', '于文明', '李健康', '赵德华', '董事会', '委员会',
        '方生物', '物医代', '代表团', '调查组', '官方结', '结果公', '公布关', '关于医',
        '医学专', '专家被', '被查经', '经调取', '取重医', '医生市', '市卫生', '生健医',
        '医长北', '北京这', '这两医', '医生贵', '贵州省', '省人方'
    ]
    
    # 方法1: 查找"X某"、"X某某"等匿名模式
    for surname in common_surnames:
        # 查找各种匿名模式
        anonymous_patterns = [
            f"{surname}某某某",  # 先匹配更长的
            f"{surname}某某",
            f"{surname}某"
        ]
        for pattern in anonymous_patterns:
            if pattern in text:
                names.append(pattern)
                break  # 找到一个就跳出，避免重复
    
    # 方法2: 查找完整姓名在违法语境中的情况（恢复原有逻辑但改进）
    violation_keywords = ['被查', '被双开', '被开除', '被免职', '被处分', '涉嫌', '被逮捕', '被拘留', '被判刑', '主动投案', '被诉', '被调查', '被立案', '被撤职']
    
    for keyword in violation_keywords:
        keyword_pos = 0
        while True:
            keyword_pos = text.find(keyword, keyword_pos)
            if keyword_pos == -1:
                break
            
            # 在关键词前查找可能的姓名
            start_pos = max(0, keyword_pos - 15)
            before_keyword = text[start_pos:keyword_pos]
            
            for i in range(len(before_keyword) - 1, -1, -1):
                if before_keyword[i] in common_surnames:
                    # 只提取姓名部分，不包含违法关键词
                    for j in range(i + 2, min(i + 5, len(before_keyword) + 1)):  # 2-4字姓名
                        candidate_name = before_keyword[i:j]
                        if (2 <= len(candidate_name) <= 4 and
                            all('\u4e00' <= c <= '\u9fff' for c in candidate_name) and
                            '某' not in candidate_name and
                            candidate_name not in invalid_words and
                            not any(bad_word in candidate_name for bad_word in ['医院', '医生', '院长', '护士', '药师', '骗保', '虚开', '委员', '主任', '局长', '处长', '书记']) and
                            not any(vk in candidate_name for vk in violation_keywords)):
                            names.append(candidate_name)
                            break
                    break
            
            keyword_pos += 1
    
    # 方法3: 职位+姓名模式（改进后的正则表达式，避免匹配违法关键词）
    position_patterns = [
        # 职位+姓名的模式，使用更严格的姓名匹配
        r'(原|前)?(院长|主任|委员|科长|书记|局长|处长|副院长|副主任|副局长|副处长|主任委员)([王李张刘陈杨赵黄周吴徐孙胡朱高林何郭马罗梁宋郑谢韩唐冯于董萧程曹袁邓许傅沈曾彭吕苏卢蒋蔡贾丁魏薛叶阎余潘杜戴夏钟汪田任姜范方石姚谭廖邹熊金陆郝孔白崔康毛邱秦江史顾侯邵孟龙万段雷钱汤尹黎易常武乔贺赖龚文阚骆邢严孟吴][^被涉遭因于已正将曾等与及接收获取给送让向从在到对为是有无不未再还都也却但而则即就既]{1,3})(?=[被涉遭因于已正将曾等与及接收获取给送让向从在到对为是有无不未再还都也却但而则即就既]|$)',
        # 姓名+职位的模式
        r'([王李张刘陈杨赵黄周吴徐孙胡朱高林何郭马罗梁宋郑谢韩唐冯于董萧程曹袁邓许傅沈曾彭吕苏卢蒋蔡贾丁魏薛叶阎余潘杜戴夏钟汪田任姜范方石姚谭廖邹熊金陆郝孔白崔康毛邱秦江史顾侯邵孟龙万段雷钱汤尹黎易常武乔贺赖龚文阚骆邢严孟吴][^被涉遭因于已正将曾等与及接收获取给送让向从在到对为是有无不未再还都也却但而则即就既]{1,3})(院长|主任|委员|科长|书记|局长|处长)'
    ]
    
    for pattern in position_patterns:
        matches = re.findall(pattern, text)
        for match in matches:
            if isinstance(match, tuple):
                for item in match:
                    if (2 <= len(item) <= 4 and 
                        all('\u4e00' <= c <= '\u9fff' for c in item) and
                        item[0] in common_surnames and
                        item not in invalid_words and
                        # 确保不以违法关键词结尾
                        not any(item.endswith(suffix) for suffix in violation_suffix)):
                        names.append(item)
            else:
                if (2 <= len(match) <= 4 and 
                    all('\u4e00' <= c <= '\u9fff' for c in match) and
                    match[0] in common_surnames and
                    match not in invalid_words and
                    # 确保不以违法关键词结尾
                    not any(match.endswith(suffix) for suffix in violation_suffix)):
                    names.append(match)
    
    # 清理姓名：去掉可能误匹配的违法关键词后缀
    def clean_name(name):
        """清理姓名中的违法关键词后缀"""
        if not name:
            return name
        
        # 特殊处理：保留"某某"、"某某某"等匿名表示
        if "某" in name and len(name) <= 4:
            return name
        
        # 去掉末尾的违法关键词，但要保留合理长度
        original_length = len(name)
        while name and name[-1] in violation_suffix and len(name) > 2:
            name = name[:-1]
        
        # 如果清理后长度变化太大，说明可能过度清理了，返回原名
        if len(name) < original_length - 1 and original_length <= 4:
            return name + name[-1]  # 恢复最后一个字符
        
        return name if len(name) >= 2 else ""
    
    # 最终验证和优化选择
    valid_names = []
    for original_name in set(names):  # 去重
        # 先清理名字
        clean_name_result = clean_name(original_name)
        
        # 更严格的姓名验证
        if (len(clean_name_result) >= 2 and len(clean_name_result) <= 4 and  # 允许4字姓名
            all('\u4e00' <= c <= '\u9fff' for c in clean_name_result) and
            clean_name_result[0] in common_surnames and
            clean_name_result not in invalid_words and
            # 只检查最明确的错误模式
            not any(bad_word in clean_name_result for bad_word in invalid_words) and
            # 排除明显的非姓名结尾
            not clean_name_result.endswith(('果', '物', '代', '表', '查', '布', '明', '组', '团', '会', '被', '涉', '遭', '生', '医', '院', '长', '任', '主', '委', '局')) and
            # 排除明显不完整的姓名（过于简短或常见词汇）
            not is_incomplete_name(clean_name_result)):
            valid_names.append(clean_name_result)
    
    # 优化选择：如果有长名字和短名字，且短名字是长名字的前缀，则只保留长名字
    final_names = []
    valid_names_sorted = sorted(valid_names, key=len, reverse=True)  # 按长度降序
    
    for name in valid_names_sorted:
        # 检查这个名字是否是其他已选名字的前缀
        is_prefix = False
        for selected_name in final_names:
            if selected_name.startswith(name) and len(selected_name) > len(name):
                is_prefix = True
                break
        
        # 如果不是前缀，或者没有更长的名字包含它，则添加
        if not is_prefix:
            # 同时检查这个名字是否包含已有的更短名字
            to_remove = []
            for i, selected_name in enumerate(final_names):
                if name.startswith(selected_name) and len(name) > len(selected_name):
                    to_remove.append(i)
            
            # 移除被包含的短名字
            for i in reversed(to_remove):
                final_names.pop(i)
            
            final_names.append(name)
    
    return final_names

def extract_description(text, person_name=""):
    """改进的违法事件描述提取 - 更准确、更完整的描述"""
    # 分割句子，保留更多标点符号
    sentences = re.split(r'[。！？；\n]', text)
    relevant_sentences = []
    
    # 高权重关键词（更重要的违法行为）
    high_priority_keywords = [
        '被查', '被双开', '被开除', '被免职', '被处分', '涉嫌', '被逮捕', '被拘留', 
        '被判刑', '主动投案', '被诉', '被调查', '被立案', '被撤职', '严重违纪违法',
        '贪污', '腐败', '受贿', '行贿', '挪用', '滥用职权', '玩忽职守'
    ]
    
    # 医疗相关关键词（扩展为所有行业）
    industry_keywords = [
        '医院', '医生', '院长', '主任', '医疗', '药品', '回扣', '红包', '医保', '骗保',
        '局长', '处长', '科长', '委员', '董事', '经理', '总监', '主管',
        '企业', '公司', '集团', '单位', '机构', '组织'
    ]
    
    # 排除的无效描述模式（但不过度限制）
    invalid_description_patterns = [
        r'.*编辑.*',
        r'.*作者.*',
        r'.*来源.*',
        r'.*转载.*',
        r'.*关注.*',
        r'.*点击.*',
        r'.*扫码.*',
        r'.*健识局.*',
        r'.*赛柏蓝.*',
        r'^\d+$',  # 纯数字
        r'^[a-zA-Z]+$',  # 纯英文
    ]
    
    for sentence in sentences:
        sentence = sentence.strip()
        if len(sentence) < 20 or len(sentence) > 300:  # 调整长度范围，太短的不准确
            continue
            
        # 检查是否为无效描述
        is_invalid = False
        for pattern in invalid_description_patterns:
            if re.match(pattern, sentence):
                is_invalid = True
                break
        
        if is_invalid:
            continue
            
        # 检查是否包含关键词
        has_high_priority = any(kw in sentence for kw in high_priority_keywords)
        has_industry = any(kw in sentence for kw in industry_keywords)
        has_general_keyword = any(kw in sentence for kw in KEYWORDS)
        has_person = person_name and person_name in sentence if person_name else True
        
        # 计算关键词密度
        keyword_count = sum(1 for kw in KEYWORDS if kw in sentence)
        
        # 优先选择包含高权重关键词的句子
        if (has_high_priority or (has_industry and has_general_keyword)) and has_person and keyword_count >= 1:
            clean_sentence = re.sub(r'\s+', '', sentence)
            
            # 额外验证：确保描述包含实质内容
            if len(clean_sentence) >= 20 and not is_meaningless_description(clean_sentence):
                relevant_sentences.append({
                    'sentence': clean_sentence,
                    'priority': 3 if has_high_priority else 2 if has_industry else 1,
                    'keyword_count': keyword_count,
                    'length': len(clean_sentence)
                })
    
    if relevant_sentences:
        # 如果有指定人名，优先返回包含该人名的句子
        if person_name:
            person_sentences = [s for s in relevant_sentences if person_name in s['sentence']]
            if person_sentences:
                # 按优先级、关键词数量和长度排序
                best_sentence = max(person_sentences, key=lambda s: (s['priority'], s['keyword_count'], min(s['length'], 150)))
                return best_sentence['sentence']
        
        # 否则返回优先级最高、关键词最多的句子
        best_sentence = max(relevant_sentences, key=lambda s: (s['priority'], s['keyword_count'], min(s['length'], 150)))
        return best_sentence['sentence']
    
    # 如果没有找到合适的句子，尝试提取包含基本关键词的句子
    for sentence in sentences:
        sentence = sentence.strip()
        if (25 <= len(sentence) <= 200 and 
            any(kw in sentence for kw in KEYWORDS) and
            (not person_name or person_name in sentence)):
            clean_sentence = re.sub(r'\s+', '', sentence)
            
            # 验证不是无效描述
            is_invalid = False
            for pattern in invalid_description_patterns:
                if re.match(pattern, clean_sentence):
                    is_invalid = True
                    break
                    
            if not is_invalid and not is_meaningless_description(clean_sentence):
                return clean_sentence
    
    return ""

def is_meaningless_description(description):
    """检查是否为无意义的描述"""
    # 过短的描述
    if len(description) < 15:
        return True
    
    # 检查是否为常见的无意义模式
    meaningless_patterns = [
        r'^.*等.*$',  # 只有"等"字的
        r'^.*具体.*$',  # 只有"具体"的
        r'^.*相关.*$',  # 只有"相关"的
        r'^.*情况.*$',  # 只有"情况"的
        r'^.*问题.*$',  # 只有"问题"的
        r'^.*事件.*$',  # 只有"事件"的
        r'^.*案件.*$',  # 只有"案件"的
        r'^.*消息.*$',  # 只有"消息"的
        r'^.*通报.*$',  # 只有"通报"的
        r'^.*公布.*$',  # 只有"公布"的
    ]
    
    for pattern in meaningless_patterns:
        if re.match(pattern, description) and len(description) < 30:
            return True
    
    # 检查重复字符过多
    unique_chars = len(set(description))
    if unique_chars < len(description) * 0.3:  # 独特字符少于30%
        return True
        
    return False

def extract_position(text, person_name=""):
    """全面的职位提取函数 - 包含所有行业职位"""
    if not text:
        return ""
    
    # 扩展的职位层级（从高到低，包含所有行业）
    position_hierarchy = [
        # 最高级职位
        ['董事长', '总裁', '总经理', '首席执行官', 'CEO', '总监', '副总裁', '副总经理'],
        # 高级职位
        ['院长', '副院长', '党委书记', '纪委书记', '局长', '副局长', '厅长', '副厅长'],
        # 中高级职位
        ['处长', '副处长', '科长', '副科长', '主任', '副主任', '主任委员', '副主任委员'],
        # 企业高管
        ['董事', '副董事长', '董事会成员', '总监', '副总监', '经理', '副经理', '主管'],
        # 医疗职位
        ['主任医师', '副主任医师', '主治医师', '主管医师', '住院医师'],
        # 其他专业职位
        ['委员', '护士长', '医生', '药师', '院长助理', '护士', '护师', '工程师', '会计师'],
        # 基础职位
        ['助理', '秘书', '专员', '主办', '干事', '职员', '工作人员']
    ]
    
    # 只排除明显错误的职位（非常少）
    invalid_positions = [
        '办公室', '任办公室', '在办公室',  # 这些不是职位而是地点
    ]
    
    found_positions = []
    
    # 如果有指定人名，优先在人名附近查找职位
    if person_name:
        # 在人名前后查找职位
        person_contexts = []
        sentences = re.split(r'[。！？；\n，]', text)
        
        for sentence in sentences:
            if person_name in sentence:
                person_contexts.append(sentence)
        
        context_text = '。'.join(person_contexts)
        if context_text:
            text = context_text  # 使用包含人名的上下文
    
    # 扩展的职位正则表达式，包含更多职位类型
    extended_position_patterns = [
        r'(董事长|总裁|总经理|首席执行官|CEO|总监|副总裁|副总经理)',
        r'(董事|副董事长|董事会成员)',
        r'(院长|副院长|党委书记|纪委书记|局长|副局长|厅长|副厅长)',
        r'(处长|副处长|科长|副科长|主任|副主任|主任委员|副主任委员)',
        r'(经理|副经理|主管|总监|副总监)',
        r'(主任医师|副主任医师|主治医师|主管医师|住院医师)',
        r'(委员|护士长|医生|药师|院长助理|护士|护师|工程师|会计师)',
        r'(助理|秘书|专员|主办|干事|职员|工作人员)'
    ]
    
    all_matches = []
    for pattern in extended_position_patterns:
        matches = re.findall(pattern, text)
        all_matches.extend(matches)
    
    # 也使用原有的正则表达式
    original_matches = POSITION_RE.findall(text)
    all_matches.extend(original_matches)
    
    # 过滤掉无效职位（很少）
    valid_matches = []
    for match in all_matches:
        if match not in invalid_positions and len(match) > 1:
            valid_matches.append(match)
    
    if not valid_matches:
        return ""
    
    # 按层级优先级选择最高职位
    for level in position_hierarchy:
        for position in valid_matches:
            if position in level:
                found_positions.append(position)
        if found_positions:
            break
    
    if not found_positions:
        # 如果没有找到层级职位，返回最长的职位（通常更具体）
        return max(valid_matches, key=len)
    
    # 在同一层级中，选择最具体的职位
    if len(found_positions) == 1:
        return found_positions[0]
    
    # 如果有多个同级职位，优先选择更具体的
    priority_order = [
        '董事长', '总裁', '总经理', 'CEO', '党委书记', '纪委书记', 
        '院长', '副院长', '局长', '副局长', '厅长', '副厅长',
        '处长', '副处长', '科长', '副科长', '主任', '副主任',
        '董事', '总监', '副总监', '经理', '副经理', '主管',
        '主任医师', '副主任医师', '主治医师', '委员'
    ]
    
    for priority_pos in priority_order:
        if priority_pos in found_positions:
            return priority_pos
    
    return found_positions[0]

def extract_hospital(text):
    """全面的机构提取 - 包括政府机构、企业、医院等所有类型"""
    institutions = []
    
    # 第一优先级：政府机构和管委会
    government_patterns = [
        # 国家级机构（最高优先级）
        r'(国家药品监督管理局|国家药监局|国家市场监督管理总局|国家卫生健康委员会|国家卫健委|国家发改委)',
        # 各种委员会
        r'([\u4e00-\u9fa5]{2,8}[省市县区][\u4e00-\u9fa5]{2,15}委员会)',
        r'([\u4e00-\u9fa5]{2,8}[省市县区](?:卫生健康委员会|卫健委|发改委))',
        # 各种局
        r'([\u4e00-\u9fa5]{2,8}[省市县区][\u4e00-\u9fa5]{2,10}局)',
        r'([\u4e00-\u9fa5]{2,8}[省市县区](?:卫生局|医保局|药监局|食药监局|税务局|工商局|公安局|人社局|民政局|财政局|审计局|司法局|自然资源局|生态环境局|住建局|交通运输局|水利局|农业农村局|商务局|文旅局|应急管理局|市场监管局|统计局|医疗保障局|机关事务管理局))',
        # 各种厅
        r'([\u4e00-\u9fa5]{2,8}省[\u4e00-\u9fa5]{2,10}厅)',
        r'([\u4e00-\u9fa5]{2,8}省(?:卫生厅|发改厅|财政厅|教育厅|民政厅|人社厅|自然资源厅|生态环境厅|住建厅|交通运输厅|水利厅|农业农村厅|商务厅|文旅厅|应急管理厅|市场监管厅|统计厅|医疗保障厅))',
        # 各种部门
        r'([\u4e00-\u9fa5]{2,8}[省市县区][\u4e00-\u9fa5]{2,10}(?:部门|中心|办公室|管理局|监督局|执法局))',
    ]
    
    # 第二优先级：企业机构
    company_patterns = [
        # 各种公司
        r'([\u4e00-\u9fa5]{2,15}(?:有限公司|股份有限公司|有限责任公司))',
        r'([\u4e00-\u9fa5]{2,15}(?:集团|控股|投资|开发|建设|科技|医药|生物|化工|电子|机械|能源|环保|食品|贸易|咨询|服务)(?:有限公司|股份有限公司|有限责任公司)?)',
        r'([\u4e00-\u9fa5]{2,15}(?:集团公司|控股集团|投资集团|开发集团|建设集团))',
        # 简化公司名
        r'([\u4e00-\u9fa5]{3,15}公司)',
    ]
    
    # 第三优先级：医疗机构
    medical_patterns = [
        # 完整的省市县+医院名称
        r'([\u4e00-\u9fa5]{2,8}[省市县区][\u4e00-\u9fa5]{2,15}(?:人民医院|中心医院|中医院|第[一二三四五六七八九十\d]+医院|医院))',
        # 大学附属医院
        r'([\u4e00-\u9fa5]{2,8}(?:大学|医科大学|医学院)附属[\u4e00-\u9fa5]{0,10}医院)',
        # 专科医院
        r'([\u4e00-\u9fa5]{2,10}(?:妇幼保健院|儿童医院|肿瘤医院|传染病医院|精神病医院|康复医院|骨科医院|眼科医院|口腔医院|心血管病医院|脑科医院))',
        # 军队医院
        r'([\u4e00-\u9fa5]{2,8}(?:军区|部队)[\u4e00-\u9fa5]{0,8}医院)',
        # 一般医院
        r'([\u4e00-\u9fa5]{3,12}医院)',
    ]
    
    # 第四优先级：其他机构
    other_patterns = [
        # 学校和科研机构
        r'([\u4e00-\u9fa5]{2,12}(?:大学|学院|研究院|研究所|科学院))',
        # 社会组织
        r'([\u4e00-\u9fa5]{2,12}(?:协会|学会|基金会|联合会|商会))',
        # 事业单位
        r'([\u4e00-\u9fa5]{2,12}(?:中心|站|所|院|馆|台))',
    ]
    
    # 大幅简化排除规则 - 只排除明显错误的
    exclude_keywords = [
        # 明显错误的词汇
        '健识局', '赛柏蓝', '被查', '被处', '调查', '官方', '结果', '公布',
        # 违法相关词汇
        '违法', '违纪', '贪污', '腐败', '受贿', '双开', '免职', '撤职',
        # 明显不合理的名称词汇（只保留最明显的）
        '宇宙', '银河', '星际', '外星', '火星', '月球', '太空', '地球'
    ]
    
    # 搜索所有类型的机构
    all_patterns = government_patterns + company_patterns + medical_patterns + other_patterns
    
    for pattern in all_patterns:
        matches = re.findall(pattern, text)
        for institution in matches:
            if is_valid_institution(institution, exclude_keywords):
                institutions.append(institution)
    
    # 返回最佳匹配
    return select_best_institution(institutions)

def is_valid_institution(institution, exclude_keywords):
    """验证机构名称是否有效 - 适用于所有类型机构"""
    if not institution or len(institution) < 3:
        return False
    
    # 长度限制（放宽）
    if len(institution) > 40:
        return False
    
    # 排除关键词检查（大幅简化）
    if any(keyword in institution for keyword in exclude_keywords):
        return False
    
    # 不能以违法相关词汇开头
    invalid_starts = ['被', '涉', '查', '处', '违', '法', '纪', '开', '免', '撤']
    if any(institution.startswith(start) for start in invalid_starts):
        return False
    
    # 必须是合理的中文字符（放宽限制）
    valid_chars = set('第一二三四五六七八九十0123456789()（）·-')
    if not all('\u4e00' <= c <= '\u9fff' or c in valid_chars or c.isalnum() for c in institution):
        return False
    
    # 检查不合理的模式（只检查最明显的）
    invalid_patterns = [
        r'.*某某.*$',  # 包含"某某"的
        r'^[A-Za-z]+$',  # 纯英文
        r'^\d+$',  # 纯数字
    ]
    
    for pattern in invalid_patterns:
        if re.match(pattern, institution):
            return False
    
    return True

def select_best_institution(institutions):
    """从候选机构中选择最佳的"""
    if not institutions:
        return ""
    
    # 去重
    unique_institutions = list(set(institutions))
    
    if len(unique_institutions) == 1:
        return unique_institutions[0]
    
    # 优先级排序：政府机构 > 企业 > 医院 > 其他
    priority_keywords = [
        # 第一优先级：重要政府机构
        ['委员会', '省政府', '市政府', '县政府'],
        ['纪委', '监委', '组织部', '宣传部'],
        # 第二优先级：各种局厅
        ['卫生健康委员会', '卫健委', '发改委'],
        ['局', '厅', '部门', '管理局', '监督局'],
        # 第三优先级：企业
        ['集团公司', '有限公司', '股份有限公司', '有限责任公司'],
        ['集团', '控股', '投资', '开发', '建设'],
        ['公司'],
        # 第四优先级：医院
        ['人民医院', '中心医院', '中医院'],
        ['第一医院', '第二医院', '第三医院'],
        ['妇幼保健院', '儿童医院', '肿瘤医院'],
        ['附属医院', '医院'],
        # 第五优先级：其他机构
        ['大学', '学院', '研究院', '研究所'],
        ['协会', '学会', '基金会'],
        ['中心', '站', '所', '院', '馆', '台']
    ]
    
    # 按优先级选择
    for priority_group in priority_keywords:
        for institution in unique_institutions:
            if any(keyword in institution for keyword in priority_group):
                return institution
    
    # 如果没有优先级匹配，选择最长且最完整的
    return max(unique_institutions, key=lambda h: (len(h), h.count('省'), h.count('市'), h.count('县'), h.count('区')))

def enhance_hospital_name(hospital, province):
    """增强医院名称，为缺少市名的医院添加市名"""
    if not hospital or not province:
        return hospital
    
    # 如果已经包含市县区，直接返回
    if re.search(r'[省市县区]', hospital):
        return hospital
    
    city_mapping = {
        '甘肃': ['兰州', '天水', '白银', '金昌', '嘉峪关', '武威', '张掖', '平凉', '酒泉', '庆阳', '定西', '陇南', '临夏', '甘南'],
        '河南': ['郑州', '开封', '洛阳', '平顶山', '安阳', '鹤壁', '新乡', '焦作', '濮阳', '许昌', '漯河', '三门峡', '南阳', '商丘', '信阳', '周口', '驻马店'],
        '山西': ['太原', '大同', '阳泉', '长治', '晋城', '朔州', '晋中', '运城', '忻州', '临汾', '吕梁'],
        '湖南': ['长沙', '株洲', '湘潭', '衡阳', '邵阳', '岳阳', '常德', '张家界', '益阳', '郴州', '永州', '怀化', '娄底', '湘西'],
        '上海': ['上海'],
        '重庆': ['重庆'],
        '黑龙江': ['哈尔滨', '齐齐哈尔', '鸡西', '鹤岗', '双鸭山', '大庆', '伊春', '佳木斯', '七台河', '牡丹江', '黑河', '绥化'],
        '浙江': ['杭州', '宁波', '温州', '嘉兴', '湖州', '绍兴', '金华', '衢州', '舟山', '台州', '丽水'],
        '山东': ['济南', '青岛', '淄博', '枣庄', '东营', '烟台', '潍坊', '济宁', '泰安', '威海', '日照', '莱芜', '临沂', '德州', '聊城', '滨州', '菏泽'],
        '四川': ['成都', '自贡', '攀枝花', '泸州', '德阳', '绵阳', '广元', '遂宁', '内江', '乐山', '南充', '眉山', '宜宾', '广安', '达州', '雅安', '巴中', '资阳', '阿坝', '甘孜', '凉山'],
        '广东': ['广州', '韶关', '深圳', '珠海', '汕头', '佛山', '江门', '湛江', '茂名', '肇庆', '惠州', '梅州', '汕尾', '河源', '阳江', '清远', '东莞', '中山', '潮州', '揭阳', '云浮'],
        '江苏': ['南京', '无锡', '徐州', '常州', '苏州', '南通', '连云港', '淮安', '盐城', '扬州', '镇江', '泰州', '宿迁'],
        '安徽': ['合肥', '芜湖', '蚌埠', '淮南', '马鞍山', '淮北', '铜陵', '安庆', '黄山', '滁州', '阜阳', '宿州', '六安', '亳州', '池州', '宣城'],
        '福建': ['福州', '厦门', '莆田', '三明', '泉州', '漳州', '南平', '龙岩', '宁德'],
        '江西': ['南昌', '景德镇', '萍乡', '九江', '新余', '鹰潭', '赣州', '吉安', '宜春', '抚州', '上饶'],
        '湖北': ['武汉', '黄石', '十堰', '宜昌', '襄阳', '鄂州', '荆门', '孝感', '荆州', '黄冈', '咸宁', '随州', '恩施'],
        '河北': ['石家庄', '唐山', '秦皇岛', '邯郸', '邢台', '保定', '张家口', '承德', '沧州', '廊坊', '衡水'],
        '辽宁': ['沈阳', '大连', '鞍山', '抚顺', '本溪', '丹东', '锦州', '营口', '阜新', '辽阳', '盘锦', '铁岭', '朝阳', '葫芦岛'],
        '吉林': ['长春', '吉林', '四平', '辽源', '通化', '白山', '松原', '白城', '延边'],
        '内蒙古': ['呼和浩特', '包头', '乌海', '赤峰', '通辽', '鄂尔多斯', '呼伦贝尔', '巴彦淖尔', '乌兰察布', '兴安', '锡林郭勒', '阿拉善'],
        '广西': ['南宁', '柳州', '桂林', '梧州', '北海', '防城港', '钦州', '贵港', '玉林', '百色', '贺州', '河池', '来宾', '崇左'],
        '海南': ['海口', '三亚', '三沙', '儋州'],
        '贵州': ['贵阳', '六盘水', '遵义', '安顺', '毕节', '铜仁', '黔西南', '黔东南', '黔南'],
        '云南': ['昆明', '曲靖', '玉溪', '保山', '昭通', '丽江', '普洱', '临沧', '楚雄', '红河', '文山', '西双版纳', '大理', '德宏', '怒江', '迪庆'],
        '西藏': ['拉萨', '日喀则', '昌都', '林芝', '山南', '那曲', '阿里'],
        '陕西': ['西安', '铜川', '宝鸡', '咸阳', '渭南', '延安', '汉中', '榆林', '安康', '商洛'],
        '青海': ['西宁', '海东', '海北', '黄南', '海南', '果洛', '玉树', '海西'],
        '宁夏': ['银川', '石嘴山', '吴忠', '固原', '中卫'],
        '新疆': ['乌鲁木齐', '克拉玛依', '吐鲁番', '哈密', '昌吉', '博尔塔拉', '巴音郭楞', '阿克苏', '克孜勒苏', '喀什', '和田', '伊犁', '塔城', '阿勒泰'],
        '北京': ['北京'],
        '天津': ['天津']
    }
    
    cities = city_mapping.get(province, [])
    
    # 对于第X人民医院/医院，添加市名
    if re.search(r'第[一二三四五六七八九十\d]+人民医院|第[一二三四五六七八九十\d]+医院|中心医院|人民医院', hospital):
        if any(city in hospital for city in cities):
            return hospital
        
        if cities:
            return f"{cities[0]}{hospital}"
        else:
            return hospital
    
    # 对于其他医院名称，也尝试添加市名
    if cities and not any(city in hospital for city in cities):
        return f"{cities[0]}{hospital}"
    
    return hospital

def extract_multiple_violations(text):
    """提取一篇文章中的多个违法事件 - 避免重复并提高准确性"""
    results = []
    
    # 提取所有可能的人名
    names = extract_name(text)
    
    # 用于去重的集合
    seen_combinations = set()
    
    if not names:
        # 如果没有找到人名，但包含关键词，仍然记录
        if any(kw in text for kw in KEYWORDS):
            province = extract_first(PROVINCE_RE, text)
            hospital = extract_hospital(text)
            desc = extract_description(text)
            date = extract_first(DATE_RE, text)
            position = extract_position(text)
            
            if desc:
                enhanced_hospital = enhance_hospital_name(hospital, province)
                # 创建唯一标识符用于去重
                unique_key = f"{province}_{enhanced_hospital}_{position}_{desc[:50]}"
                if unique_key not in seen_combinations:
                    seen_combinations.add(unique_key)
                results.append({
                    "姓名": "",
                    "省份": province,
                    "医院": enhanced_hospital if enhanced_hospital else "",
                    "职位": position,
                    "描述": desc,
                    "日期": date
                })
    else:
        # 为每个人名提取信息
        for name in names:
            context_sentences = []
            sentences = re.split(r'[。！？；\n]', text)
            
            for i, sentence in enumerate(sentences):
                if name in sentence:
                    start = max(0, i-1)
                    end = min(len(sentences), i+2)
                    context_sentences.extend(sentences[start:end])
            
            context = '。'.join(context_sentences)
            
            province = extract_first(PROVINCE_RE, context) or extract_first(PROVINCE_RE, text)
            hospital = extract_hospital(context) or extract_hospital(text)
            desc = extract_description(context, name)
            date = extract_first(DATE_RE, context) or extract_first(DATE_RE, text)
            position = extract_position(context, name)
            
            enhanced_hospital = enhance_hospital_name(hospital, province)
            
            # 创建唯一标识符用于去重
            unique_key = f"{name}_{province}_{enhanced_hospital}_{position}_{desc[:50]}"
            if unique_key not in seen_combinations:
                seen_combinations.add(unique_key)
            results.append({
                "姓名": name,
                "省份": province,
                "医院": enhanced_hospital if enhanced_hospital else "",
                "职位": position,
                "描述": desc,
                "日期": date
            })
    
    return results

def filter_and_extract(articles, gzh_name, existing_results=None, max_workers=None):
    """优化版本 - 使用多线程并发处理文章"""
    if max_workers is None:
        max_workers = PERFORMANCE_CONFIG['max_workers']
    
    print(f"本次共获取{len(articles)}篇文章，开始并发筛选处理（{max_workers}线程）...")
    results = existing_results or []
    
    # 用于全局去重的集合
    global_seen_combinations = set()
    if existing_results:
        for result in existing_results:
            description = result.get('Description', '')
            if pd.isna(description):
                description = ''
            elif isinstance(description, str):
                description = description[:50]
            else:
                description = str(description)[:50]
            
            unique_key = f"{result.get('姓名', '')}_{result.get('省份', '')}_{result.get('医院', '')}_{result.get('职位', '')}_{description}"
            global_seen_combinations.add(unique_key)
    
    # 准备并发处理的数据
    article_tasks = []
    for article in articles:
        try:
            info = json.loads(article.get("publish_info", "{}"))
            if info.get("appmsgex"):
                title = info["appmsgex"][0].get("title", "")
                link = info["appmsgex"][0].get("link", "")
                create_time = info["appmsgex"][0].get("create_time")
                if isinstance(create_time, int):
                    create_time = time.strftime("%Y/%m/%d", time.localtime(create_time))
                
                # 提前进行标题筛选
                if quick_title_filter(title):
                    article_tasks.append((
                        {"title": title, "link": link}, 
                        gzh_name, 
                        create_time
                    ))
        except Exception:
            continue
    
    print(f"标题预筛选后剩余 {len(article_tasks)} 篇文章需要处理")
    
    # 并发处理文章内容获取和筛选
    matched_articles = []
    processed_count = 0
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 提交所有任务
        future_to_article = {
            executor.submit(process_single_article, task): task 
            for task in article_tasks
        }
        
        # 收集结果
        for future in as_completed(future_to_article):
            processed_count += 1
            if processed_count % 10 == 0:
                print(f"已处理 {processed_count}/{len(article_tasks)} 篇文章")
            
            try:
                result = future.result()
                if result:  # 如果文章通过了筛选
                    matched_articles.append(result)
            except Exception as e:
                continue
    
    print(f"内容筛选后匹配到 {len(matched_articles)} 篇相关文章")
    
    # 串行处理信息提取（避免并发时的数据竞争）
    for i, article_info in enumerate(matched_articles):
        try:
            title = article_info['title']
            link = article_info['link']
            content = article_info['content']
            create_time = article_info['create_time']
            
            # 调试信息（只显示前3篇）
            if i < 3:
                print(f"\n匹配文章 {i+1}: {title}")
                print(f"文章内容预览: {content[:200]}...")
            
            # 提取多个违法事件
            violations = extract_multiple_violations(content)
            
            if i < 3:
                print(f"提取到 {len(violations)} 个违法事件:")
                for j, v in enumerate(violations):
                    print(f"  事件{j+1}: 姓名={v['姓名']}, 机构={v['医院']}, 职位={v['职位']}")
                    print(f"           描述={v['描述'][:100]}...")
            
            for violation in violations:
                # 使用公众号发布时间作为优先时间，并标准化为xxxx/xx/xx格式
                publish_time = create_time or violation["日期"]
                violation["发布时间"] = normalize_date_format(publish_time)
                violation["Resource"] = gzh_name
                violation["文章链接"] = link
                violation["标题"] = title
                
                # 创建全局唯一标识符用于去重
                description = violation['描述']
                if isinstance(description, str):
                    description_slice = description[:50]
                else:
                    description_slice = str(description)[:50]
                
                global_unique_key = f"{violation['姓名']}_{violation['省份']}_{violation['医院']}_{violation['职位']}_{description_slice}"
                
                # 只有在没有见过这个组合时才添加记录
                if global_unique_key not in global_seen_combinations:
                    global_seen_combinations.add(global_unique_key)
                    
                    # 字段顺序：发布时间放在第一列，格式为xxxx/xx/xx
                    result = {
                        "发布时间": violation["发布时间"],
                        "省份": violation["省份"],
                        "机构": violation["医院"],  # 改为更通用的"机构"
                        "职位": violation["职位"],
                        "姓名": violation["姓名"],
                        "Resource": violation["Resource"],
                        "Description": violation["描述"],
                        "文章链接": violation["文章链接"],
                        "标题": violation["标题"]
                    }
                    results.append(result)
                else:
                    if i < 3:  # 只在前几篇显示调试信息
                        print(f"跳过重复记录: {violation['姓名']} - {violation['医院']}")
            
        except Exception as e:
            print(f"[{gzh_name}] 文章信息提取失败: {e}")
    
    print(f"总共处理了 {len(article_tasks)} 篇文章，匹配到 {len(matched_articles)} 篇相关文章，提取到 {len(results) - len(existing_results if existing_results else [])} 条新记录")
    return results

def get_fakeid(gzh_name, token, cookie):
    """通过公众号名称自动获取fakeid"""
    search_url = 'https://mp.weixin.qq.com/cgi-bin/searchbiz'
    params = {
        'action': 'search_biz',
        'token': token,
        'lang': 'zh_CN',
        'f': 'json',
        'ajax': '1',
        'random': random.random(),
        'query': gzh_name,
        'begin': 0,
        'count': 5
    }
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
        'Cookie': cookie
    }
    try:
        resp = requests.get(search_url, params=params, headers=headers, timeout=15)
        data = resp.json()
        if data.get('list'):
            return data['list'][0]['fakeid']
        else:
            print(f"未找到公众号 {gzh_name} 的fakeid，请检查名称、token、cookie")
            return None
    except Exception as e:
        print(f"获取fakeid失败: {e}")
        return None

def save_progress(current_page, gzh_name, all_results):
    """保存爬取进度和数据"""
    os.makedirs('微信公众号文章', exist_ok=True)
    
    # 保存进度信息
    progress_info = {
        'last_page': current_page,
        'gzh_name': gzh_name,
        'total_records': len(all_results),
        'last_update': time.strftime("%Y-%m-%d %H:%M:%S")
    }
    
    progress_file = '微信公众号文章/健识局_爬取进度.json'
    with open(progress_file, 'w', encoding='utf-8') as f:
        json.dump(progress_info, f, ensure_ascii=False, indent=2)
    
    # 保存数据到统一Excel文件
    if all_results:
        df = pd.DataFrame(all_results)
        output_file = '微信公众号文章/健识局违法事件.xlsx'
        df.to_excel(output_file, index=False)
        print(f"已保存 {len(all_results)} 条记录到 {output_file}")
        print(f"进度已保存：当前页面{current_page}")

def load_progress():
    """加载爬取进度"""
    progress_file = '微信公众号文章/健识局_爬取进度.json'
    data_file = '微信公众号文章/健识局违法事件.xlsx'
    
    start_page = 0
    existing_results = []
    
    # 加载进度信息
    if os.path.exists(progress_file):
        try:
            with open(progress_file, 'r', encoding='utf-8') as f:
                progress_info = json.load(f)
            start_page = progress_info.get('last_page', 0)
            print(f"发现进度文件：上次爬取到第{start_page}页")
            print(f"上次更新时间：{progress_info.get('last_update', '未知')}")
        except Exception as e:
            print(f"读取进度文件失败: {e}")
    
    # 加载已有数据
    if os.path.exists(data_file):
        try:
            existing_df = pd.read_excel(data_file)
            existing_results = existing_df.to_dict('records')
            print(f"发现已有数据文件，包含 {len(existing_results)} 条记录")
        except Exception as e:
            print(f"读取数据文件失败: {e}")
    
    return start_page, existing_results

def main():
    # 加载已有进度和数据
    start_page, all_results = load_progress()
    
    if start_page > 0 or all_results:
        print(f"\n是否从上次中断处继续？")
        print(f"- 上次爬取到第{start_page}页")
        print(f"- 已有{len(all_results)}条记录")
        response = input("继续上次进度(y) 还是重新开始(n)？[y/n]: ").lower()
        
        if response != 'y':
            start_page = 0
            all_results = []
            print("将重新开始爬取")
        else:
            print(f"将从第{start_page + 1}页开始继续爬取")
    
    for gzh in GZH_LIST:
        print(f"\n开始抓取公众号: {gzh['name']}")
        fakeid = get_fakeid(gzh['name'], gzh['token'], gzh['cookie'])
        if not fakeid:
            continue
        gzh['fakeid'] = fakeid
        
        # 批量处理：每30页抓取一次，然后筛选（提升批次大小）
        current_page = start_page
        max_pages = 1000  # 最大页数
        batch_size = 30   # 每批次30页（原来20页 -> 30页，减少频繁保存）
        
        try:
            while current_page < max_pages:
                print(f"\n=== 开始处理第{current_page//batch_size + 1}批次 (第{current_page+1}-{min(current_page+batch_size, max_pages)}页) ===")
                
                # 抓取当前批次的文章
                batch_articles, success = fetch_articles_batch(gzh, current_page, batch_size, page_size=10)
                
                if not success or not batch_articles:
                    print(f"第{current_page//batch_size + 1}批次抓取失败或无文章，停止爬取")
                    break
                
                print(f"第{current_page//batch_size + 1}批次共获取 {len(batch_articles)} 篇文章，开始筛选...")
                
                # 立即筛选和提取当前批次的文章
                batch_results = filter_and_extract(batch_articles, gzh['name'], all_results)
                all_results = batch_results  # 更新结果列表
                
                print(f"第{current_page//batch_size + 1}批次处理完成，累计获得 {len(all_results)} 条记录")
                
                # 移动到下一批次
                current_page += batch_size
                
                # 每批次都保存进度（避免数据丢失）
                save_progress(current_page, gzh['name'], all_results)
                
                # 批次间休息，避免请求过于频繁
                if current_page < max_pages:
                    print(f"批次间休息3秒...")
                    time.sleep(3)
                    
        except KeyboardInterrupt:
            print(f"\n\n用户中断爬取，正在保存当前进度...")
            save_progress(current_page, gzh['name'], all_results)
            print(f"进度已保存，下次可从第{current_page + 1}页继续")
            return
        except Exception as e:
            print(f"\n爬取过程中出现错误: {e}")
            save_progress(current_page, gzh['name'], all_results)
            print(f"进度已保存，下次可从第{current_page + 1}页继续")
            return
    
    # 最终保存和统计
    if all_results:
        save_progress(current_page, gzh['name'], all_results)
        
        df = pd.DataFrame(all_results)
        print(f"\n=== 爬取完成！数据统计 ===")
        print(f"总记录数: {len(df)}")
        print(f"有机构信息的记录: {len(df[df['机构'].notna() & (df['机构'] != '')])}")
        print(f"有省份信息的记录: {len(df[df['省份'].notna() & (df['省份'] != '')])}")
        print(f"有人名信息的记录: {len(df[df['姓名'].notna() & (df['姓名'] != '')])}")
        
        # 显示前几条记录
        print(f"\n=== 前5条记录预览 ===")
        for i, row in df.head().iterrows():
            print(f"记录 {i+1}:")
            print(f"  发布时间: {row['发布时间']}")
            print(f"  标题: {row['标题']}")
            print(f"  姓名: {row['姓名']}")
            print(f"  省份: {row['省份']}")
            print(f"  机构: {row['机构']}")
            print(f"  职位: {row['职位']}")
            print(f"  描述: {row['Description'][:100]}...")
            print()
    else:
        print("无有效数据")

if __name__ == "__main__":
    main()