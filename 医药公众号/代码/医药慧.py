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

# 关键词 - 扩大范围以获取更多相关文章
KEYWORDS = [
    "贪污", "腐败", "受贿", "处罚", "双开", "调查", "起诉", "纪检", "廉洁", "投案", "被查", "违规", "违纪", "处分", "免职", "撤职",
    "医院", "医生", "院长", "主任", "医疗", "药品", "回扣", "红包", "贿赂", "违法", "犯罪", "立案", "逮捕", "拘留", "判刑",
    "医保", "医保基金", "骗保", "套保", "虚开", "发票", "税务", "逃税", "漏税", "偷税", "补税", "罚款", "没收", "追缴",
    "主动投案", "被查", "被诉", "被逮捕", "被拘留", "被判刑", "被开除", "被免职", "被处分", "被双开", "被调查", "被立案",
    "副院长", "副主任", "委员", "科长", "副科长", "护士长", "药师", "院长助理", "主任医师", "副主任医师", "主治医师", "护士", "护师",
    "医疗腐败", "医药腐败", "医疗回扣", "药品回扣", "医疗贿赂", "药品贿赂", "医疗违法", "药品违法", "医疗犯罪", "药品犯罪"
]

# 医药慧公众号配置
GZH_LIST = [
    {
        "name": "医药慧",
        "token": "1022341612",  # 更新token
        "fakeid": "MjM5MzU5MzUwNQ%3D%3D",  # 从URL中提取的fakeid
        "cookie": "RK=DC2Uq4Wf9P; ptcz=c9f4dcf0c0fb279d2316b228ce1d2d7a6b107f591ae8bbce0eac0ce98bc9de36; wxuin=51340895845860; mm_lang=zh_CN; _hp2_id.1405110977=%7B%22userId%22%3A%226801447023479475%22%2C%22pageviewId%22%3A%228306667787246811%22%2C%22sessionId%22%3A%224504468753015668%22%2C%22identity%22%3Anull%2C%22trackerVersion%22%3A%224.0%22%7D; ua_id=mxGDXOVuOo8d0D2hAAAAACdqUxp53FqemlDjGf2eSLM=; rewardsn=; wxtokenkey=777; poc_sid=HBg3iGijlmGc_2ocHEPN26JgrEcR59UETkMwwy7P; _clck=3911425635|1|fy6|0; uuid=8e983b004f998c2f2486628daa965d23; rand_info=CAESIMFXa7pbxrMmqQVBmDy8My3x6V5q80/zyyDTpZ2tsrja; slave_bizuin=3911425635; data_bizuin=3911425635; bizuin=3911425635; data_ticket=sbBLX2/f8X437MsBZwOn/8Td8c5XU5k77liknjRNwChFMBeuW9H5ZZl2mgaAshvO; slave_sid=WXVGQ3FBNG0zTlpDTGlUSGJQTnpIYlhpeDU3YllubzZRckwxSXdwZ05oSkxIQ0hxdmk1QVY2UHVtRXBwUmJkN2VGUHVVeF9RazBiY05Fa3FQYWlodU9xcnFhRmtBdFA2T2ptbk9BMEo4NFNOWHFzamtXWERldExQZm1CRzllZVBVSUtJNUVTYW5nR0FOeFJw; slave_user=gh_b3cdf815ccbf; xid=24ae585f4840c2e3c147341722f8af33; _clsk=1y2cy24|1754273381311|4|1|mp.weixin.qq.com/weheat-agent/payload/record"
    },
]

# 创建全局会话对象，复用连接
SESSION = requests.Session()
SESSION.headers.update({
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36'
})

# 性能配置 - 提高多线程性能
PERFORMANCE_CONFIG = {
    'max_workers': 8,  # 增加并发线程数
    'timeout': 15,     # 增加请求超时时间
    'page_delay': 0.3, # 减少页面间延迟
    'cache_size': 256  # 增加LRU缓存大小
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

def fetch_articles_batch(gzh, start_page=0, batch_size=20, page_size=10):
    """批量抓取公众号文章列表 - 优化版本使用会话复用"""
    batch_articles = []
    consecutive_empty_pages = 0  # 连续空页计数
    max_consecutive_empty = 3    # 最大连续空页数
    
    end_page = start_page + batch_size
    print(f"开始抓取第{start_page+1}页到第{end_page}页...")
    
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
            'referer': f'https://mp.weixin.qq.com/cgi-bin/appmsg?t=media/appmsg_edit_v2&action=edit&isNew=1&type=77&createType=0&token={gzh["token"]}&lang=zh_CN&timestamp={int(time.time()*1000)}',
            'accept': '*/*',
            'accept-language': 'zh-CN,zh;q=0.9',
            'x-requested-with': 'XMLHttpRequest',
            'sec-ch-ua': '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin'
        }
        try:
            # 使用全局会话复用连接
            resp = SESSION.get("https://mp.weixin.qq.com/cgi-bin/appmsgpublish", 
                              params=params, headers=headers, timeout=PERFORMANCE_CONFIG['timeout'])
            print(f"第{page+1}页请求状态码: {resp.status_code}")
            if resp.status_code != 200:
                print(f"请求失败，状态码: {resp.status_code}")
                return batch_articles, False  # 返回False表示出错
            result = resp.json()
            if result.get("base_resp", {}).get("ret") != 0:
                print(f"API返回错误: {result.get('base_resp', {})}")
                return batch_articles, False
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
            batch_articles.extend(articles)
            
            # 页面间延迟
            time.sleep(PERFORMANCE_CONFIG['page_delay'])
                
        except Exception as e:
            print(f"{gzh['name']} 第{page+1}页抓取失败: {e}")
            consecutive_empty_pages += 1
            if consecutive_empty_pages >= max_consecutive_empty:
                print(f"连续{max_consecutive_empty}次失败，停止本批次")
                break
            time.sleep(3)
            continue
    
    success = len(batch_articles) > 0
    print(f"批次获取完成，共获取到 {len(batch_articles)} 篇文章")
    return batch_articles, success

def get_article_content(link, gzh_name, session=None):
    """获取文章正文 - 特别处理蓝色字体中的机构/职位信息"""
    if session is None:
        session = SESSION
        
    try:
        headers = {
            'referer': 'https://mp.weixin.qq.com/',
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8'
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
            
            # 先尝试提取蓝色字体信息
            enhanced_text = extract_blue_text_info(content_div)
            if enhanced_text:
                return enhanced_text
            
            # 如果没有蓝色字体，返回常规文本
            text = content_div.get_text(separator='\n', strip=True)
            return text
        else:
            # 如果找不到特定区域，尝试获取整个body
            body = soup.find('body')
            if body:
                enhanced_text = extract_blue_text_info(body)
                if enhanced_text:
                    return enhanced_text
                text = body.get_text(separator='\n', strip=True)
                return text
            return ''
            
    except Exception as e:
        # 减少错误输出，避免影响性能
        return ''

def extract_blue_text_info(soup_element):
    """专门提取蓝色字体中的机构/职位信息"""
    try:
        enhanced_content = []
        base_text = soup_element.get_text(separator='\n', strip=True)
        
        # 查找蓝色字体元素（多种可能的CSS表示方式）
        blue_elements = []
        
        # 查找所有可能的蓝色字体标签
        for element in soup_element.find_all():
            style = element.get('style', '')
            class_name = ' '.join(element.get('class', []))
            
            # 检查是否是蓝色字体
            is_blue = (
                'color:blue' in style.replace(' ', '') or
                'color:#' in style and ('blue' in style or '0000ff' in style.lower() or 
                                       '0066cc' in style.lower() or '003399' in style.lower()) or
                'blue' in class_name.lower() or
                'highlight' in class_name.lower() or
                element.name in ['strong', 'b'] and any(keyword in element.get_text() for keyword in 
                    ['医院', '委员会', '局', '厅', '科', '处', '部', '中心', '公司', '集团'])
            )
            
            if is_blue:
                blue_text = element.get_text(strip=True)
                if blue_text and len(blue_text) > 2:
                    blue_elements.append({
                        'text': blue_text,
                        'element': element
                    })
        
        # 如果找到蓝色字体，构建增强的文本
        if blue_elements:
            enhanced_content.append("=== 蓝色字体信息提取 ===")
            for blue_item in blue_elements:
                blue_text = blue_item['text']
                element = blue_item['element']
                
                # 获取蓝色字体后面的文本（可能包含人名）
                next_text = ""
                try:
                    # 查找紧跟在蓝色字体后面的文本
                    next_sibling = element.next_sibling
                    if next_sibling:
                        if hasattr(next_sibling, 'get_text'):
                            next_text = next_sibling.get_text(strip=True)
                        else:
                            next_text = str(next_sibling).strip()
                    
                    # 如果没有直接兄弟节点，查找父级的下一个文本
                    if not next_text and element.parent:
                        parent_next = element.parent.next_sibling
                        if parent_next:
                            if hasattr(parent_next, 'get_text'):
                                next_text = parent_next.get_text(strip=True)
                            else:
                                next_text = str(parent_next).strip()
                except:
                    pass
                
                # 构建增强信息
                enhanced_line = f"【机构/职位】{blue_text}"
                if next_text and len(next_text) <= 10:  # 可能是人名
                    enhanced_line += f" 【人名】{next_text}"
                enhanced_content.append(enhanced_line)
            
            enhanced_content.append("=== 原文内容 ===")
            enhanced_content.append(base_text)
            return '\n'.join(enhanced_content)
        
        return None  # 没有找到蓝色字体
        
    except Exception as e:
        return None

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

def is_valid_chinese_name(name):
    """检查是否为有效的中文姓名 - 更严格的验证"""
    if not name or len(name) < 2 or len(name) > 4:
        return False
    
    # 必须全部是中文字符
    if not all('\u4e00' <= c <= '\u9fff' for c in name):
        return False
    
    # 扩展的常见姓氏列表
    common_surnames = [
        '王', '李', '张', '刘', '陈', '杨', '赵', '黄', '周', '吴', '徐', '孙', '胡', '朱', 
        '高', '林', '何', '郭', '马', '罗', '梁', '宋', '郑', '谢', '韩', '唐', '冯', '于', 
        '董', '萧', '程', '曹', '袁', '邓', '许', '傅', '沈', '曾', '彭', '吕', '苏', '卢', 
        '蒋', '蔡', '贾', '丁', '魏', '薛', '叶', '阎', '余', '潘', '杜', '戴', '夏', '钟', 
        '汪', '田', '任', '姜', '范', '方', '石', '姚', '谭', '廖', '邹', '熊', '金', '陆', 
        '郝', '孔', '白', '崔', '康', '毛', '邱', '秦', '江', '史', '顾', '侯', '邵', '孟', 
        '龙', '万', '段', '雷', '钱', '汤', '尹', '黎', '易', '常', '武', '乔', '贺', '赖', 
        '龚', '文', '阚', '骆', '邢', '严', '孟', '吴', '欧阳', '司马', '上官', '诸葛', 
        '东方', '南宫', '西门', '北冥', '令狐', '皇甫', '尉迟', '公孙', '慕容', '长孙'
    ]
    
    # 必须以常见姓氏开头
    if not any(name.startswith(surname) for surname in common_surnames):
        return False
    
    # 大幅扩展的无效词汇列表 - 包含用户提到的错误名字
    invalid_words = {
        # 基础无效词
        '医药慧', '赛柏蓝', '医院', '诊所', '卫生院', '卫生所', '委员会', '党委会', '董事会',
        
        # 职位相关词汇
        '主任医师', '副主任医师', '主治医师', '住院医师', '护师', '护士', '药师', 
        '董事长', '副总裁', '总经理', '副总经理', '总监', '副总监', '经理', '副经理',
        '院长', '副院长', '主任', '副主任', '科长', '副科长', '处长', '副处长',
        '局长', '副局长', '厅长', '副厅长', '书记', '副书记', '委员', '常委',
        
        # 用户特别提到的错误名字
        '严肃', '万元', '常委', '省委', '市委', '县委', '区委', '党委', '纪委',
        
        # 时间相关词汇
        '昨日', '今日', '近日', '日前', '目前', '现在', '当前', '此前', '此后', 
        '之前', '之后', '以来', '上午', '下午', '晚上', '深夜', '凌晨', '早上', 
        '中午', '傍晚', '夜间', '白天', '夜晚', '月底', '月初', '月中', '年初', 
        '年中', '年底', '季度', '半年', '全年', '当年', '去年', '明年',
        
        # 数字和金额相关
        '万元', '千元', '百元', '亿元', '十万', '百万', '千万', '一万', '两万', 
        '三万', '四万', '五万', '六万', '七万', '八万', '九万', '十万',
        
        # 违法相关词汇
        '违法', '违规', '违纪', '犯罪', '腐败', '受贿', '贪污', '挪用', '侵占', 
        '滥用', '双开', '免职', '撤职', '开除', '处分', '调查', '审查', '立案',
        '起诉', '判刑', '拘留', '逮捕', '监禁', '羁押', '取保', '缓刑',
        
        # 行政区划相关
        '省份', '市区', '县区', '街道', '社区', '村委', '居委', '乡镇', '开发区',
        
        # 媒体和发布相关
        '内容', '来源', '消息', '通报', '公告', '声明', '通知', '公示', '发布', 
        '报道', '新闻', '媒体', '记者', '编辑', '作者', '转载', '关注', '点击', 
        '扫码', '订阅', '分享', '评论', '点赞', '收藏',
        
        # 机构类型词汇
        '部门', '单位', '机构', '组织', '团体', '协会', '学会', '联盟', '集团', 
        '公司', '企业', '工厂', '车间', '班组', '小组', '团队', '队伍',
        
        # 明确的错误识别词汇（从实际数据中观察到的）
        '方结果', '康方生物', '生物医', '于文明', '李健康', '赵德华', 
        '方生物', '物医代', '代表团', '调查组', '官方结', '结果公', '公布关', 
        '关于医', '医学专', '专家被', '被查经', '经调取', '取重医', '医生市', 
        '市卫生', '生健医', '医长北', '北京这', '这两医', '医生贵', '贵州省', 
        '省人方', '人方面', '方面的', '面的问', '问题的', '题的处', '处理情',
        
        # 常见的错误组合
        '被查', '被抓', '被捕', '被诉', '被告', '被审', '被判', '被罚', '被处',
        '涉嫌', '涉案', '涉及', '涉及到', '因为', '由于', '根据', '按照',
        '接受', '收受', '获得', '取得', '得到', '拿到', '收到', '给予',
        
        # 其他容易误识别的词汇
        '一些', '一定', '一直', '一般', '一旦', '一方', '一样', '一起', '一切',
        '对于', '对方', '对此', '对待', '关于', '关系', '关键', '关注', '关心',
        '就是', '就在', '就有', '就能', '就会', '就要', '就可', '就此', '就此',
        '可以', '可能', '可是', '可见', '可谓', '可惜', '可行', '可信', '可疑',
        '还有', '还是', '还在', '还会', '还能', '还要', '还可', '还将', '还得',
        '都是', '都有', '都在', '都会', '都能', '都要', '都可', '都将', '都得',
        
        # 特殊字符组合
        '不是', '不在', '不会', '不能', '不要', '不可', '不得', '不如', '不但',
        '没有', '没在', '没会', '没能', '没要', '没可', '没得', '没法', '没事'
    }
    
    # 检查是否在无效词汇列表中
    if name in invalid_words:
        return False
    
    # 检查是否包含无效词汇的一部分
    for invalid_word in invalid_words:
        if len(invalid_word) >= 2 and invalid_word in name:
            return False
    
    # 检查不合理的重复字符
    if len(set(name)) == 1:  # 全部是相同字符
        return False
    
    # 检查常见的错误模式
    error_patterns = [
        r'.*被.*',  # 包含"被"的
        r'.*涉.*',  # 包含"涉"的
        r'.*查.*',  # 包含"查"的
        r'.*处.*',  # 包含"处"的
        r'.*调.*',  # 包含"调"的
        r'.*审.*',  # 包含"审"的
        r'.*案.*',  # 包含"案"的
        r'.*罪.*',  # 包含"罪"的
        r'.*法.*',  # 包含"法"的
        r'.*纪.*',  # 包含"纪"的
        r'.*元$',   # 以"元"结尾的
        r'.*委$',   # 以"委"结尾的
        r'.*院$',   # 以"院"结尾的
        r'.*局$',   # 以"局"结尾的
        r'.*厅$',   # 以"厅"结尾的
        r'.*部$',   # 以"部"结尾的
        r'.*会$',   # 以"会"结尾的
    ]
    
    for pattern in error_patterns:
        if re.match(pattern, name):
            return False
    
    return True

def extract_name(text):
    """改进的姓名提取逻辑 - 更准确的识别"""
    names = []
    
    # 优先处理蓝色字体标记中的人名
    blue_text_names = re.findall(r'【人名】([^【】\n]+)', text)
    if blue_text_names:
        for name in blue_text_names:
            name = name.strip()
            if is_valid_chinese_name(name):
                names.append(name)
        if names:
            return names  # 返回所有有效姓名
    
    # 扩展的常见姓氏
    common_surnames = [
        '王', '李', '张', '刘', '陈', '杨', '赵', '黄', '周', '吴', '徐', '孙', '胡', '朱', 
        '高', '林', '何', '郭', '马', '罗', '梁', '宋', '郑', '谢', '韩', '唐', '冯', '于', 
        '董', '萧', '程', '曹', '袁', '邓', '许', '傅', '沈', '曾', '彭', '吕', '苏', '卢', 
        '蒋', '蔡', '贾', '丁', '魏', '薛', '叶', '阎', '余', '潘', '杜', '戴', '夏', '钟', 
        '汪', '田', '任', '姜', '范', '方', '石', '姚', '谭', '廖', '邹', '熊', '金', '陆', 
        '郝', '孔', '白', '崔', '康', '毛', '邱', '秦', '江', '史', '顾', '侯', '邵', '孟', 
        '龙', '万', '段', '雷', '钱', '汤', '尹', '黎', '易', '常', '武', '乔', '贺', '赖', 
        '龚', '文', '阚', '骆', '邢', '严'
    ]
    
    # 方法1: 查找"X某"、"X某某"等匿名模式
    for surname in common_surnames:
        anonymous_patterns = [
            f"{surname}某某某",
            f"{surname}某某",
            f"{surname}某"
        ]
        for pattern in anonymous_patterns:
            if pattern in text:
                names.append(pattern)
                break
    
    # 方法2: 使用更精确的正则表达式查找完整姓名
    # 职位 + 姓名模式
    position_name_patterns = [
        r'(院长|副院长|主任|副主任|科长|副科长|处长|副处长|局长|副局长|厅长|副厅长|书记|副书记|委员|主席|副主席|董事长|副董事长|总经理|副总经理|总裁|副总裁|总监|副总监|经理|副经理|主管|主任医师|副主任医师|主治医师|主管医师|住院医师|护士长|药师|护师|医生|护士)([王李张刘陈杨赵黄周吴徐孙胡朱高林何郭马罗梁宋郑谢韩唐冯于董萧程曹袁邓许傅沈曾彭吕苏卢蒋蔡贾丁魏薛叶阎余潘杜戴夏钟汪田任姜范方石姚谭廖邹熊金陆郝孔白崔康毛邱秦江史顾侯邵孟龙万段雷钱汤尹黎易常武乔贺赖龚文阚骆邢严][\u4e00-\u9fff]{1,3})(?=被|涉|接受|因|严重|违|的)',
        
        # 姓名 + 违法关键词模式
        r'([王李张刘陈杨赵黄周吴徐孙胡朱高林何郭马罗梁宋郑谢韩唐冯于董萧程曹袁邓许傅沈曾彭吕苏卢蒋蔡贾丁魏薛叶阎余潘杜戴夏钟汪田任姜范方石姚谭廖邹熊金陆郝孔白崔康毛邱秦江史顾侯邵孟龙万段雷钱汤尹黎易常武乔贺赖龚文阚骆邢严][\u4e00-\u9fff]{1,3})(?=被查|被双开|被开除|被免职|被处分|涉嫌|被逮捕|被拘留|被判刑|主动投案|被诉|被调查|被立案|被撤职|因涉嫌|因严重违纪|接受调查|接受审查)',
        
        # 对...进行调查/处分模式
        r'对([王李张刘陈杨赵黄周吴徐孙胡朱高林何郭马罗梁宋郑谢韩唐冯于董萧程曹袁邓许傅沈曾彭吕苏卢蒋蔡贾丁魏薛叶阎余潘杜戴夏钟汪田任姜范方石姚谭廖邹熊金陆郝孔白崔康毛邱秦江史顾侯邵孟龙万段雷钱汤尹黎易常武乔贺赖龚文阚骆邢严][\u4e00-\u9fff]{1,3})(?=严重违纪违法|进行|立案|调查|审查|处分)'
    ]
    
    for pattern in position_name_patterns:
        matches = re.findall(pattern, text)
        for match in matches:
            if isinstance(match, tuple):
                # 对于元组，取第二个元素（姓名部分）
                name = match[1] if len(match) > 1 else match[0]
            else:
                name = match
            
            if is_valid_chinese_name(name):
                names.append(name)
    
    # 去重并返回
    valid_names = list(set(names))
    return valid_names

def extract_description(text, person_name=""):
    """改进的违法事件描述提取"""
    sentences = re.split(r'[。！？；\n]', text)
    relevant_sentences = []
    
    # 高权重关键词
    high_priority_keywords = [
        '被查', '被双开', '被开除', '被免职', '被处分', '涉嫌', '被逮捕', '被拘留', 
        '被判刑', '主动投案', '被诉', '被调查', '被立案', '被撤职', '严重违纪违法',
        '贪污', '腐败', '受贿', '行贿', '挪用', '滥用职权', '玩忽职守'
    ]
    
    # 行业关键词
    industry_keywords = [
        '医院', '医生', '院长', '主任', '医疗', '药品', '回扣', '红包', '医保', '骗保',
        '局长', '处长', '科长', '委员', '董事', '经理', '总监', '主管',
        '企业', '公司', '集团', '单位', '机构', '组织'
    ]
    
    for sentence in sentences:
        sentence = sentence.strip()
        if len(sentence) < 20 or len(sentence) > 300:
            continue
            
        # 检查是否包含关键词
        has_high_priority = any(kw in sentence for kw in high_priority_keywords)
        has_industry = any(kw in sentence for kw in industry_keywords)
        has_general_keyword = any(kw in sentence for kw in KEYWORDS)
        has_person = person_name and person_name in sentence if person_name else True
        
        if (has_high_priority or (has_industry and has_general_keyword)) and has_person:
            clean_sentence = re.sub(r'\s+', '', sentence)
            if len(clean_sentence) >= 20:
                relevant_sentences.append({
                    'sentence': clean_sentence,
                    'priority': 3 if has_high_priority else 2 if has_industry else 1,
                    'length': len(clean_sentence)
                })
    
    if relevant_sentences:
        # 如果有指定人名，优先返回包含该人名的句子
        if person_name:
            person_sentences = [s for s in relevant_sentences if person_name in s['sentence']]
            if person_sentences:
                best_sentence = max(person_sentences, key=lambda s: (s['priority'], min(s['length'], 150)))
                return best_sentence['sentence']
        
        # 否则返回优先级最高的句子
        best_sentence = max(relevant_sentences, key=lambda s: (s['priority'], min(s['length'], 150)))
        return best_sentence['sentence']
    
    return ""

def extract_position(text, person_name=""):
    """改进的职位提取函数"""
    if not text:
        return ""
    
    # 优先处理蓝色字体标记中的职位信息
    blue_text_positions = re.findall(r'【机构/职位】([^【】\n]+)', text)
    if blue_text_positions:
        for mixed_text in blue_text_positions:
            mixed_text = mixed_text.strip()
            position = extract_position_from_mixed_text(mixed_text)
            if position:
                return position
    
    # 职位层级（从高到低）
    position_hierarchy = [
        ['董事长', '总裁', '总经理', '首席执行官', 'CEO', '总监', '副总裁', '副总经理'],
        ['院长', '副院长', '党委书记', '纪委书记', '局长', '副局长', '厅长', '副厅长'],
        ['处长', '副处长', '科长', '副科长', '主任', '副主任', '主任委员', '副主任委员'],
        ['董事', '副董事长', '总监', '副总监', '经理', '副经理', '主管'],
        ['主任医师', '副主任医师', '主治医师', '主管医师', '住院医师'],
        ['委员', '护士长', '医生', '药师', '院长助理', '护士', '护师', '工程师', '会计师']
    ]
    
    # 如果有指定人名，优先在人名附近查找职位
    if person_name:
        sentences = re.split(r'[。！？；\n，]', text)
        person_contexts = []
        for sentence in sentences:
            if person_name in sentence:
                person_contexts.append(sentence)
        context_text = '。'.join(person_contexts)
        if context_text:
            text = context_text
    
    # 使用正则表达式查找职位
    all_matches = []
    for pattern in [
        r'(董事长|总裁|总经理|首席执行官|CEO|总监|副总裁|副总经理)',
        r'(院长|副院长|党委书记|纪委书记|局长|副局长|厅长|副厅长)',
        r'(处长|副处长|科长|副科长|主任|副主任|主任委员|副主任委员)',
        r'(董事|副董事长|总监|副总监|经理|副经理|主管)',
        r'(主任医师|副主任医师|主治医师|主管医师|住院医师)',
        r'(委员|护士长|医生|药师|院长助理|护士|护师|工程师|会计师)'
    ]:
        matches = re.findall(pattern, text)
        all_matches.extend(matches)
    
    if not all_matches:
        return ""
    
    # 按层级优先级选择最高职位
    for level in position_hierarchy:
        for position in all_matches:
            if position in level:
                return position
    
    return all_matches[0] if all_matches else ""

def extract_position_from_mixed_text(mixed_text):
    """从混合的机构/职位文本中提取职位"""
    position_keywords = [
        '董事长', '总裁', '总经理', '副总裁', '副总经理', '副董事长',
        '院长', '副院长', '党委书记', '纪委书记', '局长', '副局长', '厅长', '副厅长',
        '处长', '副处长', '科长', '副科长', '主任', '副主任', '主任委员', '副主任委员',
        '董事', '总监', '副总监', '经理', '副经理', '主管',
        '主任医师', '副主任医师', '主治医师', '主管医师', '住院医师',
        '委员', '护士长', '医生', '药师', '院长助理', '护士', '护师', '工程师', '会计师'
    ]
    
    for keyword in position_keywords:
        if keyword in mixed_text:
            return keyword
    
    return ""

def extract_hospital(text):
    """改进的机构提取函数"""
    institutions = []
    
    # 优先处理蓝色字体标记中的机构信息
    blue_text_institutions = re.findall(r'【机构/职位】([^【】\n]+)', text)
    if blue_text_institutions:
        for inst in blue_text_institutions:
            inst = inst.strip()
            if any(keyword in inst for keyword in ['医院', '委员会', '局', '厅', '公司', '集团', '中心', '科', '处', '部']):
                clean_inst = extract_institution_from_mixed_text(inst)
                if clean_inst and len(clean_inst) > 3:
                    institutions.append(clean_inst)
        if institutions:
            return select_best_institution(institutions)
    
    # 各种机构匹配模式
    patterns = [
        # 政府机构
        r'([\u4e00-\u9fa5]{2,8}[省市县区][\u4e00-\u9fa5]{2,15}(?:委员会|卫健委|发改委|局|厅|部门|中心|办公室|管理局|监督局))',
        # 企业机构
        r'([\u4e00-\u9fa5]{2,15}(?:有限公司|股份有限公司|有限责任公司|集团公司|控股集团|投资集团))',
        r'([\u4e00-\u9fa5]{3,15}(?:集团|公司))',
        # 医疗机构
        r'([\u4e00-\u9fa5]{2,8}[省市县区][\u4e00-\u9fa5]{2,15}(?:人民医院|中心医院|中医院|第[一二三四五六七八九十\d]+医院|医院))',
        r'([\u4e00-\u9fa5]{2,8}(?:大学|医科大学|医学院)附属[\u4e00-\u9fa5]{0,10}医院)',
        r'([\u4e00-\u9fa5]{3,12}医院)',
        # 其他机构
        r'([\u4e00-\u9fa5]{2,12}(?:大学|学院|研究院|研究所|科学院))',
        r'([\u4e00-\u9fa5]{2,12}(?:协会|学会|基金会|联合会|商会))'
    ]
    
    # 排除关键词
    exclude_keywords = [
        '医药慧', '赛柏蓝', '被查', '被处', '调查', '官方', '结果', '公布',
        '违法', '违纪', '贪污', '腐败', '受贿', '双开', '免职', '撤职'
    ]
    
    for pattern in patterns:
        matches = re.findall(pattern, text)
        for institution in matches:
            if is_valid_institution(institution, exclude_keywords):
                institutions.append(institution)
    
    return select_best_institution(institutions)

def extract_institution_from_mixed_text(mixed_text):
    """从混合的机构/职位文本中提取机构名称"""
    position_keywords = [
        '院长', '副院长', '主任', '副主任', '书记', '副书记', '局长', '副局长', 
        '厅长', '副厅长', '委员', '主席', '副主席', '科长', '处长', '部长',
        '总经理', '副总经理', '董事长', '副董事长', '总裁', '副总裁', '经理',
        '主任医师', '副主任医师', '主治医师', '医师', '护士长', '护师'
    ]
    
    for keyword in position_keywords:
        if keyword in mixed_text:
            parts = mixed_text.split(keyword)
            if parts[0]:
                institution = parts[0].strip()
                if any(org_keyword in institution for org_keyword in 
                      ['医院', '委员会', '局', '厅', '公司', '集团', '中心', '科', '处', '部']):
                    return institution
    
    return mixed_text.strip()

def is_valid_institution(institution, exclude_keywords):
    """验证机构名称是否有效"""
    if not institution or len(institution) < 3 or len(institution) > 40:
        return False
    
    if any(keyword in institution for keyword in exclude_keywords):
        return False
    
    invalid_starts = ['被', '涉', '查', '处', '违', '法', '纪', '开', '免', '撤']
    if any(institution.startswith(start) for start in invalid_starts):
        return False
    
    return True

def select_best_institution(institutions):
    """从候选机构中选择最佳的"""
    if not institutions:
        return ""
    
    unique_institutions = list(set(institutions))
    if len(unique_institutions) == 1:
        return unique_institutions[0]
    
    # 优先级关键词
    priority_keywords = [
        ['委员会', '省政府', '市政府', '县政府'],
        ['纪委', '监委', '组织部', '宣传部'],
        ['卫健委', '发改委', '局', '厅', '部门'],
        ['集团公司', '有限公司', '股份有限公司'],
        ['人民医院', '中心医院', '中医院', '医院'],
        ['大学', '学院', '研究院', '研究所']
    ]
    
    for priority_group in priority_keywords:
        for institution in unique_institutions:
            if any(keyword in institution for keyword in priority_group):
                return institution
    
    return max(unique_institutions, key=len)

def extract_multiple_violations(text):
    """提取一篇文章中的多个违法事件"""
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
                unique_key = f"{province}_{hospital}_{position}_{desc[:50]}"
                if unique_key not in seen_combinations:
                    seen_combinations.add(unique_key)
                    results.append({
                        "姓名": "",
                        "省份": province,
                        "医院": hospital,
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
            
            unique_key = f"{name}_{province}_{hospital}_{position}_{desc[:50]}"
            if unique_key not in seen_combinations:
                seen_combinations.add(unique_key)
                results.append({
                    "姓名": name,
                    "省份": province,
                    "医院": hospital,
                    "职位": position,
                    "描述": desc,
                    "日期": date
                })
    
    return results

def filter_and_extract(articles, gzh_name, existing_results=None, max_workers=None):
    """优化的筛选和提取函数 - 使用并发处理"""
    print(f"📊 本次共获取{len(articles)}篇文章，开始智能筛选...")
    results = existing_results or []
    
    if max_workers is None:
        max_workers = PERFORMANCE_CONFIG['max_workers']
    
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
                    create_time = time.strftime("%Y-%m-%d", time.localtime(create_time))
                
                if quick_title_filter(title):
                    article_tasks.append((
                        {"title": title, "link": link}, 
                        gzh_name, 
                        create_time
                    ))
        except Exception:
            continue
    
    print(f"🔍 标题预筛选：{len(articles)} -> {len(article_tasks)} 篇文章")
    
    if not article_tasks:
        print("❌ 没有匹配的文章")
        return results
    
    # 并发获取文章内容
    print(f"🚀 使用 {max_workers} 线程并发处理文章内容...")
    valid_articles = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_article = {
            executor.submit(process_single_article, task): task 
            for task in article_tasks
        }
        
        completed = 0
        for future in as_completed(future_to_article):
            completed += 1
            if completed % 10 == 0:
                print(f"⏳ 已处理 {completed}/{len(article_tasks)} 篇文章")
                
            try:
                result = future.result()
                if result:
                    valid_articles.append(result)
            except Exception as e:
                pass
    
    print(f"✅ 内容筛选：{len(article_tasks)} -> {len(valid_articles)} 篇有效文章")
    
    if not valid_articles:
        print("❌ 没有有效的文章内容")
        return results
    
    # 串行处理违法事件提取
    matched_count = 0
    for article_data in valid_articles:
        try:
            matched_count += 1
            title = article_data['title']
            link = article_data['link']
            content = article_data['content']
            create_time = article_data['create_time']
            
            print(f"📄 处理文章 {matched_count}: {title[:50]}...")
            
            # 提取多个违法事件
            violations = extract_multiple_violations(content)
            
            if matched_count <= 5:
                print(f"   📋 提取到 {len(violations)} 个违法事件")
                for j, v in enumerate(violations):
                    print(f"      事件{j+1}: 姓名={v['姓名']}, 机构={v['医院'][:20]}{'...' if len(v['医院']) > 20 else ''}, 职位={v['职位']}")
            
            for violation in violations:
                violation["通报时间"] = violation["日期"] or create_time
                violation["Resource"] = gzh_name
                violation["文章链接"] = link
                violation["标题"] = title
                
                description = violation['描述']
                if isinstance(description, str):
                    description_slice = description[:50]
                else:
                    description_slice = str(description)[:50]
                
                global_unique_key = f"{violation['姓名']}_{violation['省份']}_{violation['医院']}_{violation['职位']}_{description_slice}"
                
                if global_unique_key not in global_seen_combinations:
                    global_seen_combinations.add(global_unique_key)
                    
                    result = {
                        "通报时间": violation["通报时间"],
                        "省份": violation["省份"],
                        "医院": violation["医院"],
                        "职位": violation["职位"],
                        "姓名": violation["姓名"],
                        "Resource": violation["Resource"],
                        "Description": violation["描述"],
                        "文章链接": violation["文章链接"],
                        "标题": violation["标题"]
                    }
                    results.append(result)
                else:
                    if matched_count <= 5:
                        print(f"      ⚠️ 跳过重复记录: {violation['姓名']} - {violation['医院'][:20]}{'...' if len(violation['医院']) > 20 else ''}")
                    
        except Exception as e:
            print(f"❌ [{gzh_name}] 文章处理失败: {e}")
    
    print(f"🎯 处理完成：匹配到 {len(valid_articles)} 篇相关文章，提取到 {len(results) - len(existing_results or [])} 条新记录")
    return results

def save_progress(current_page, gzh_name, all_results):
    """保存爬取进度和数据"""
    os.makedirs('微信公众号文章', exist_ok=True)
    
    progress_info = {
        'last_page': current_page,
        'gzh_name': gzh_name,
        'total_records': len(all_results),
        'last_update': time.strftime("%Y-%m-%d %H:%M:%S")
    }
    
    progress_file = '微信公众号文章/医药慧_爬取进度.json'
    with open(progress_file, 'w', encoding='utf-8') as f:
        json.dump(progress_info, f, ensure_ascii=False, indent=2)
    
    if all_results:
        df = pd.DataFrame(all_results)
        output_file = '微信公众号文章/医药慧违法事件_优化版.xlsx'
        df.to_excel(output_file, index=False)
        print(f"已保存 {len(all_results)} 条记录到 {output_file}")
        print(f"进度已保存：当前页面{current_page}")

def load_progress():
    """加载爬取进度"""
    progress_file = '微信公众号文章/医药慧_爬取进度.json'
    data_file = '微信公众号文章/医药慧违法事件_优化版.xlsx'
    
    start_page = 0
    existing_results = []
    
    if os.path.exists(progress_file):
        try:
            with open(progress_file, 'r', encoding='utf-8') as f:
                progress_info = json.load(f)
            start_page = progress_info.get('last_page', 0)
            print(f"发现进度文件：上次爬取到第{start_page}页")
            print(f"上次更新时间：{progress_info.get('last_update', '未知')}")
        except Exception as e:
            print(f"读取进度文件失败: {e}")
    
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
        print(f"使用预设fakeid: {gzh['fakeid']}")
        
        current_page = start_page
        max_pages = 1000
        batch_size = 20
        
        try:
            while current_page < max_pages:
                print(f"\n=== 开始处理第{current_page//batch_size + 1}批次 (第{current_page+1}-{min(current_page+batch_size, max_pages)}页) ===")
                
                batch_articles, success = fetch_articles_batch(gzh, current_page, batch_size, page_size=10)
                
                if not success or not batch_articles:
                    print(f"第{current_page//batch_size + 1}批次抓取失败或无文章，停止爬取")
                    break
                
                print(f"第{current_page//batch_size + 1}批次共获取 {len(batch_articles)} 篇文章，开始筛选...")
                
                batch_results = filter_and_extract(batch_articles, gzh['name'], all_results)
                all_results = batch_results
                
                print(f"第{current_page//batch_size + 1}批次处理完成，累计获得 {len(all_results)} 条记录")
                
                current_page += batch_size
                save_progress(current_page, gzh['name'], all_results)
                
                if current_page < max_pages:
                    print(f"批次间休息2秒...")
                    time.sleep(2)
                    
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
        print(f"有医院信息的记录: {len(df[df['医院'].notna() & (df['医院'] != '')])}")
        print(f"有省份信息的记录: {len(df[df['省份'].notna() & (df['省份'] != '')])}")
        print(f"有人名信息的记录: {len(df[df['姓名'].notna() & (df['姓名'] != '')])}")
        
        print(f"\n=== 前5条记录预览 ===")
        for i, row in df.head().iterrows():
            print(f"记录 {i+1}:")
            print(f"  标题: {row['标题']}")
            print(f"  姓名: {row['姓名']}")
            print(f"  省份: {row['省份']}")
            print(f"  医院: {row['医院']}")
            print(f"  职位: {row['职位']}")
            print(f"  描述: {row['Description'][:100]}...")
            print()
    else:
        print("无有效数据")

if __name__ == "__main__":
    main()