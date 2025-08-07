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
from threading import Lock
import threading

# 全局锁和计数器用于线程安全
results_lock = Lock()
progress_lock = Lock()
processed_count = 0
matched_count = 0

# 关键词 - 扩大范围以获取更多相关文章
KEYWORDS = [
    "贪污", "腐败", "受贿", "处罚", "双开", "调查", "起诉", "纪检", "廉洁", "投案", "被查", "违规", "违纪", "处分", "免职", "撤职",
    "医院", "医生", "院长", "主任", "医疗", "药品", "回扣", "红包", "贿赂", "违法", "犯罪", "立案", "逮捕", "拘留", "判刑",
    "医保", "医保基金", "骗保", "套保", "虚开", "发票", "税务", "逃税", "漏税", "偷税", "补税", "罚款", "没收", "追缴",
    "主动投案", "被查", "被诉", "被逮捕", "被拘留", "被判刑", "被开除", "被免职", "被处分", "被双开", "被调查", "被立案",
    "副院长", "副主任", "委员", "科长", "副科长", "护士长", "药师", "院长助理", "主任医师", "副主任医师", "主治医师", "护士", "护师",
    "医疗腐败", "医药腐败", "医疗回扣", "药品回扣", "医疗贿赂", "药品贿赂", "医疗违法", "药品违法", "医疗犯罪", "药品犯罪"
]

# 易药人公众号配置
GZH_LIST = [
    {
        "name": "易药人",
        "token": "840103440",
        "cookie": "RK=DC2Uq4Wf9P; ptcz=c9f4dcf0c0fb279d2316b228ce1d2d7a6b107f591ae8bbce0eac0ce98bc9de36; wxuin=51340895845860; mm_lang=zh_CN; _hp2_id.1405110977=%7B%22userId%22%3A%226801447023479475%22%2C%22pageviewId%22%3A%228306667787246811%22%2C%22sessionId%22%3A%224504468753015668%22%2C%22identity%22%3Anull%2C%22trackerVersion%22%3A%224.0%22%7D; ua_id=mxGDXOVuOo8d0D2hAAAAACdqUxp53FqemlDjGf2eSLM=; rewardsn=; wxtokenkey=777; poc_sid=HBg3iGijlmGc_2ocHEPN26JgrEcR59UETkMwwy7P; _clck=3911425635|1|fy3|0; uuid=0f23747e8a4ce4803ac4c2e81813d9c3; rand_info=CAESICRG+nL2+PnQWtbjYd6JuRPOT89alJ8x3l0VMgP1oYnS; slave_bizuin=3911425635; data_bizuin=3911425635; bizuin=3911425635; data_ticket=0s37cmjlBOpA+6yyl1Vmc2ZL5TY2yMPaZb8t5y2aenlcBIOvu0qMjhWGtWAn5OiS; slave_sid=eFlBWmtDMTdWZUJGMFRpUUNyRGp6TFEyd2czYTRSa1NQY1RiWWtxQVY2dExTMDl4QVhGNHZGamZwUzJ1djJlelpnTjlTcmdsNGVFaXBWSXNnVDJvcEdQUGJoMWFQTzU1MzVlbnpXRXdLQWk5a2FQWWpwNXkyMk1sb0NDMGhPTkpjanF0N2dsVlR3b2prWkts; slave_user=gh_b3cdf815ccbf; xid=e437bfb7c69a0dd7d1b3c2fa393774e4; _clsk=hkj0kq|1754023274661|2|1|mp.weixin.qq.com/weheat-agent/payload/record"
    },
]

# 信息抽取正则（可根据实际优化）
DATE_RE = re.compile(r"(\d{4}[年/-]\d{1,2}[月/-]\d{1,2}日?)|(\d{1,2}/\d{1,2}/\d{4})|(\d{4}-\d{1,2}-\d{1,2})")
PROVINCE_RE = re.compile(
    r"(北京|天津|上海|重庆|河北|山西|辽宁|吉林|黑龙江|江苏|浙江|安徽|福建|江西|山东|河南|湖北|湖南|广东|海南|四川|贵州|云南|陕西|甘肃|青海|台湾|内蒙古|广西|西藏|宁夏|新疆|香港|澳门)"
)
HOSPITAL_RE = re.compile(r"[\u4e00-\u9fa5]{2,}(医院|卫生院|卫生服务中心|中医院|医科大学附属医院|人民医院|中心医院|第一医院|第二医院|第三医院)")
NAME_RE = re.compile(r"[\u4e00-\u9fa5]{2,4}")
POSITION_RE = re.compile(r"(院长|书记|主任|副院长|副主任|委员|科长|副科长|护士长|医生|药师|院长助理|主任医师|副主任医师|主治医师|护士|护师)")

def fetch_articles(gzh, page_size=10, max_pages=1000):
    """抓取公众号文章列表 - 使用新的POST接口"""
    all_articles = []
    consecutive_empty_pages = 0  # 连续空页计数
    max_consecutive_empty = 3    # 最大连续空页数
    
    for page in range(max_pages):
        # 构造GET请求参数
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
            'fingerprint': '45e623cde6e181b213b3f595227b4a71',
            'token': gzh['token'],
            'lang': 'zh_CN',
            'f': 'json',
            'ajax': '1',
        }
        
        headers = {
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
            'cookie': gzh['cookie'],
            'referer': 'https://mp.weixin.qq.com/',
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'accept-encoding': 'gzip, deflate, br, zstd',
            'sec-ch-ua': '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'x-requested-with': 'XMLHttpRequest'
        }
        
        try:
            # 恢复使用原来的GET接口，但保留更新的认证信息
            resp = requests.get(
                "https://mp.weixin.qq.com/cgi-bin/appmsgpublish", 
                params=params, 
                headers=headers, 
                timeout=20
            )
            
            print(f"第{page+1}页请求状态码: {resp.status_code}")
            if resp.status_code != 200:
                print(f"请求失败，状态码: {resp.status_code}")
                if resp.status_code == 429:  # 请求过于频繁
                    print("请求频率限制，增加延迟...")
                    time.sleep(5)
                    continue
                break
                
            result = resp.json()
            
            # 调试：打印响应结构
            if page == 0:
                print(f"API响应结构: {list(result.keys()) if isinstance(result, dict) else type(result)}")
            
            # 尝试不同的响应结构
            articles = []
            if result.get("base_resp", {}).get("ret") != 0:
                print(f"API返回错误: {result.get('base_resp', {})}")
                consecutive_empty_pages += 1
                if consecutive_empty_pages >= max_consecutive_empty:
                    print(f"连续{max_consecutive_empty}次API错误，停止爬取")
                    break
                continue
                
            # 尝试解析文章数据 - 适配新接口的响应格式
            if "publish_page" in result:
                publish_page = result.get("publish_page")
                if publish_page:
                    articleList = json.loads(publish_page)
                    articles = articleList.get("publish_list", [])
            elif "data" in result:
                # 如果新接口直接返回data字段
                articles = result.get("data", {}).get("articles", []) or result.get("data", [])
            elif "list" in result:
                # 如果新接口直接返回list字段
                articles = result.get("list", [])
            else:
                # 尝试直接从根级别获取文章列表
                articles = result.get("articles", []) or result.get("items", [])
            
            if not articles:
                print(f"第{page+1}页没有文章")
                consecutive_empty_pages += 1
                if consecutive_empty_pages >= max_consecutive_empty:
                    print(f"连续{max_consecutive_empty}页无文章，停止爬取")
                    break
                continue
            else:
                consecutive_empty_pages = 0  # 重置连续空页计数
            
            print(f"第{page+1}页获取到{len(articles)}篇文章")
            all_articles.extend(articles)
            
            # 动态调整延迟，避免被限制
            if page % 10 == 0:
                time.sleep(3)  # 每10页增加延迟
            else:
                time.sleep(1.5)  # POST请求增加延迟
                
        except requests.exceptions.RequestException as e:
            print(f"{gzh['name']} 第{page+1}页网络请求失败: {e}")
            consecutive_empty_pages += 1
            if consecutive_empty_pages >= max_consecutive_empty:
                print(f"连续{max_consecutive_empty}次网络失败，停止爬取")
                break
            time.sleep(5)  # 网络失败后增加更长延迟
            continue
        except json.JSONDecodeError as e:
            print(f"{gzh['name']} 第{page+1}页JSON解析失败: {e}")
            print(f"响应内容前500字符: {resp.text[:500]}")
            consecutive_empty_pages += 1
            if consecutive_empty_pages >= max_consecutive_empty:
                print(f"连续{max_consecutive_empty}次解析失败，停止爬取")
                break
            time.sleep(3)
            continue
        except Exception as e:
            print(f"{gzh['name']} 第{page+1}页抓取失败: {e}")
            consecutive_empty_pages += 1
            if consecutive_empty_pages >= max_consecutive_empty:
                print(f"连续{max_consecutive_empty}次失败，停止爬取")
                break
            time.sleep(3)  # 失败后增加延迟
            continue
    
    print(f"总共获取到 {len(all_articles)} 篇文章")
    return all_articles

def get_article_content(link, gzh_name):
    """获取文章正文 - 为并行处理优化"""
    try:
        # 添加随机延迟避免请求过于频繁
        time.sleep(random.uniform(0.1, 0.5))
        
        headers = {
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36', 
            'referer': 'https://mp.weixin.qq.com/',
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8'
        }
        
        # 增加重试机制
        max_retries = 3
        for attempt in range(max_retries):
            try:
                resp = requests.get(link, headers=headers, timeout=20)
                
                if resp.status_code == 200:
                    break
                elif resp.status_code == 429:  # 请求过于频繁
                    if attempt < max_retries - 1:
                        wait_time = (attempt + 1) * 2
                        print(f"请求频率限制，等待 {wait_time} 秒后重试...")
                        time.sleep(wait_time)
                        continue
                else:
                    print(f"文章请求失败，状态码: {resp.status_code}")
                    if attempt < max_retries - 1:
                        time.sleep(1)
                        continue
                    else:
                        return ''
            except requests.RequestException as e:
                if attempt < max_retries - 1:
                    print(f"请求异常，重试中... {e}")
                    time.sleep(2)
                    continue
                else:
                    print(f"请求最终失败: {e}")
                    return ''
        else:
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
        print(f"[{gzh_name}] 文章内容获取失败: {e}")
        return ''

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

def extract_name(text):
    """改进的姓名提取逻辑 - 大幅提高准确性，减少误识别"""
    names = []
    
    # 扩展常见的中文姓氏
    common_surnames = [
        '王', '李', '张', '刘', '陈', '杨', '赵', '黄', '周', '吴', '徐', '孙', '胡', '朱', '高', '林', '何', '郭', '马', '罗', 
        '梁', '宋', '郑', '谢', '韩', '唐', '冯', '于', '董', '萧', '程', '曹', '袁', '邓', '许', '傅', '沈', '曾', '彭', '吕', 
        '苏', '卢', '蒋', '蔡', '贾', '丁', '魏', '薛', '叶', '阎', '余', '潘', '杜', '戴', '夏', '钟', '汪', '田', '任', '姜', 
        '范', '方', '石', '姚', '谭', '廖', '邹', '熊', '金', '陆', '郝', '孔', '白', '崔', '康', '毛', '邱', '秦', '江', '史', 
        '顾', '侯', '邵', '孟', '龙', '万', '段', '雷', '钱', '汤', '尹', '黎', '易', '常', '武', '乔', '贺', '赖', '龚', '文',
        '向', '许', '欧', '欧阳', '太史', '端木', '上官', '司马', '东方', '独孤', '南宫', '万俟', '闻人', '夏侯', '诸葛', 
        '尉迟', '公羊', '赫连', '澹台', '皇甫', '宗政', '濮阳', '公冶', '太叔', '申屠', '公孙', '慕容', '仲孙', '钟离',
        '长孙', '宇文', '城池', '司徒', '鲜于', '司空', '汝嫣', '闾丘', '子车', '亓官', '司寇', '巫马', '公西', '颛孙',
        '壤驷', '公良', '漆雕', '乐正', '宰父', '谷梁', '拓跋', '夹谷', '轩辕', '令狐', '段干', '百里', '呼延', '东郭',
        '南门', '羊舌', '微生', '公户', '公玉', '公仪', '梁丘', '公仲', '公上', '公门', '公山', '公坚', '左丘', '公伯',
        '西门', '公祖', '第五', '公乘', '贯丘', '公皙', '南荣', '东里', '东宫', '仲长', '子书', '子桑', '即墨', '达奚',
        '褚师', '吴铭'
    ]
    
    # 更严格的无效词汇列表
    invalid_words = [
        '易药人', '赛柏蓝', '医院', '医生', '主任', '院长', '护士', '药师', '委员', '科长', '副科长', '护士长', 
        '院长助理', '主任医师', '副主任医师', '主治医师', '护师', '董事长', '副总裁', '常委', '骗保', '虚开', 
        '行为', '还', '因', '被', '处', '分', '开', '除', '免', '职', '撤', '诉', '调', '查', '立', '案', '逮', '捕', 
        '拘', '留', '判', '刑', '投', '双', '涉', '嫌', '报告', '通报', '发现', '检查', '巡视', '审计', '监督',
        '违规', '违纪', '违法', '腐败', '贪污', '受贿', '贿赂', '回扣', '红包', '公司', '集团', '有限', '股份',
        '机构', '组织', '部门', '单位', '局', '厅', '委', '办', '处', '科', '股', '所', '中心', '站', '队', '会',
        '协会', '学会', '研究', '试验', '检测', '认证', '服务', '管理', '发展', '建设', '规划', '设计', '咨询',
        '培训', '教育', '学校', '大学', '学院', '系', '专业', '课程', '教学', '老师', '教授', '讲师', '学生',
        '实习', '毕业', '招生', '考试', '成绩', '证书', '资格', '执照', '许可', '批准', '核准', '备案', '登记',
        '注册', '申请', '受理', '审批', '验收', '评估', '考核', '监管', '执法', '处罚', '罚款', '没收', '追缴',
        '退赔', '赔偿', '损失', '责任', '义务', '权利', '权限', '职责', '职能', '职务', '岗位', '工作', '任务',
        '项目', '计划', '方案', '措施', '办法', '规定', '制度', '标准', '规范', '要求', '条件', '程序', '流程',
        '步骤', '环节', '阶段', '时间', '期限', '截止', '开始', '结束', '完成', '落实', '执行', '实施', '推进',
        '开展', '进行', '组织', '安排', '部署', '协调', '配合', '支持', '帮助', '指导', '监督', '检查', '督促',
        '跟踪', '反馈', '汇报', '总结', '评价', '分析', '研究', '讨论', '决定', '确定', '明确', '统一', '一致'
    ]
    
    # 方法1: 精确匹配"某某"模式
    for surname in common_surnames:
        # 匹配"李某"、"张某某"等模式
        patterns = [f"{surname}某", f"{surname}某某"]
        for pattern in patterns:
            if pattern in text:
                names.append(pattern)
    
    # 方法2: 使用更精确的正则匹配违法人员姓名
    violation_keywords = [
        '被查', '被双开', '被开除', '被免职', '被处分', '涉嫌', '被逮捕', '被拘留', '被判刑', 
        '主动投案', '被诉', '被调查', '被立案', '被撤职', '移送', '立案调查', '纪律处分',
        '开除党籍', '开除公职', '取消资格', '吊销执照', '禁止从业'
    ]
    
    # 精确的姓名模式：姓氏 + 1-2个字符，但排除常见误匹配
    for keyword in violation_keywords:
        # 查找关键词前的姓名
        pattern = r'([' + ''.join(common_surnames) + r'])([^\u4e00-\u9fff\s]{0,2})([被涉主移])?' + re.escape(keyword)
        matches = re.finditer(pattern, text)
        for match in matches:
            start_pos = max(0, match.start() - 8)
            end_pos = match.start()
            before_text = text[start_pos:end_pos]
            
            # 查找完整姓名
            name_pattern = r'([' + ''.join(common_surnames) + r'])([^\s\u3001\u3002\uff0c\uff1b\uff1a\u201c\u201d\u2018\u2019\u300a\u300b\u3008\u3009\u3010\u3011\u3014\u3015\uff08\uff09]{1,3})(?=' + re.escape(keyword) + '|被|涉)'
            name_matches = re.findall(name_pattern, before_text)
            
            for name_match in name_matches:
                if isinstance(name_match, tuple):
                    candidate_name = ''.join(name_match)
                else:
                    candidate_name = name_match
                
                # 严格验证候选姓名
                if (2 <= len(candidate_name) <= 4 and
                    all('\u4e00' <= c <= '\u9fff' for c in candidate_name) and
                    not any(invalid in candidate_name for invalid in invalid_words) and
                    candidate_name[0] in common_surnames):
                    names.append(candidate_name)
    
    # 方法3: 职位 + 姓名模式（更精确）
    position_keywords = [
        '院长', '副院长', '书记', '副书记', '主任', '副主任', '委员', '科长', '副科长', '护士长', 
        '医生', '药师', '院长助理', '主任医师', '副主任医师', '主治医师', '护士', '护师',
        '主席', '副主席', '会长', '副会长', '理事长', '副理事长', '秘书长', '副秘书长',
        '局长', '副局长', '处长', '副处长', '股长', '副股长', '所长', '副所长',
        '经理', '副经理', '总监', '副总监', '总裁', '副总裁', '董事', '监事'
    ]
    
    for position in position_keywords:
        # 职位前的姓名
        pattern1 = r'([' + ''.join(common_surnames) + r'])([^\s\u3001\u3002\uff0c\uff1b\uff1a\u201c\u201d\u2018\u2019\u300a\u300b\u3008\u3009\u3010\u3011\u3014\u3015\uff08\uff09]{1,3})' + re.escape(position)
        matches1 = re.findall(pattern1, text)
        
        # 职位后的姓名  
        pattern2 = re.escape(position) + r'([' + ''.join(common_surnames) + r'])([^\s\u3001\u3002\uff0c\uff1b\uff1a\u201c\u201d\u2018\u2019\u300a\u300b\u3008\u3009\u3010\u3011\u3014\u3015\uff08\uff09]{1,3})'
        matches2 = re.findall(pattern2, text)
        
        for matches in [matches1, matches2]:
            for match in matches:
                if isinstance(match, tuple):
                    candidate_name = ''.join(match)
                else:
                    candidate_name = match
                
                if (2 <= len(candidate_name) <= 4 and
                    all('\u4e00' <= c <= '\u9fff' for c in candidate_name) and
                    not any(invalid in candidate_name for invalid in invalid_words) and
                    candidate_name[0] in common_surnames and
                    candidate_name != position):
                    names.append(candidate_name)
    
    # 去重并最终过滤
    unique_names = list(set(names))
    final_names = []
    
    for name in unique_names:
        # 最严格的过滤：确保是真实姓名
        if (2 <= len(name) <= 4 and
            all('\u4e00' <= c <= '\u9fff' for c in name) and
            name[0] in common_surnames and
            not any(invalid in name for invalid in invalid_words) and
            not re.search(r'[0-9a-zA-Z]', name) and  # 不包含数字和字母
            '某' not in name):  # 排除"某"字，除非是标准的"李某"格式
            
            # 特殊检查：如果包含"某"，必须是标准格式
            if '某' in name:
                if name.endswith('某') or name.endswith('某某'):
                    final_names.append(name)
            else:
                final_names.append(name)
    
    return final_names

def extract_description(text, person_name=""):
    """改进的违法事件描述提取"""
    sentences = re.split(r'[。！？；\n]', text)
    relevant_sentences = []
    
    for sentence in sentences:
        sentence = sentence.strip()
        if len(sentence) < 10 or len(sentence) > 200:
            continue
            
        # 检查是否包含关键词
        has_keyword = any(kw in sentence for kw in KEYWORDS)
        has_person = person_name and person_name in sentence if person_name else True
        
        if has_keyword and has_person:
            clean_sentence = re.sub(r'\s+', '', sentence)
            if not any(word in clean_sentence for word in ['编辑', '作者', '来源', '转载', '关注', '点击', '扫码']):
                relevant_sentences.append(clean_sentence)
    
    if relevant_sentences:
        if person_name:
            person_sentences = [s for s in relevant_sentences if person_name in s]
            if person_sentences:
                return person_sentences[0]
        
        best_sentence = max(relevant_sentences, key=lambda s: sum(1 for kw in KEYWORDS if kw in s))
        return best_sentence
    
    return ""

def extract_institution(text):
    """扩展的机构名称提取 - 包括医院、委员会、公司、机构等"""
    institution_patterns = [
        # 医院相关
        r'([\u4e00-\u9fa5]{2,15}(?:人民医院|中心医院|中医院|医院|妇幼保健院|儿童医院|肿瘤医院|传染病医院|精神病医院|口腔医院|眼科医院|骨科医院))',
        r'([\u4e00-\u9fa5]{2,12}(?:大学|医学院|学院)[\u4e00-\u9fa5]{0,8}(?:附属)?[\u4e00-\u9fa5]{0,8}医院)',
        r'([\u4e00-\u9fa5]{2,10}第[一二三四五六七八九十\d]+(?:人民医院|医院|中心医院))',
        
        # 卫生健康委员会
        r'([\u4e00-\u9fa5]{2,8}(?:省|市|县|区)?(?:卫生健康委员会|卫健委|卫生委员会|健康委员会))',
        r'([\u4e00-\u9fa5]{2,8}(?:省|市|县|区)?(?:卫生局|健康局|卫生健康局))',
        
        # 药监局、食药监
        r'([\u4e00-\u9fa5]{2,8}(?:省|市|县|区)?(?:药品监督管理局|药监局|食品药品监督管理局|食药监局|市场监督管理局|市监局))',
        
        # 医保局
        r'([\u4e00-\u9fa5]{2,8}(?:省|市|县|区)?(?:医疗保障局|医保局|社会保险基金管理局|社保局))',
        
        # 公司、企业
        r'([\u4e00-\u9fa5]{2,20}(?:医药|药业|生物|健康|科技|医疗)(?:有限公司|股份有限公司|集团有限公司|公司|企业|集团))',
        r'([\u4e00-\u9fa5]{2,20}(?:有限公司|股份有限公司|集团有限公司))',
        
        # 协会、学会
        r'([\u4e00-\u9fa5]{2,15}(?:医学会|医师协会|护理学会|药师协会|医院协会|健康协会|协会|学会))',
        
        # 基金会、慈善机构
        r'([\u4e00-\u9fa5]{2,15}(?:基金会|慈善基金会|公益基金会|教育基金会))',
        
        # 研究院、研究所
        r'([\u4e00-\u9fa5]{2,15}(?:医学研究院|科学研究院|研究院|医学研究所|科研所|研究所))',
        
        # 中心、机构
        r'([\u4e00-\u9fa5]{2,15}(?:医疗中心|健康中心|体检中心|诊疗中心|康复中心|中心))',
        r'([\u4e00-\u9fa5]{2,15}(?:检验检测机构|认证机构|评估机构|咨询机构|服务机构|机构))',
        
        # 政府部门
        r'([\u4e00-\u9fa5]{2,10}(?:省|市|县|区)?(?:人民政府|政府|发展和改革委员会|发改委|财政局|审计局|纪委监委|纪委))',
        
        # 医疗器械、检验机构
        r'([\u4e00-\u9fa5]{2,15}(?:医疗器械|检验检测|质量检验|技术检测|第三方检测)(?:有限公司|公司|中心|机构|所))',
        
        # 其他医疗相关机构
        r'([\u4e00-\u9fa5]{2,15}(?:卫生服务中心|社区卫生服务站|疾病预防控制中心|疾控中心|血站|采供血机构|妇幼保健所))'
    ]
    
    # 无效关键词 - 排除通用描述
    invalid_keywords = [
        '税务穿透到', '也就是允许', '虚开', '处罚', '审计', '廉洁', '调查', 
        '违规', '贿赂', 'CSO', '药企', '大批', '多家', '各大', '所有', '全部',
        '公立', '私立', '三甲', '二甲', '基层', '社区', '乡镇', '医教协同', 
        '送礼多名', '配方颗粒', '和公立', '包括', '安排患者到就诊', '出货情况与终端',
        '刻制某', '医药企业主要谋求向', '管理', '改革', '发展', '建设', '服务', '质量',
        '安全', '文化', '制度', '规范', '标准', '要求', '规定', '政策', '措施', '方案', 
        '计划', '目标', '任务', '工作', '业务', '技术', '设备', '设施', '环境', '条件', 
        '水平', '能力', '实力', '规模', '数量', '分布', '布局', '结构', '体系', '网络',
        '联盟', '合作', '交流', '培训', '教育', '科研', '创新', '转型', '升级', '改造',
        '扩建', '新建', '搬迁', '合并', '重组', '整合', '等', '及', '或', '和', '与',
        '在', '的', '了', '是', '有', '为', '以', '从', '对', '将', '被', '向', '由'
    ]
    
    institutions = []
    for pattern in institution_patterns:
        matches = re.findall(pattern, text)
        for institution in matches:
            # 基本长度和有效性检查
            if (len(institution) >= 3 and len(institution) <= 30 and
                not any(keyword in institution for keyword in invalid_keywords) and
                not re.search(r'[0-9a-zA-Z]', institution) and  # 不包含数字字母
                '\u4e00' <= institution[0] <= '\u9fff'):  # 以中文开头
                
                # 额外验证：确保包含机构类型词汇
                institution_types = [
                    '医院', '委员会', '局', '公司', '企业', '集团', '协会', '学会', '基金会',
                    '研究院', '研究所', '中心', '机构', '政府', '所', '站', '处', '科', '股',
                    '厅', '部', '委', '办'
                ]
                
                if any(inst_type in institution for inst_type in institution_types):
                    institutions.append(institution)
    
    # 返回最长的机构名称（通常最完整）
    if institutions:
        return max(institutions, key=len)
    
    return ""

def enhance_institution_name(institution, province):
    """增强机构名称，为缺少地区名的机构添加地区名"""
    if not institution or not province:
        return institution
    
    # 如果已经包含省市县区，直接返回
    if re.search(r'[省市县区]', institution):
        return institution
    
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
    
    # 对于医院类机构，添加市名
    if re.search(r'第[一二三四五六七八九十\d]+人民医院|第[一二三四五六七八九十\d]+医院|中心医院|人民医院|医院', institution):
        if any(city in institution for city in cities):
            return institution
        
        if cities:
            return f"{cities[0]}{institution}"
        else:
            return institution
    
    # 对于其他机构，也尝试添加地区名
    if cities and not any(city in institution for city in cities):
        # 只为医疗相关机构添加地区名
        medical_keywords = ['医院', '卫生', '健康', '医疗', '药监', '医保', '疾控']
        if any(keyword in institution for keyword in medical_keywords):
            return f"{cities[0]}{institution}"
    
    return institution

def extract_multiple_violations(text):
    """提取一篇文章中的多个违法事件 - 避免重复并提高准确性，过滤无姓名记录"""
    results = []
    
    # 提取所有可能的人名
    names = extract_name(text)
    
    # 用于去重的集合
    seen_combinations = set()
    
    # 只处理有姓名的情况，无姓名的记录一律过滤掉
    if names:
        # 为每个人名提取信息
        for name in names:
            # 验证姓名的有效性
            if not name or len(name) < 2 or len(name) > 4:
                continue
                
            context_sentences = []
            sentences = re.split(r'[。！？；\n]', text)
            
            for i, sentence in enumerate(sentences):
                if name in sentence:
                    start = max(0, i-1)
                    end = min(len(sentences), i+2)
                    context_sentences.extend(sentences[start:end])
            
            context = '。'.join(context_sentences)
            
            province = extract_first(PROVINCE_RE, context) or extract_first(PROVINCE_RE, text)
            institution = extract_institution(context) or extract_institution(text)
            desc = extract_description(context, name)
            date = extract_first(DATE_RE, context) or extract_first(DATE_RE, text)
            position = extract_first(POSITION_RE, context)
            
            # 只有描述不为空才记录
            if desc:
                enhanced_institution = enhance_institution_name(institution, province)
                
                # 创建唯一标识符用于去重
                unique_key = f"{name}_{province}_{enhanced_institution}_{position}_{desc[:50]}"
                if unique_key not in seen_combinations:
                    seen_combinations.add(unique_key)
                    results.append({
                        "姓名": name,
                        "省份": province,
                        "机构": enhanced_institution if enhanced_institution else "",
                        "职位": position,
                        "描述": desc,
                        "日期": date
                    })
    
    return results

def process_single_article(article_data):
    """处理单篇文章的函数，用于并行处理"""
    article, gzh_name, global_seen_combinations = article_data
    global processed_count, matched_count
    
    try:
        info = json.loads(article.get("publish_info", "{}"))
        if not info.get("appmsgex"):
            return []
            
        title = info["appmsgex"][0].get("title", "")
        link = info["appmsgex"][0].get("link", "")
        create_time = info["appmsgex"][0].get("create_time")
        
        # 统一时间格式为YYYY/MM/DD
        if isinstance(create_time, int):
            formatted_time = time.strftime("%Y/%m/%d", time.localtime(create_time))
        else:
            formatted_time = time.strftime("%Y/%m/%d")
        
        # 线程安全的计数更新
        with progress_lock:
            processed_count += 1
            current_processed = processed_count
            if current_processed % 10 == 0:
                print(f"已处理 {current_processed} 篇文章")
        
        content = get_article_content(link, gzh_name)
        
        # 放宽关键词过滤条件 - 确保能捕获主动投案、被查、被诉等文章
        if not any(k in title or k in content for k in KEYWORDS):
            return []
        
        # 线程安全的匹配计数更新
        with progress_lock:
            matched_count += 1
            current_matched = matched_count
            print(f"\n匹配文章 {current_matched}: {title}")
        
        # 提取多个违法事件
        violations = extract_multiple_violations(content)
        results = []
        
        for violation in violations:
            # 创建全局唯一标识符用于去重
            description = violation['描述']
            if isinstance(description, str):
                description_slice = description[:50]
            else:
                description_slice = str(description)[:50]
            
            global_unique_key = f"{violation['姓名']}_{violation['省份']}_{violation['机构']}_{violation['职位']}_{description_slice}"
            
            # 线程安全的去重检查
            with results_lock:
                if global_unique_key not in global_seen_combinations:
                    global_seen_combinations.add(global_unique_key)
                    
                    # 重新排列字段顺序，第一列为发布时间
                    result = {
                        "发布时间": formatted_time,  # 统一使用文章发布时间
                        "省份": violation["省份"],
                        "机构": violation["机构"],
                        "职位": violation["职位"],
                        "姓名": violation["姓名"],
                        "Resource": gzh_name,
                        "Description": violation["描述"],
                        "文章链接": link,
                        "标题": title
                    }
                    results.append(result)
        
        return results
        
    except Exception as e:
        print(f"[{gzh_name}] 文章解析失败: {e}")
        return []

def filter_and_extract(articles, gzh_name, existing_results=None):
    print(f"本次共获取{len(articles)}篇文章，开始并行关键词筛选...")
    global processed_count, matched_count
    
    # 重置全局计数器
    processed_count = 0
    matched_count = 0
    
    results = existing_results or []
    
    # 用于全局去重的集合
    global_seen_combinations = set()
    if existing_results:
        for result in existing_results:
            description = result.get('Description', '')
            # 处理NaN值
            if pd.isna(description):
                description = ''
            elif isinstance(description, str):
                description = description[:50]
            else:
                description = str(description)[:50]
            
            unique_key = f"{result.get('姓名', '')}_{result.get('省份', '')}_{result.get('机构', '')}_{result.get('职位', '')}_{description}"
            global_seen_combinations.add(unique_key)
    
    # 准备并行处理的数据
    article_data_list = [(article, gzh_name, global_seen_combinations) for article in articles]
    
    # 使用线程池并行处理文章
    max_workers = min(8, len(articles))  # 最大8个线程，避免过多请求
    print(f"使用 {max_workers} 个线程并行处理文章...")
    
    all_new_results = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 提交所有任务
        future_to_article = {executor.submit(process_single_article, article_data): i 
                            for i, article_data in enumerate(article_data_list)}
        
        # 收集结果
        for future in as_completed(future_to_article):
            try:
                article_results = future.result()
                if article_results:
                    all_new_results.extend(article_results)
                    
                    # 每累积50条新结果保存一次进度
                    if len(all_new_results) % 50 == 0:
                        with results_lock:
                            temp_results = results + all_new_results
                            temp_df = pd.DataFrame(temp_results)
                            os.makedirs('微信公众号文章', exist_ok=True)
                            temp_df.to_excel('微信公众号文章/易药人违法事件_临时保存.xlsx', index=False)
                            print(f"已临时保存 {len(temp_results)} 条记录")
                            
            except Exception as e:
                print(f"处理文章时出错: {e}")
    
    # 合并所有结果
    results.extend(all_new_results)
    
    print(f"并行处理完成！总共处理了 {processed_count} 篇文章，匹配到 {matched_count} 篇相关文章")
    print(f"本次新增 {len(all_new_results)} 条有效记录")
    
    return results

def get_fakeid(gzh_name, token, cookie):
    """通过公众号名称自动获取fakeid - 使用更新的headers"""
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
        'Cookie': cookie,
        'Accept': '*/*',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br, zstd',
        'Referer': 'https://mp.weixin.qq.com/',
        'sec-ch-ua': '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'X-Requested-With': 'XMLHttpRequest'
    }
    try:
        resp = requests.get(search_url, params=params, headers=headers, timeout=15)
        print(f"获取fakeid响应状态码: {resp.status_code}")
        
        if resp.status_code != 200:
            print(f"获取fakeid请求失败，状态码: {resp.status_code}")
            return None
            
        data = resp.json()
        print(f"搜索响应: {data}")
        
        if data.get('list'):
            fakeid = data['list'][0]['fakeid']
            print(f"成功获取到 {gzh_name} 的fakeid: {fakeid}")
            return fakeid
        else:
            print(f"未找到公众号 {gzh_name} 的fakeid，请检查名称、token、cookie")
            print(f"搜索返回的数据结构: {list(data.keys()) if isinstance(data, dict) else type(data)}")
            return None
    except Exception as e:
        print(f"获取fakeid失败: {e}")
        return None

def test_new_api():
    """测试更新后的接口是否正常工作"""
    print("=== 测试更新后的接口 ===")
    gzh = GZH_LIST[0]  # 使用第一个公众号进行测试
    
    # 获取fakeid
    print(f"正在获取 {gzh['name']} 的fakeid...")
    fakeid = get_fakeid(gzh['name'], gzh['token'], gzh['cookie'])
    if not fakeid:
        print("无法获取fakeid，测试失败")
        return False
    
    gzh['fakeid'] = fakeid
    print(f"成功获取fakeid: {fakeid}")
    
    # 测试获取第一页文章
    print("正在测试获取文章列表...")
    articles = fetch_articles(gzh, page_size=5, max_pages=1)  # 只获取第一页，5篇文章
    
    if articles:
        print(f"✅ 成功获取到 {len(articles)} 篇文章")
        print("\n=== 第一篇文章信息示例 ===")
        first_article = articles[0]
        print(f"文章数据结构: {list(first_article.keys()) if isinstance(first_article, dict) else type(first_article)}")
        
        if isinstance(first_article, dict):
            # 尝试解析文章信息
            try:
                info = json.loads(first_article.get("publish_info", "{}"))
                if info.get("appmsgex"):
                    title = info["appmsgex"][0].get("title", "")
                    link = info["appmsgex"][0].get("link", "")
                    create_time = info["appmsgex"][0].get("create_time")
                    print(f"标题: {title}")
                    print(f"链接: {link[:100]}...")
                    print(f"创建时间: {create_time}")
                else:
                    print("未找到appmsgex字段，可能需要调整解析逻辑")
                    print(f"publish_info内容: {first_article.get('publish_info', '')[:200]}...")
            except Exception as e:
                print(f"解析文章信息失败: {e}")
                print(f"原始数据: {str(first_article)[:300]}...")
        
        return True
    else:
        print("❌ 未能获取到文章，可能需要调整请求参数")
        return False

def main():
    all_results = []
    
    # 首先测试更新后的接口
    if not test_new_api():
        print("接口测试失败，请检查配置后重试")
        return
    
    print("\n" + "="*50)
    print("开始正式爬取...")
    
    # 检查是否有已保存的数据
    existing_file = '微信公众号文章/易药人违法事件_改进版.xlsx'
    if os.path.exists(existing_file):
        try:
            existing_df = pd.read_excel(existing_file)
            print(f"发现已有数据文件，包含 {len(existing_df)} 条记录")
            # 可以选择是否继续追加数据
            response = input("是否继续追加新数据？(y/n): ").lower()
            if response == 'y':
                all_results = existing_df.to_dict('records')
                print(f"将基于现有 {len(all_results)} 条记录继续爬取")
            else:
                print("将重新开始爬取")
        except Exception as e:
            print(f"读取现有文件失败: {e}")
    
    for gzh in GZH_LIST:
        print(f"抓取公众号: {gzh['name']}")
        fakeid = get_fakeid(gzh['name'], gzh['token'], gzh['cookie'])
        if not fakeid:
            continue
        gzh['fakeid'] = fakeid
        articles = fetch_articles(gzh, page_size=10, max_pages=1000)  # 增加到1000页，爬取全部
        results = filter_and_extract(articles, gzh['name'], all_results)
        all_results = results  # 更新结果列表
    
    if all_results:
        df = pd.DataFrame(all_results)
        os.makedirs('微信公众号文章', exist_ok=True)
        
        # 保存最终结果
        output_file = '微信公众号文章/易药人违法事件_改进版.xlsx'
        df.to_excel(output_file, index=False)
        print(f"已保存 {len(all_results)} 条结果到 {output_file}")
        
        # 同时保存一个带时间戳的备份
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        backup_file = f'微信公众号文章/易药人违法事件_改进版_{timestamp}.xlsx'
        df.to_excel(backup_file, index=False)
        print(f"已保存备份到 {backup_file}")
        
        # 显示统计信息
        print(f"\n=== 数据统计 ===")
        print(f"总记录数: {len(df)}")
        print(f"有机构信息的记录: {len(df[df['机构'].notna() & (df['机构'] != '')])}")
        print(f"有省份信息的记录: {len(df[df['省份'].notna() & (df['省份'] != '')])}")
        print(f"有人名信息的记录: {len(df[df['姓名'].notna() & (df['姓名'] != '')])}")
        
        # 显示前几条记录
        print(f"\n=== 前5条记录 ===")
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