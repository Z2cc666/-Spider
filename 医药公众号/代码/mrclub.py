#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
import json
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import lru_cache
from threading import Lock
import logging

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('mrclub_spider.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# 全局锁和计数器
results_lock = Lock()
progress_lock = Lock()
processed_count = 0
matched_count = 0

# 重点关键词 - 专注违法犯罪被抓被查
VIOLATION_KEYWORDS = [
    # 违法犯罪核心词汇
    "被查", "被抓", "被捕", "被拘", "落马", "双开", "被开除", "被免职", "被撤职", "被处分",
    "涉嫌", "违法", "违纪", "犯罪", "腐败", "贪污", "受贿", "行贿", "挪用", "滥用职权",
    "主动投案", "自首", "被诉", "被判", "被立案", "被调查", "纪检监察", "监委", "纪委",
    
    # 医疗相关违法
    "医疗腐败", "医药腐败", "回扣", "红包", "骗保", "套保", "虚开发票", "药品违法",
    "医保违法", "收受贿赂", "滥开药品", "过度医疗", "医疗欺诈", "药品回扣",
    
    # 处罚措施
    "严重违纪违法", "开除党籍", "开除公职", "移送司法机关", "党纪政务处分",
    "警告", "记过", "记大过", "降级", "撤职", "留党察看", "取消资格",
    
    # 调查相关
    "纪律审查", "监察调查", "立案审查", "审查调查", "接受调查", "配合调查"
]

# MRCLUB公众号配置 - 使用最新的认证信息
GZH_LIST = [
    {
        "name": "MRCLUB",
        "token": "840103440",
        "cookie": "RK=DC2Uq4Wf9P; ptcz=c9f4dcf0c0fb279d2316b228ce1d2d7a6b107f591ae8bbce0eac0ce98bc9de36; wxuin=51340895845860; mm_lang=zh_CN; _hp2_id.1405110977=%7B%22userId%22%3A%226801447023479475%22%2C%22pageviewId%22%3A%228306667787246811%22%2C%22sessionId%22%3A%224504468753015668%22%2C%22identity%22%3Anull%2C%22trackerVersion%22%3A%224.0%22%7D; ua_id=mxGDXOVuOo8d0D2hAAAAACdqUxp53FqemlDjGf2eSLM=; rewardsn=; wxtokenkey=777; poc_sid=HBg3iGijlmGc_2ocHEPN26JgrEcR59UETkMwwy7P; _clck=3911425635|1|fy3|0; uuid=0f23747e8a4ce4803ac4c2e81813d9c3; rand_info=CAESICRG+nL2+PnQWtbjYd6JuRPOT89alJ8x3l0VMgP1oYnS; slave_bizuin=3911425635; data_bizuin=3911425635; bizuin=3911425635; data_ticket=0s37cmjlBOpA+6yyl1Vmc2ZL5TY2yMPaZb8t5y2aenlcBIOvu0qMjhWGtWAn5OiS; slave_sid=eFlBWmtDMTdWZUJGMFRpUUNyRGp6TFEyd2czYTRSa1NQY1RiWWtxQVY2dExTMDl4QVhGNHZGamZwUzJ1djJlelpnTjlTcmdsNGVFaXBWSXNnVDJvcEdQUGJoMWFQTzU1MzVlbnpXRXdLQWk5a2FQWWpwNXkyMk1sb0NDMGhPTkpjanF0N2dsVlR3b2prWkts; slave_user=gh_b3cdf815ccbf; xid=e437bfb7c69a0dd7d1b3c2fa393774e4; _clsk=1vy8tkg|1754033023118|3|1|mp.weixin.qq.com/weheat-agent/payload/record"
    }
]

# 性能配置 - 大幅提升并发性能
PERFORMANCE_CONFIG = {
    'max_workers': 12,     # 增加线程数
    'timeout': 15,         # 请求超时时间
    'page_delay': 0.3,     # 减少页面延迟
    'cache_size': 256,     # 增加缓存大小
    'batch_size': 50       # 增加批次大小以获取更多数据
}

# 创建全局会话
SESSION = requests.Session()
SESSION.headers.update({
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36'
})

@lru_cache(maxsize=PERFORMANCE_CONFIG['cache_size'])
def quick_violation_filter(title):
    """快速违法关键词过滤"""
    return any(keyword in title for keyword in VIOLATION_KEYWORDS)

def normalize_date_format(date_str):
    """标准化日期格式为 xxxx/xx/xx"""
    if not date_str:
        return ''
    
    date_str = str(date_str).strip()
    
    # 如果已经是目标格式
    if re.match(r'^\d{4}/\d{2}/\d{2}$', date_str):
        return date_str
    
    # 处理时间戳
    if isinstance(date_str, (int, float)) or date_str.isdigit():
        timestamp = int(date_str)
        return time.strftime("%Y/%m/%d", time.localtime(timestamp))
    
    # 处理各种日期格式
    date_patterns = [
        (r'(\d{4})年(\d{1,2})月(\d{1,2})日?', r'\1/\2/\3'),
        (r'(\d{4})-(\d{1,2})-(\d{1,2})', r'\1/\2/\3'),
        (r'(\d{4})\.(\d{1,2})\.(\d{1,2})', r'\1/\2/\3'),
        (r'(\d{4})/(\d{1,2})/(\d{1,2})', r'\1/\2/\3'),
    ]
    
    for pattern, replacement in date_patterns:
        match = re.search(pattern, date_str)
        if match:
            year, month, day = match.groups()
            month = month.zfill(2)
            day = day.zfill(2)
            return f"{year}/{month}/{day}"
    
    return date_str

def extract_precise_names(text):
    """精确的姓名提取逻辑 - 严格精确版"""
    names = []
    
    # 常见姓氏（保持原有）
    common_surnames = {
        '王', '李', '张', '刘', '陈', '杨', '赵', '黄', '周', '吴',
        '徐', '孙', '胡', '朱', '高', '林', '何', '郭', '马', '罗',
        '梁', '宋', '郑', '谢', '韩', '唐', '冯', '董', '萧', '程',
        '曹', '袁', '邓', '许', '傅', '沈', '曾', '彭', '吕', '苏',
        '卢', '蒋', '蔡', '贾', '丁', '魏', '薛', '叶', '阎', '余',
        '潘', '杜', '戴', '夏', '钟', '汪', '田', '姜', '范', '方',
        '石', '姚', '谭', '廖', '邹', '熊', '金', '陆', '郝', '孔',
        '白', '崔', '康', '毛', '邱', '秦', '江', '史', '顾', '侯',
        '邵', '孟', '龙', '万', '段', '钱', '汤', '尹', '黎', '易',
        '常', '武', '乔', '贺', '赖', '龚', '文', '庞', '樊', '兰',
        '殷', '施', '陶', '洪', '翟', '安', '颜', '倪', '严', '牛',
        '温', '季', '俞', '章', '鲁', '葛', '伍', '韦', '申', '尤',
        '毕', '聂', '焦', '向', '柳', '邢', '路', '岳', '齐', '梅',
        '莫', '庄', '辛', '管', '祝', '左', '涂', '谷', '祁', '时',
        '舒', '耿', '牟', '卜', '詹', '关', '苗', '凌', '费', '纪',
        '靳', '盛', '童', '欧', '甄', '项', '曲', '成', '游', '阳',
        '裴', '席', '卫', '查', '屈', '鲍', '覃', '霍', '翁', '隋',
        '甘', '景', '薄', '单', '包', '柏', '宁', '柯', '阮', '桂'
    }
    
    # 大幅扩展禁止词汇 - 严格过滤
    invalid_words = {
        # 基础禁止词
        'MRCLUB', '赛柏蓝', '医院', '委员会', '管理局', '监督局',
        '被查', '被抓', '落马', '双开', '涉嫌', '违法', '违纪', '犯罪',
        '贪污', '腐败', '受贿', '处分', '免职', '撤职', '开除',
        '调查', '审查', '立案', '起诉', '判决', '逮捕', '拘留',
        
        # 职位相关禁止词
        '董事长', '总经理', '副总', '高管', '经理', '主管', '助理',
        '原院长', '前院长', '时任', '现任', '原任', '曾任',
        '院长', '副院长', '主任', '副主任', '书记', '委员', '局长', '副局长',
        
        # 明显错误的组合词
        '高管加', '康中国', '纪违法', '卫健委', '方结果', '李健',
        '管加', '中国', '违法', '健委', '结果', '某某', '某人',
        
        # 地名和机构名
        '北京', '上海', '天津', '重庆', '安徽', '山东', '江苏', '浙江',
        '广东', '湖南', '湖北', '河南', '河北', '四川', '云南', '贵州',
        '医药', '药品', '医疗', '卫生', '健康', '保健', '药监',
        
        # 其他无效词汇
        '有关', '相关', '等人', '等等', '通报', '公布', '消息',
        '严重', '纪委', '监委', '处长', '科长', '主治', '护士',
        '药师', '技师', '检验', '影像', '临床', '门诊'
    }
    
    # 无效的结尾字符
    invalid_endings = {'被', '也', '还', '又', '就', '都', '却', '但', '而', '则', '即', '既', '已', '正', '在', '到', '从', '向', '于', '对', '为', '与', '和', '或', '及', '以', '将', '会', '要', '能', '可', '应', '该', '当', '若', '如', '因', '由', '经', '过', '通', '接', '受', '给', '让', '使', '令', '叫', '说', '讲', '告', '报', '知', '道', '解', '了', '的', '地', '得'}
    
    # 精确但不过于严格的匹配模式
    position_name_patterns = [
        # 原职位 + 姓名 + 违法关键词
        r'(原|前|时任|曾任)?\s*(主任医师|副主任医师|主治医师|院长|副院长|党委书记|纪委书记|主任|副主任|局长|副局长|处长|副处长|科长|副科长|主席|副主席|董事长|总经理|副总经理)\s*([王李张刘陈杨赵黄周吴徐孙胡朱高林何郭马罗梁宋郑谢韩唐冯董萧程曹袁邓许傅沈曾彭吕苏卢蒋蔡贾丁魏薛叶阎余潘杜戴夏钟汪田姜范方石姚谭廖邹熊金陆郝孔白崔康毛邱秦江史顾侯邵孟龙万段钱汤尹黎易常武乔贺赖龚文庞樊兰殷施陶洪翟安颜倪严牛温季俞章鲁葛伍韦申尤毕聂焦向柳邢路岳齐梅莫庄辛管祝左涂谷祁时舒耿牟卜詹关苗凌费纪靳盛童欧甄项曲成游阳裴席卫查屈鲍覃霍翁隋甘景薄单包柏宁柯阮桂][\u4e00-\u9fff]{1,2})\s*(?=被查|被抓|被双开|被免职|被撤职|被处分|涉嫌|接受.*?调查|严重违纪|严重违法)',
        
        # 姓名 + 职位 + 违法关键词  
        r'([王李张刘陈杨赵黄周吴徐孙胡朱高林何郭马罗梁宋郑谢韩唐冯董萧程曹袁邓许傅沈曾彭吕苏卢蒋蔡贾丁魏薛叶阎余潘杜戴夏钟汪田姜范方石姚谭廖邹熊金陆郝孔白崔康毛邱秦江史顾侯邵孟龙万段钱汤尹黎易常武乔贺赖龚文庞樊兰殷施陶洪翟安颜倪严牛温季俞章鲁葛伍韦申尤毕聂焦向柳邢路岳齐梅莫庄辛管祝左涂谷祁时舒耿牟卜詹关苗凌费纪靳盛童欧甄项曲成游阳裴席卫查屈鲍覃霍翁隋甘景薄单包柏宁柯阮桂][\u4e00-\u9fff]{1,2})\s*(?:，|,)?\s*(主任医师|副主任医师|主治医师|院长|副院长|党委书记|纪委书记|主任|副主任|局长|副局长|处长|副处长|科长|副科长|主席|副主席|董事长|总经理|副总经理)\s*(?=被查|被抓|被双开|被免职|被撤职|被处分|涉嫌|接受.*?调查|严重违纪|严重违法)',
        
        # 精确的上下文姓名匹配
        r'(?:违法|违纪|涉嫌|被查|被抓|被调查|接受调查)[^。]{0,20}([王李张刘陈杨赵黄周吴徐孙胡朱高林何郭马罗梁宋郑谢韩唐冯董萧程曹袁邓许傅沈曾彭吕苏卢蒋蔡贾丁魏薛叶阎余潘杜戴夏钟汪田姜范方石姚谭廖邹熊金陆郝孔白崔康毛邱秦江史顾侯邵孟龙万段钱汤尹黎易常武乔贺赖龚文庞樊兰殷施陶洪翟安颜倪严牛温季俞章鲁葛伍韦申尤毕聂焦向柳邢路岳齐梅莫庄辛管祝左涂谷祁时舒耿牟卜詹关苗凌费纪靳盛童欧甄项曲成游阳裴席卫查屈鲍覃霍翁隋甘景薄单包柏宁柯阮桂][\u4e00-\u9fff]{1,2})',
        
        # 反向匹配：姓名在违法关键词前面
        r'([王李张刘陈杨赵黄周吴徐孙胡朱高林何郭马罗梁宋郑谢韩唐冯董萧程曹袁邓许傅沈曾彭吕苏卢蒋蔡贾丁魏薛叶阎余潘杜戴夏钟汪田姜范方石姚谭廖邹熊金陆郝孔白崔康毛邱秦江史顾侯邵孟龙万段钱汤尹黎易常武乔贺赖龚文庞樊兰殷施陶洪翟安颜倪严牛温季俞章鲁葛伍韦申尤毕聂焦向柳邢路岳齐梅莫庄辛管祝左涂谷祁时舒耿牟卜詹关苗凌费纪靳盛童欧甄项曲成游阳裴席卫查屈鲍覃霍翁隋甘景薄单包柏宁柯阮桂][\u4e00-\u9fff]{1,2})[^。]{0,20}(?=被查|被抓|被双开|被免职|被撤职|被处分|涉嫌|接受.*?调查|严重违纪|严重违法)'
    ]
    
    for pattern in position_name_patterns:
        matches = re.findall(pattern, text)
        for match in matches:
            # 提取姓名部分
            name_candidate = None
            if isinstance(match, tuple):
                # 根据匹配组数确定姓名位置
                if len(match) == 4:  # 原职位 + 职位 + 姓名
                    name_candidate = match[2]
                elif len(match) == 3:  # 职位 + 姓名 或 原+职位+姓名
                    name_candidate = match[2]
                elif len(match) == 2:  # 姓名 + 职位
                    name_candidate = match[0]
                elif len(match) == 1:  # 单个姓名
                    name_candidate = match[0]
            elif isinstance(match, str):
                name_candidate = match
            
            if (name_candidate and
                2 <= len(name_candidate) <= 3 and 
                name_candidate not in invalid_words and
                name_candidate[0] in common_surnames and
                all('\u4e00' <= c <= '\u9fff' for c in name_candidate) and
                name_candidate[-1] not in invalid_endings and
                                    # 严格验证：不能包含任何职位、违法关键词（移除常见名字用字）
                    not any(bad in name_candidate for bad in ['院长', '主任', '书记', '委员', '局长', '处长', '科长', '被', '也', '接', '查', '医', '药', '管', '理', '卫']) and
                # 验证姓名的合理性
                is_valid_chinese_name(name_candidate)):
                names.append(name_candidate)
    
    # 移除所有不精确的匹配方法，只保留最可靠的
    
    # 去重并验证
    valid_names = []
    for name in set(names):
        if (len(name) >= 2 and len(name) <= 4 and
            name[0] in common_surnames and
            name not in invalid_words and
            name[-1] not in invalid_endings and
            all('\u4e00' <= c <= '\u9fff' for c in name)):
            valid_names.append(name)
    
    return valid_names

def is_valid_chinese_name(name):
    """验证是否为有效的中文姓名"""
    if not name or len(name) < 2 or len(name) > 3:
        return False
    
    # 检查是否包含明显的非姓名字符 (移除常见名字用字)
    invalid_chars = {'加', '理', '管', '卫', '医', '药', '院', '委', '会', '局', '处', '科', '法', '查', '被', '抓', '审', '调', '违', '纪', '某', '等', '之', '其', '有', '无', '此', '该'}
    if any(char in name for char in invalid_chars):
        return False
    
    # 检查是否为常见的错误组合
    error_combinations = ['高管', '管加', '方结', '结果', '康中', '中国', '纪违', '违法', '卫健', '健委', '主任', '院长', '局长', '处长', '科长', '书记', '委员', '严重']
    if any(combo in name for combo in error_combinations):
        return False
    
    # 特殊检查：包含"某"的一律过滤
    if '某' in name:
        return False
    
    return True

def extract_comprehensive_institutions(text):
    """精确的机构提取 - 严格边界识别"""
    institutions = []
    
    # 使用更精确的边界匹配
    precise_patterns = [
        # 完整的政府机构名称（从边界开始匹配）
        r'(?:^|[。！？；\n，,、])\s*([A-Z]*[\u4e00-\u9fff]{2,8}(?:省|市|县|区)[\u4e00-\u9fff]{2,15}(?:委员会|管理局|监督局|药监局|医保局|卫健委|纪委监委|卫生局|卫生厅|人民政府))',
        
        # 完整的医院名称
        r'(?:^|[。！？；\n，,、])\s*([\u4e00-\u9fff]{2,15}(?:人民医院|中心医院|中医医?院|第[一二三四五六七八九十\d]+医院|妇幼保健院|儿童医院|肿瘤医院|专科医院|总医院))',
        
        # 大学附属医院
        r'(?:^|[。！？；\n，,、])\s*([\u4e00-\u9fff]{2,15}(?:医科大学|医学院|大学)附属[\u4e00-\u9fff]{0,10}医院)',
        
        # 国家级机构
        r'(?:^|[。！？；\n，,、])\s*(国家[\u4e00-\u9fff]{2,15}(?:委员会|管理局|监督局|药监局|医保局|卫健委))',
    ]
    
    # 严格的排除词汇
    strict_invalid_keywords = [
        'MRCLUB', '赛柏蓝', '被查', '被抓', '落马', '双开', '涉嫌',
        '违法', '违纪', '犯罪', '贪污', '腐败', '受贿', '处分',
        '调查', '审查', '立案', '起诉', '判决', '逮捕', '拘留',
        '某某', '有关', '相关', '等等', '时任', '原任', '曾任', '前任',
        '月我在', '重医生', '我们主任', '市医副', '市药', '任山东'
    ]
    
    for pattern in precise_patterns:
        matches = re.findall(pattern, text)
        for institution in matches:
            # 清理机构名称
            institution = institution.strip()
            
            # 验证机构名称的有效性
            if (4 <= len(institution) <= 30 and
                not any(invalid in institution for invalid in strict_invalid_keywords) and
                all('\u4e00' <= c <= '\u9fff' or c in '()（）第一二三四五六七八九十0123456789' for c in institution) and
                # 必须以有效的机构类型结尾
                any(institution.endswith(suffix) for suffix in ['医院', '委员会', '管理局', '监督局', '药监局', '医保局', '卫健委', '纪委监委', '卫生局', '卫生厅', '人民政府']) and
                # 不能包含明显的错误模式
                not any(error in institution for error in ['副局长', '主任主', '院长院', '被查', '违法'])):
                institutions.append(institution)
    
    if institutions:
        # 返回最长最完整的机构名称
        best_institution = max(institutions, key=lambda x: (
            # 优先级：政府机构 > 医院 > 其他
            100 if any(keyword in x for keyword in ['委员会', '管理局', '监督局', '药监局', '医保局', '卫健委', '纪委监委']) else
            50 if '医院' in x else 10,
            # 长度奖励
            len(x)
        ))
        return best_institution
    
    return ""

def extract_comprehensive_positions(text, person_name=""):
    """全面的职位提取"""
    if not text:
        return ""
    
    # 医疗职位
    medical_positions = [
        '主任医师', '副主任医师', '主治医师', '住院医师', '医师', '医生',
        '护士长', '主管护师', '护师', '护士',
        '药师', '主管药师', '药剂师', '临床药师',
        '技师', '主管技师', '检验师', '影像师'
    ]
    
    # 行政职位
    admin_positions = [
        '院长', '副院长', '党委书记', '纪委书记', '院长助理',
        '主任', '副主任', '主任委员', '委员',
        '局长', '副局长', '厅长', '副厅长',
        '处长', '副处长', '科长', '副科长',
        '部长', '副部长', '司长', '副司长',
        '主席', '副主席', '秘书长', '副秘书长'
    ]
    
    # 企业职位
    business_positions = [
        '董事长', '副董事长', '总经理', '副总经理',
        '总裁', '副总裁', '总监', '副总监',
        '经理', '副经理', '主管', '总助'
    ]
    
    # 合并所有职位
    all_positions = medical_positions + admin_positions + business_positions
    
    # 如果指定了人名，优先在人名附近查找
    if person_name:
        context_sentences = []
        sentences = re.split(r'[。！？；\n]', text)
        for sentence in sentences:
            if person_name in sentence:
                context_sentences.append(sentence)
        
        if context_sentences:
            context_text = '。'.join(context_sentences)
            # 在上下文中查找职位
            for position in all_positions:
                if position in context_text:
                    return position
    
    # 全文查找职位
    found_positions = []
    for position in all_positions:
        if position in text:
            found_positions.append(position)
    
    if found_positions:
        # 按优先级排序：医疗职位 > 行政职位 > 企业职位
        for position_list in [medical_positions, admin_positions, business_positions]:
            for position in found_positions:
                if position in position_list:
                    return position
    
    return ""

def extract_province(text):
    """提取省份信息"""
    provinces = [
        '北京', '天津', '上海', '重庆', '河北', '山西', '辽宁', '吉林', '黑龙江',
        '江苏', '浙江', '安徽', '福建', '江西', '山东', '河南', '湖北', '湖南',
        '广东', '海南', '四川', '贵州', '云南', '陕西', '甘肃', '青海', '台湾',
        '内蒙古', '广西', '西藏', '宁夏', '新疆', '香港', '澳门'
    ]
    
    for province in provinces:
        if province in text:
            return province
    
    return ""

def clean_and_deduplicate_results(results):
    """清洗和去重违法事件数据"""
    if not results:
        return []
    
    cleaned_results = []
    seen_combinations = set()
    
    for result in results:
        # 数据清洗
        cleaned_result = {}
        for key, value in result.items():
            # 清理字符串值
            if isinstance(value, str):
                value = value.strip()
                # 移除多余的空白字符
                value = re.sub(r'\s+', ' ', value)
                # 移除特殊字符
                value = re.sub(r'[^\u4e00-\u9fff\w\s\-.,()（）：:；;！!？?。、/]', '', value)
            cleaned_result[key] = value
        
        # 去重检查 - 使用多个字段组合
        unique_key = f"{cleaned_result.get('姓名', '')}_{cleaned_result.get('机构', '')}_{cleaned_result.get('职位', '')}_{cleaned_result.get('Description', '')[:100]}"
        
        # 额外的相似性检查
        is_duplicate = False
        for seen_key in seen_combinations:
            # 检查姓名和描述是否高度相似
            if (cleaned_result.get('姓名') and 
                cleaned_result.get('姓名') in seen_key and 
                len(cleaned_result.get('Description', '')) > 20):
                # 计算描述相似度
                desc1 = cleaned_result.get('Description', '')[:200]
                desc2 = [k for k in seen_combinations if cleaned_result.get('姓名') in k]
                if desc2:
                    desc2 = desc2[0].split('_')[-1] if '_' in desc2[0] else ''
                    if desc1 and desc2:
                        # 简单的重叠检查
                        overlap = len(set(desc1) & set(desc2)) / max(len(set(desc1)), len(set(desc2)), 1)
                        if overlap > 0.7:  # 70%重叠认为是重复
                            is_duplicate = True
                            break
        
        if not is_duplicate and unique_key not in seen_combinations:
            # 验证数据质量
            if (cleaned_result.get('姓名') or 
                (cleaned_result.get('机构') and len(cleaned_result.get('机构', '')) > 3) or
                len(cleaned_result.get('Description', '')) > 30):
                
                seen_combinations.add(unique_key)
                cleaned_results.append(cleaned_result)
    
    logging.info(f"数据清洗完成：原始 {len(results)} 条 -> 清洗后 {len(cleaned_results)} 条")
    return cleaned_results

def extract_violation_description(text, person_name=""):
    """提取违法事件描述"""
    sentences = re.split(r'[。！？；\n]', text)
    relevant_sentences = []
    
    for sentence in sentences:
        sentence = sentence.strip()
        if len(sentence) < 15 or len(sentence) > 300:
            continue
        
        # 必须包含违法关键词
        has_violation_keyword = any(keyword in sentence for keyword in VIOLATION_KEYWORDS)
        
        # 如果指定了人名，优先选择包含该人名的句子
        has_person_name = not person_name or person_name in sentence
        
        # 排除无意义的句子
        invalid_sentence_patterns = [
            r'^\s*$',  # 空句子
            r'^.*编辑.*$', r'^.*作者.*$', r'^.*来源.*$', r'^.*转载.*$',
            r'^.*关注.*$', r'^.*点击.*$', r'^.*扫码.*$', r'^.*微信.*$',
            r'^.*声明.*$', r'^.*免责.*$', r'^.*版权.*$'
        ]
        
        is_invalid_sentence = any(re.match(pattern, sentence) for pattern in invalid_sentence_patterns)
        
        if has_violation_keyword and has_person_name and not is_invalid_sentence:
            # 计算违法关键词密度
            keyword_count = sum(1 for keyword in VIOLATION_KEYWORDS if keyword in sentence)
            
            # 额外加分：包含具体违法行为描述
            specific_violation_keywords = [
                '利用职务便利', '收受财物', '非法收受', '索要财物', '挪用资金',
                '滥用职权', '玩忽职守', '徇私舞弊', '以权谋私', '权钱交易',
                '医疗腐败', '药品回扣', '医保诈骗', '虚开发票', '套取资金'
            ]
            specific_score = sum(1 for keyword in specific_violation_keywords if keyword in sentence)
            
            relevant_sentences.append({
                'sentence': sentence,
                'keyword_count': keyword_count,
                'specific_score': specific_score,
                'length': len(sentence)
            })
    
    if relevant_sentences:
        # 选择最佳句子：优先考虑具体违法行为，然后是关键词数量，最后是长度
        best_sentence = max(relevant_sentences, 
                          key=lambda s: (s['specific_score'], s['keyword_count'], min(s['length'], 200)))
        return best_sentence['sentence']
    
    # 如果没有找到包含人名的句子，尝试找到包含违法关键词的句子
    if person_name:
        for sentence in sentences:
            sentence = sentence.strip()
            if (15 <= len(sentence) <= 300 and 
                any(keyword in sentence for keyword in VIOLATION_KEYWORDS)):
                return sentence
    
    return ""

def get_article_content_optimized(link, session=None):
    """优化的文章内容获取"""
    if session is None:
        session = SESSION
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            # 添加随机延迟避免被限制
            time.sleep(random.uniform(0.5, 1.5))
            
            headers = {
                'referer': 'https://mp.weixin.qq.com/',
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'accept-language': 'zh-CN,zh;q=0.9',
                'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36'
            }
            
            resp = session.get(link, headers=headers, timeout=PERFORMANCE_CONFIG['timeout'])
            
            if resp.status_code != 200:
                if attempt < max_retries - 1:
                    time.sleep(random.uniform(1, 3))
                    continue
                return ''
            
            soup = BeautifulSoup(resp.text, 'html.parser')
            
            # 多种方式获取内容
            content_selectors = [
                {'id': 'js_content'},
                {'class_': 'rich_media_content'},
                {'class_': 'content'},
                {'tag': 'article'}
            ]
            
            content_div = None
            for selector in content_selectors:
                if 'id' in selector:
                    content_div = soup.find('div', {'id': selector['id']})
                elif 'class_' in selector:
                    content_div = soup.find('div', class_=selector['class_'])
                elif 'tag' in selector:
                    content_div = soup.find(selector['tag'])
                
                if content_div:
                    break
            
            if content_div:
                # 移除脚本和样式
                for script in content_div(["script", "style"]):
                    script.decompose()
                
                text = content_div.get_text(separator='\n', strip=True)
                return text
            
            return ''
            
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(random.uniform(2, 5))
                continue
            return ''
    
    return ''

def process_single_article_optimized(article_data):
    """优化的单篇文章处理 - 移除标题预筛选，获取所有文章内容"""
    global processed_count, matched_count
    
    try:
        article, gzh_name, create_time = article_data
        title = article.get("title", "")
        link = article.get("link", "")
        
        # 获取文章内容
        content = get_article_content_optimized(link)
        if not content or len(content) < 20:  # 降低内容长度限制
            with progress_lock:
                processed_count += 1
            return None
        
        # 只进行内容筛选，不进行标题预筛选
        if not any(keyword in content for keyword in VIOLATION_KEYWORDS):
            with progress_lock:
                processed_count += 1
            return None
        
        with progress_lock:
            processed_count += 1
            matched_count += 1
            if processed_count % 50 == 0:  # 增加统计频率
                logging.info(f"已处理 {processed_count} 篇，匹配 {matched_count} 篇")
        
        return {
            'title': title,
            'link': link,
            'content': content,
            'create_time': create_time
        }
        
    except Exception as e:
        with progress_lock:
            processed_count += 1
        return None

def extract_violation_events(article_info, gzh_name):
    """从文章中提取违法事件信息"""
    title = article_info['title']
    link = article_info['link']
    content = article_info['content']
    create_time = article_info['create_time']
    
    # 标准化发布时间
    formatted_time = normalize_date_format(create_time)
    
    # 提取基本信息
    names = extract_precise_names(content)
    province = extract_province(content)
    institution = extract_comprehensive_institutions(content)
    
    results = []
    
    if names:
        # 为每个人名创建记录
        for name in names:
            position = extract_comprehensive_positions(content, name)
            description = extract_violation_description(content, name)
            
            if description:  # 只有有描述的才记录
                result = {
                    "发布时间": formatted_time,
                    "省份": province,
                    "机构": institution,
                    "职位": position,
                    "姓名": name,
                    "Resource": gzh_name,
                    "Description": description,
                    "文章链接": link,
                    "标题": title
                }
                results.append(result)
    else:
        # 即使没有人名，如果有违法事件描述也记录
        description = extract_violation_description(content)
        if description:
            position = extract_comprehensive_positions(content)
            result = {
                "发布时间": formatted_time,
                "省份": province,
                "机构": institution,
                "职位": position,
                "姓名": "",
                "Resource": gzh_name,
                "Description": description,
                "文章链接": link,
                "标题": title
            }
            results.append(result)
    
    return results

def fetch_articles_batch_optimized(gzh, start_page=0, batch_size=30, page_size=10):
    """优化的批量文章获取"""
    batch_articles = []
    consecutive_empty_pages = 0
    max_consecutive_empty = 10  # 增加连续空页容忍度
    
    end_page = start_page + batch_size
    logging.info(f"开始抓取第{start_page+1}页到第{end_page}页...")
    
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
            'fingerprint': 'a1075c4cfacf4c13a46dad10285e6122',
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
            'x-requested-with': 'XMLHttpRequest'
        }
        
        try:
            resp = SESSION.get("https://mp.weixin.qq.com/cgi-bin/appmsgpublish", 
                              params=params, headers=headers, 
                              timeout=PERFORMANCE_CONFIG['timeout'])
            
            if resp.status_code != 200:
                logging.warning(f"第{page+1}页请求失败，状态码: {resp.status_code}")
                consecutive_empty_pages += 1
                if consecutive_empty_pages >= max_consecutive_empty:
                    break
                continue
            
            result = resp.json()
            if result.get("base_resp", {}).get("ret") != 0:
                logging.warning(f"第{page+1}页API返回错误: {result.get('base_resp', {})}")
                consecutive_empty_pages += 1
                if consecutive_empty_pages >= max_consecutive_empty:
                    break
                continue
            
            publish_page = result.get("publish_page")
            if not publish_page:
                consecutive_empty_pages += 1
                if consecutive_empty_pages >= max_consecutive_empty:
                    break
                continue
            
            articleList = json.loads(publish_page)
            articles = articleList.get("publish_list", [])
            
            if not articles:
                consecutive_empty_pages += 1
                if consecutive_empty_pages >= max_consecutive_empty:
                    break
                continue
            
            consecutive_empty_pages = 0
            batch_articles.extend(articles)
            logging.info(f"第{page+1}页获取到{len(articles)}篇文章")
            
            time.sleep(PERFORMANCE_CONFIG['page_delay'])
            
        except Exception as e:
            logging.error(f"第{page+1}页抓取异常: {e}")
            consecutive_empty_pages += 1
            if consecutive_empty_pages >= max_consecutive_empty:
                break
            time.sleep(2)
    
    logging.info(f"本批次获取到 {len(batch_articles)} 篇文章")
    return batch_articles, len(batch_articles) > 0

def filter_and_extract_optimized(articles, gzh_name, existing_results=None):
    """优化的文章筛选和信息提取 - 处理所有文章"""
    global processed_count, matched_count
    processed_count = 0
    matched_count = 0
    
    results = existing_results or []
    
    # 准备所有文章任务，不进行标题预筛选
    article_tasks = []
    for article in articles:
        try:
            info = json.loads(article.get("publish_info", "{}"))
            if info.get("appmsgex"):
                title = info["appmsgex"][0].get("title", "")
                link = info["appmsgex"][0].get("link", "")
                create_time = info["appmsgex"][0].get("create_time")
                
                # 移除标题预筛选，处理所有文章
                article_tasks.append((
                    {"title": title, "link": link}, 
                    gzh_name, 
                    create_time
                ))
        except Exception:
            continue
    
    logging.info(f"共 {len(article_tasks)} 篇文章需要处理（已移除标题预筛选）")
    
    # 并发处理文章
    matched_articles = []
    
    with ThreadPoolExecutor(max_workers=PERFORMANCE_CONFIG['max_workers']) as executor:
        future_to_article = {
            executor.submit(process_single_article_optimized, task): task 
            for task in article_tasks
        }
        
        for future in as_completed(future_to_article):
            try:
                result = future.result()
                if result:
                    matched_articles.append(result)
            except Exception:
                continue
    
    logging.info(f"内容筛选后匹配到 {len(matched_articles)} 篇相关文章")
    
    # 串行提取信息避免数据竞争
    batch_violations = []
    
    for article_info in matched_articles:
        try:
            violations = extract_violation_events(article_info, gzh_name)
            batch_violations.extend(violations)
        except Exception as e:
            logging.error(f"信息提取失败: {e}")
    
    # 对本批次数据进行清洗和去重
    if batch_violations:
        cleaned_batch = clean_and_deduplicate_results(batch_violations)
        results.extend(cleaned_batch)
    
    # 对全量数据进行最终去重
    if len(results) > 100:  # 当数据量较大时进行全量去重
        results = clean_and_deduplicate_results(results)
    
    new_records = len(batch_violations) if batch_violations else 0
    final_records = len(cleaned_batch) if 'cleaned_batch' in locals() else 0
    logging.info(f"本批次提取 {new_records} 条 -> 清洗后 {final_records} 条，累计 {len(results)} 条")
    
    return results

def get_fakeid(gzh_name, token, cookie):
    """获取公众号fakeid"""
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
            logging.error(f"未找到公众号 {gzh_name} 的fakeid")
            return None
    except Exception as e:
        logging.error(f"获取fakeid失败: {e}")
        return None

def save_progress_optimized(current_page, gzh_name, all_results):
    """保存进度和数据"""
    os.makedirs('微信公众号文章', exist_ok=True)
    
    # 保存进度
    progress_info = {
        'last_page': current_page,
        'gzh_name': gzh_name,
        'total_records': len(all_results),
        'last_update': time.strftime("%Y-%m-%d %H:%M:%S")
    }
    
    progress_file = '微信公众号文章/MRCLUB_爬取进度.json'
    with open(progress_file, 'w', encoding='utf-8') as f:
        json.dump(progress_info, f, ensure_ascii=False, indent=2)
    
    # 保存数据
    if all_results:
        df = pd.DataFrame(all_results)
        output_file = '微信公众号文章/MRCLUB违法事件_优化版.xlsx'
        df.to_excel(output_file, index=False)
        logging.info(f"已保存 {len(all_results)} 条记录到 {output_file}")

def load_progress_optimized():
    """加载进度"""
    progress_file = '微信公众号文章/MRCLUB_爬取进度.json'
    data_file = '微信公众号文章/MRCLUB违法事件_优化版.xlsx'
    
    start_page = 0
    existing_results = []
    
    if os.path.exists(progress_file):
        try:
            with open(progress_file, 'r', encoding='utf-8') as f:
                progress_info = json.load(f)
            start_page = progress_info.get('last_page', 0)
            logging.info(f"发现进度文件：上次爬取到第{start_page}页")
        except Exception as e:
            logging.error(f"读取进度文件失败: {e}")
    
    if os.path.exists(data_file):
        try:
            existing_df = pd.read_excel(data_file)
            existing_results = existing_df.to_dict('records')
            logging.info(f"发现已有数据文件，包含 {len(existing_results)} 条记录")
        except Exception as e:
            logging.error(f"读取数据文件失败: {e}")
    
    return start_page, existing_results

def main():
    """主函数"""
    logging.info("🚀 启动MRCLUB违法事件爬虫优化版")
    
    # 加载进度
    start_page, all_results = load_progress_optimized()
    
    if start_page > 0 or all_results:
        print(f"\n📋 发现历史数据:")
        print(f"   上次爬取到第{start_page}页")
        print(f"   已有{len(all_results)}条记录")
        response = input("是否继续上次进度？[y/n]: ").lower()
        
        if response != 'y':
            start_page = 0
            all_results = []
            logging.info("重新开始爬取")
        else:
            logging.info(f"从第{start_page + 1}页继续爬取")
    
    for gzh in GZH_LIST:
        logging.info(f"🎯 开始处理公众号: {gzh['name']}")
        
        # 获取fakeid
        fakeid = get_fakeid(gzh['name'], gzh['token'], gzh['cookie'])
        if not fakeid:
            continue
        gzh['fakeid'] = fakeid
        
        current_page = start_page
        max_pages = 1000
        batch_size = PERFORMANCE_CONFIG['batch_size']
        
        try:
            while current_page < max_pages:
                batch_num = current_page // batch_size + 1
                logging.info(f"📦 开始处理第{batch_num}批次 (第{current_page+1}-{min(current_page+batch_size, max_pages)}页)")
                
                # 抓取文章
                batch_articles, success = fetch_articles_batch_optimized(
                    gzh, current_page, batch_size, page_size=10
                )
                
                if not success or not batch_articles:
                    logging.warning(f"第{batch_num}批次抓取失败，停止爬取")
                    break
                
                # 筛选和提取
                all_results = filter_and_extract_optimized(
                    batch_articles, gzh['name'], all_results
                )
                
                # 计算进度百分比和预估剩余时间
                progress_percent = (current_page + batch_size) / max_pages * 100
                if current_page > 0:
                    avg_time_per_batch = (time.time() - batch_start_time) if 'batch_start_time' in locals() else 3
                    remaining_batches = (max_pages - current_page - batch_size) // batch_size
                    estimated_time = remaining_batches * avg_time_per_batch / 60  # 转换为分钟
                    
                    logging.info(f"✅ 第{batch_num}批次完成，累计 {len(all_results)} 条记录")
                    logging.info(f"📈 进度: {progress_percent:.1f}% | 预估剩余: {estimated_time:.1f}分钟")
                else:
                    logging.info(f"✅ 第{batch_num}批次完成，累计 {len(all_results)} 条记录")
                
                # 移动到下一批次
                current_page += batch_size
                batch_start_time = time.time()  # 记录批次开始时间
                
                # 保存进度
                save_progress_optimized(current_page, gzh['name'], all_results)
                
                # 显示中期统计
                if len(all_results) > 0 and len(all_results) % 50 == 0:
                    temp_df = pd.DataFrame(all_results)
                    name_ratio = len(temp_df[temp_df['姓名'] != '']) / len(temp_df) * 100
                    logging.info(f"📊 中期统计: 有姓名记录占比 {name_ratio:.1f}%")
                
                # 批次间休息
                if current_page < max_pages:
                    time.sleep(3)
        
        except KeyboardInterrupt:
            logging.info("⏹️ 用户中断，保存进度...")
            save_progress_optimized(current_page, gzh['name'], all_results)
            return
        except Exception as e:
            logging.error(f"❌ 爬取异常: {e}")
            save_progress_optimized(current_page, gzh['name'], all_results)
            return
    
    # 最终统计和数据清洗
    if all_results:
        # 进行最终的全量数据清洗
        logging.info("🧹 开始最终数据清洗...")
        all_results = clean_and_deduplicate_results(all_results)
        
        save_progress_optimized(current_page, gzh['name'], all_results)
        
        df = pd.DataFrame(all_results)
        
        # 详细统计信息
        total_records = len(df)
        has_name = len(df[df['姓名'].notna() & (df['姓名'] != '')])
        has_institution = len(df[df['机构'].notna() & (df['机构'] != '')])
        has_position = len(df[df['职位'].notna() & (df['职位'] != '')])
        has_province = len(df[df['省份'].notna() & (df['省份'] != '')])
        
        logging.info(f"🎉 爬取完成！总计 {total_records} 条有效记录")
        logging.info(f"📊 数据完整性统计:")
        logging.info(f"   有姓名: {has_name} 条 ({has_name/total_records*100:.1f}%)")
        logging.info(f"   有机构: {has_institution} 条 ({has_institution/total_records*100:.1f}%)")
        logging.info(f"   有职位: {has_position} 条 ({has_position/total_records*100:.1f}%)")
        logging.info(f"   有省份: {has_province} 条 ({has_province/total_records*100:.1f}%)")
        
        # 按省份统计
        if has_province > 0:
            province_stats = df[df['省份'] != '']['省份'].value_counts().head(10)
            logging.info(f"🌍 top10省份分布:")
            for province, count in province_stats.items():
                logging.info(f"   {province}: {count} 条")
        
        # 按机构类型统计
        if has_institution > 0:
            hospital_count = len(df[df['机构'].str.contains('医院', na=False)])
            committee_count = len(df[df['机构'].str.contains('委员会|管理局|监督局', na=False)])
            logging.info(f"🏥 机构类型分布:")
            logging.info(f"   医院类: {hospital_count} 条")
            logging.info(f"   政府机构: {committee_count} 条")
            logging.info(f"   其他机构: {has_institution - hospital_count - committee_count} 条")
        
        # 显示数据示例
        print(f"\n📋 数据示例（前10条）:")
        sample_cols = ['发布时间', '姓名', '机构', '职位', '省份']
        available_cols = [col for col in sample_cols if col in df.columns]
        print(df[available_cols].head(10).to_string(index=False))
        
        # 保存详细统计报告
        stats_report = {
            '爬取时间': time.strftime("%Y-%m-%d %H:%M:%S"),
            '总记录数': total_records,
            '有姓名记录数': has_name,
            '有机构记录数': has_institution,
            '有职位记录数': has_position,
            '有省份记录数': has_province,
            '数据完整率': f"{(has_name + has_institution + has_position) / (total_records * 3) * 100:.1f}%"
        }
        
        os.makedirs('微信公众号文章', exist_ok=True)
        stats_file = '微信公众号文章/爬取统计报告.json'
        with open(stats_file, 'w', encoding='utf-8') as f:
            json.dump(stats_report, f, ensure_ascii=False, indent=2)
        
        logging.info(f"📈 统计报告已保存到: {stats_file}")
        
    else:
        logging.info("❌ 无有效数据，请检查关键词匹配或网络连接")

if __name__ == "__main__":
    main()