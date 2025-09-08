#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Vigor (丰炜科技) 档案下载爬虫
网站：https://www.vigorplc.com/zh-cn/download.html
时间筛选：只下载2024年11月之后的文件
"""

import os
import sys
import time
import requests
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
import logging
from pathlib import Path
import re
from typing import Dict, List, Tuple, Optional
import json
import pickle
import hashlib
import hmac
import base64
import urllib.parse
from datetime import datetime, date
import dateutil.parser

class VigorSpider:
    def __init__(self, base_dir: str = None, monitor_mode: bool = False):
        """
        初始化爬虫
        
        Args:
            base_dir: 下载文件保存的基础目录
            monitor_mode: 监控模式，只检测新文件而不下载
        """
        if base_dir is None:
            # 默认使用服务器目录
            base_dir = "/srv/downloads/approved/丰炜"
        
        self.base_url = "https://www.vigorplc.com"
        self.base_dir = Path(base_dir)
        self.session = requests.Session()
        self.monitor_mode = monitor_mode
        
        # 设置请求头
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
        })
        
        # 创建基础目录
        self.base_dir.mkdir(parents=True, exist_ok=True)
        
        # 设置日志
        self.setup_logging()
        
        # 钉钉通知配置
        self.ACCESS_TOKEN = "1a431b6482e59ec652a6eab37705d50fd282dc426f0b829b3eb9a9c1b277ed24"
        self.SECRET = "SECc5e2168011dd97d4c511e06832d172f31635ef635771668e4e6003fce23c07bb"
        
        # 已处理URL记录
        self.processed_urls = self.load_processed_urls()
        self.new_files = []  # 新增文件列表
        
        # 文件历史状态跟踪
        self.file_history = self.load_file_history()
        self.detected_new_files = []  # 新检测到的文件
        self.detected_updated_files = []  # 检测到更新的文件
        
        # 文件分类规则
        self.file_classification_rules = {
            '端子尺寸': '端子尺寸图',
            '通讯协议': '通讯协议驱动程序',
            '驱动程序': '通讯协议驱动程序',
            'CE证书': 'CE证书',
            '型录': '型录',
            'Cata': '型录',
            '编程软件': '软件',
            'LadderMaster': '软件',
            'simple': '软件'
        }
        
        # 判断是否为首次运行
        self.is_first_run = not (self.base_dir / 'processed_urls.pkl').exists()
        
        # 时间过滤：只下载2024年11月之后的文件
        self.cutoff_date = date(2024, 11, 1)
        
        # 下载统计
        self.stats = {
            'total_files': 0,
            'downloaded_files': 0,
            'failed_files': 0,
            'skipped_files': 0,
            'filtered_by_date': 0
        }
        
        # 档案下载模块配置 - 基于实际发现的URL结构
        self.modules = {
            '软件': {
                'has_subcategories': True,
                'subcategories': {
                    'VS/VBS系列程序编程软件': '/zh-cn/download-c5023/VS-VBS系列程序编程软件.html',
                    'VB/VH系列程序编程软件': '/zh-cn/download-c5024/VB-VH系列程序编程软件.html'
                },
                'folder_name': '软件',
                'main_url': '/zh-cn/download-c5013/软件.html'
            },
            '型录': {
                'has_subcategories': False,
                'url': '/zh-cn/download-c5012/型录.html',
                'folder_name': '型录'
            },
            '手册/说明书': {
                'has_subcategories': True,
                'subcategories': {
                    'VS系列': '/zh-cn/download-c5036/VS系列.html',
                    'VBS系列': '/zh-cn/download-c18142/VBS系列.html',
                    'VB/VH系列': '/zh-cn/download-c5037/VB-VH系列.html'
                },
                'folder_name': '手册说明书',
                'main_url': '/zh-cn/download-c5017/手册-说明书.html'
            },
            '通讯协议 / 驱动程序': {
                'has_subcategories': True,
                'subcategories': {
                    'VS系列通讯协议': '/zh-cn/download-c5028/VS系列通讯协议.html',
                    'VB/VH系列通讯协议': '/zh-cn/download-c5029/VB-VH系列通讯协议.html',
                    'VS驱动程序手动安装说明': '/zh-cn/download-c16442/VS驱动程序手动安装说明.html',
                    'VB/VH驱动程序': '/zh-cn/download-c16438/VB-VH驱动程序.html'
                },
                'folder_name': '通讯协议驱动程序',
                'main_url': '/zh-cn/download-c5014/通讯协议-驱动程序.html'
            },
            '端子尺寸图': {
                'has_subcategories': False,
                'url': '/zh-cn/download-c5015/端子尺寸图.html',
                'folder_name': '端子尺寸图'
            },
            'CE证书': {
                'has_subcategories': True,
                'subcategories': {
                    'VS系列CE证书': '/zh-cn/download-c5032/VS系列CE证书.html',
                    'VBS系列CE证书': '/zh-cn/download-c18230/VBS系列CE证书.html',
                    'VH系列CE证书': '/zh-cn/download-c5034/VH系列CE证书.html',
                    'VB系列CE证书': '/zh-cn/download-c5033/VB系列CE证书.html'
                },
                'folder_name': 'CE证书',
                'main_url': '/zh-cn/download-c5016/CE证书.html'
            }
        }

    def setup_logging(self):
        """设置日志记录"""
        log_file = self.base_dir / 'spider.log'
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def load_processed_urls(self):
        """加载已处理的URL"""
        urls_file = self.base_dir / 'processed_urls.pkl'
        if urls_file.exists():
            try:
                with open(urls_file, 'rb') as f:
                    return pickle.load(f)
            except:
                return set()
        return set()
        
    def save_processed_urls(self):
        """保存已处理的URL"""
        urls_file = self.base_dir / 'processed_urls.pkl'
        with open(urls_file, 'wb') as f:
            pickle.dump(self.processed_urls, f)

    def load_file_history(self):
        """加载文件历史记录"""
        history_file = self.base_dir / 'file_history.json'
        if history_file.exists():
            try:
                with open(history_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                self.logger.warning(f"加载文件历史记录失败: {e}")
        return {}

    def save_file_history(self):
        """保存文件历史记录"""
        history_file = self.base_dir / 'file_history.json'
        history_file.parent.mkdir(parents=True, exist_ok=True)
        try:
            with open(history_file, 'w', encoding='utf-8') as f:
                json.dump(self.file_history, f, ensure_ascii=False, indent=2)
        except Exception as e:
            self.logger.error(f"保存文件历史记录失败: {e}")

    def check_file_changes(self, file_info):
        """检查文件是否有变化"""
        file_key = f"{file_info['module']}_{file_info['title']}"
        
        if file_key not in self.file_history:
            # 新文件
            self.detected_new_files.append(file_info)
            self.file_history[file_key] = {
                'title': file_info['title'],
                'module': file_info['module'],
                'url': file_info['url'],
                'size': file_info.get('size', 0),
                'date': file_info.get('date', ''),
                'first_detected': datetime.now().isoformat(),
                'last_checked': datetime.now().isoformat(),
                'status': 'new'
            }
            return 'new'
        else:
            # 检查是否有更新
            existing = self.file_history[file_key]
            has_changes = False
            
            if existing.get('size', 0) != file_info.get('size', 0):
                has_changes = True
            if existing.get('date', '') != file_info.get('date', ''):
                has_changes = True
                
            # 更新检查时间
            existing['last_checked'] = datetime.now().isoformat()
            
            if has_changes:
                existing['size'] = file_info.get('size', 0)
                existing['date'] = file_info.get('date', '')
                existing['status'] = 'updated'
                existing['last_updated'] = datetime.now().isoformat()
                self.detected_updated_files.append(file_info)
                return 'updated'
            else:
                existing['status'] = 'unchanged'
                return 'unchanged'
    
    def classify_file_by_name(self, filename: str, title: str = "") -> str:
        """
        根据文件名和标题智能分类文件
        
        Args:
            filename: 文件名
            title: 文件标题
            
        Returns:
            分类目录名
        """
        text_to_check = f"{filename} {title}".lower()
        
        for keyword, category in self.file_classification_rules.items():
            if keyword.lower() in text_to_check:
                return category
        
        # 默认返回None，让调用者决定
        return None

    def safe_request(self, url: str, timeout: int = 30) -> Optional[requests.Response]:
        """
        安全的HTTP请求
        
        Args:
            url: 请求的URL
            timeout: 超时时间
            
        Returns:
            Response对象或None
        """
        try:
            self.logger.info(f"请求URL: {url}")
            response = self.session.get(url, timeout=timeout, verify=False)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            self.logger.error(f"请求失败 {url}: {e}")
            return None

    def parse_date_from_text(self, text: str) -> Optional[date]:
        """
        从文本中解析日期（改进版，避免误判）
        
        Args:
            text: 包含日期的文本
            
        Returns:
            解析到的日期或None
        """
        if not text:
            return None
        
        # 常见的日期格式（优先使用正则表达式，更准确）
        date_patterns = [
            r'(\d{4})/(\d{1,2})/(\d{1,2})',  # 2024/11/23
            r'(\d{4})-(\d{1,2})-(\d{1,2})',  # 2024-11-23
            r'(\d{4})\.(\d{1,2})\.(\d{1,2})', # 2024.11.23
            r'(\d{4})年(\d{1,2})月(\d{1,2})日', # 2024年11月23日
            r'(\d{4})年(\d{1,2})月',          # 2024年11月
        ]
        
        for pattern in date_patterns:
            match = re.search(pattern, text)
            if match:
                try:
                    year = int(match.group(1))
                    month = int(match.group(2))
                    day = int(match.group(3)) if len(match.groups()) >= 3 else 1
                    # 验证日期的合理性
                    if 1900 <= year <= 2030 and 1 <= month <= 12 and 1 <= day <= 31:
                        return date(year, month, day)
                except (ValueError, IndexError):
                    continue
        
        # 只有在文本明确包含数字日期格式时才使用dateutil
        # 避免从"VS系列6页简易型录"这样的文本中误解析日期
        if re.search(r'\d{4}[\-/\.年]\d{1,2}[\-/\.月]', text):
            try:
                parsed_date = dateutil.parser.parse(text, fuzzy=True)
                # 验证解析结果的合理性
                result_date = parsed_date.date()
                if 1900 <= result_date.year <= 2030:
                    return result_date
            except:
                pass
        
        return None

    def is_file_recent(self, file_info: Dict) -> bool:
        """
        检查文件是否为2024年11月之后的文件
        
        Args:
            file_info: 文件信息字典
            
        Returns:
            是否为最近的文件
        """
        # 优先从明确的日期字段获取日期，避免从标题误解析
        date_sources = [
            file_info.get('date_text', ''),    # 首先使用明确的日期文本
            file_info.get('description', ''),  # 然后是描述
            file_info.get('filename', ''),     # 最后是文件名
            # 注意：不再从标题解析日期，避免"VS系列6页简易型录"被误判
        ]
        
        for source in date_sources:
            if source:
                file_date = self.parse_date_from_text(source)
                if file_date and file_date >= self.cutoff_date:
                    self.logger.info(f"文件日期符合要求: {file_date} >= {self.cutoff_date}")
                    return True
                elif file_date:
                    self.logger.info(f"文件日期过早: {file_date} < {self.cutoff_date}")
                    return False
        
        # 如果从明确字段无法确定日期，再尝试从标题解析（但要更严格）
        title = file_info.get('title', '')
        if title:
            # 只有当标题明确包含年份时才尝试解析
            if re.search(r'\d{4}', title):
                file_date = self.parse_date_from_text(title)
                if file_date and file_date >= self.cutoff_date:
                    self.logger.info(f"从标题解析日期符合要求: {file_date} >= {self.cutoff_date}")
                    return True
                elif file_date:
                    self.logger.info(f"从标题解析日期过早: {file_date} < {self.cutoff_date}")
                    return False
        
        # 如果无法确定日期，默认下载（让用户手动筛选）
        self.logger.warning(f"无法确定文件日期，默认下载: {file_info.get('title', '未知文件')}")
        return True

    def get_download_page_url(self) -> str:
        """获取档案下载页面URL"""
        return f"{self.base_url}/zh-cn/download.html"

    def parse_navigation_categories(self, soup: BeautifulSoup) -> List[Dict]:
        """
        解析主导航分类
        
        Args:
            soup: BeautifulSoup对象
            
        Returns:
            分类列表
        """
        categories = []
        
        # 查找左侧导航菜单
        # 根据网站结构，查找档案下载部分的导航
        nav_sections = soup.find_all(['div', 'ul', 'li'], class_=lambda x: x and any(
            keyword in str(x).lower() for keyword in ['nav', 'menu', 'sidebar', 'category']))
        
        for section in nav_sections:
            # 查找所有链接
            links = section.find_all('a', href=True)
            
            for link in links:
                text = link.get_text(strip=True)
                href = link.get('href', '')
                
                # 匹配我们关心的分类
                if text in self.modules:
                    full_url = href if href.startswith('http') else urljoin(self.base_url, href)
                    categories.append({
                        'name': text,
                        'url': full_url,
                        'has_subcategories': self.modules[text]['has_subcategories']
                    })
        
        # 如果没有找到导航链接，构建默认URL
        if not categories:
            for module_name in self.modules:
                # 根据观察到的URL模式构建链接
                category_url = f"{self.base_url}/zh-cn/download.html#{module_name}"
                categories.append({
                    'name': module_name,
                    'url': category_url,
                    'has_subcategories': self.modules[module_name]['has_subcategories']
                })
        
        return categories

    def parse_subcategories(self, category_name: str, soup: BeautifulSoup) -> List[Dict]:
        """
        解析子分类
        
        Args:
            category_name: 主分类名称
            soup: BeautifulSoup对象
            
        Returns:
            子分类列表
        """
        subcategories = []
        
        if not self.modules[category_name]['has_subcategories']:
            return subcategories
        
        expected_subcats = self.modules[category_name]['subcategories']
        
        # 查找子分类链接
        for subcat_name in expected_subcats:
            # 在页面中查找匹配的链接
            links = soup.find_all('a', href=True)
            
            for link in links:
                link_text = link.get_text(strip=True)
                if subcat_name in link_text or link_text in subcat_name:
                    href = link.get('href', '')
                    full_url = href if href.startswith('http') else urljoin(self.base_url, href)
                    
                    subcategories.append({
                        'name': subcat_name,
                        'url': full_url,
                        'folder': self.sanitize_folder_name(subcat_name)
                    })
                    break
        
        return subcategories

    def parse_download_links(self, url: str) -> List[Dict]:
        """
        解析下载链接
        
        Args:
            url: 页面URL
            
        Returns:
            下载信息列表
        """
        response = self.safe_request(url)
        if not response:
            return []
        
        soup = BeautifulSoup(response.content, 'html.parser')
        downloads = []
        
        # 查找实际的下载链接
        # 查找所有带href的链接
        all_links = soup.find_all('a', href=True)
        
        for link in all_links:
            href = link.get('href', '').strip()
            text = link.get_text(strip=True)
            
            # 只处理真正的下载链接
            if self.is_download_link(href, text):
                # 构建完整URL
                if href.startswith('/'):
                    full_url = self.base_url + href
                elif href.startswith('http'):
                    full_url = href
                else:
                    full_url = urljoin(url, href)
                
                # 提取文件信息
                file_info = self.extract_file_info(link, soup)
                file_info['url'] = full_url
                file_info['original_href'] = href
                
                # 避免重复添加相同的链接
                if not any(d['url'] == full_url for d in downloads):
                    downloads.append(file_info)
                    self.logger.info(f"找到下载链接: {full_url} - {text}")
        
        self.logger.info(f"在 {url} 找到 {len(downloads)} 个有效下载链接")
        return downloads

    def is_download_link(self, href: str, text: str) -> bool:
        """
        判断是否为下载链接
        
        Args:
            href: 链接地址
            text: 链接文本
            
        Returns:
            是否为下载链接
        """
        if not href:
            return False
        
        # 首先排除明显的HTML页面链接
        exclude_patterns = [
            'download.html', 'download.asp?customer_id=', 'product-c', 
            '.html', 'Directory_ID=', 'name_id=', '#'
        ]
        
        for pattern in exclude_patterns:
            if pattern in href:
                return False
        
        # 检查真正的下载链接格式
        # 丰炜科技的真实下载链接格式: /v_comm/inc/download_file.asp?re_id=xxx&fid=xxx
        if 'download_file.asp' in href and 'fid=' in href and 're_id=' in href:
            return True
        
        # 检查直接文件链接
        download_extensions = ['.pdf', '.doc', '.docx', '.zip', '.rar', '.exe', '.msi']
        href_lower = href.lower()
        for ext in download_extensions:
            if href_lower.endswith(ext):
                return True
        
        return False

    def extract_file_info(self, link, soup: BeautifulSoup) -> Dict:
        """
        提取文件信息（针对丰炜科技页面结构优化）
        
        Args:
            link: 下载链接元素
            soup: BeautifulSoup对象
            
        Returns:
            文件信息字典
        """
        file_info = {
            'title': '',
            'filename': '',
            'date_text': '',
            'description': '',
            'size': ''
        }
        
        # 提取标题
        title = link.get_text(strip=True)
        
        # 特殊处理丰炜科技的页面结构
        if 'download_file.asp' in link.get('href', ''):
            # 如果是"档案下载"按钮，寻找实际的文件标题
            if not title or '档案下载' in title:
                # 寻找同一个dl元素中的标题
                parent_dl = link.find_parent('dl')
                if parent_dl:
                    title_elem = parent_dl.find('dt', class_='list_title')
                    if title_elem:
                        title_link = title_elem.find('a')
                        if title_link:
                            title = title_link.get_text(strip=True)
            
            # 提取文件大小
            parent_span = link.find_parent('span', class_='list_download_icon')
            if parent_span:
                size_text = parent_span.get_text(strip=True)
                size_match = re.search(r'\(([\d.]+\s*[KMGT]?B)\)', size_text)
                if size_match:
                    file_info['size'] = size_match.group(1)
            
            # 提取发布日期
            parent_dl = link.find_parent('dl')
            if parent_dl:
                # 查找vigor-time类的div
                date_elem = parent_dl.find('div', class_='vigor-time')
                if date_elem:
                    date_text = date_elem.get_text(strip=True)
                    date_match = re.search(r'(\d{4}/\d{1,2}/\d{1,2})', date_text)
                    if date_match:
                        file_info['date_text'] = date_match.group(1)
                
                # 如果没找到，查找其他可能的日期格式
                if not file_info['date_text']:
                    dd_elem = parent_dl.find('dd', class_='list_txt')
                    if dd_elem:
                        dd_text = dd_elem.get_text()
                        date_match = re.search(r'(\d{4}/\d{1,2}/\d{1,2})', dd_text)
                        if date_match:
                            file_info['date_text'] = date_match.group(1)
        else:
            # 原有的通用逻辑
            if title:
                file_info['title'] = title
                file_info['filename'] = self.extract_filename_from_title(title)
            
            # 从父元素或兄弟元素提取更多信息
            parent = link.parent
            if parent:
                # 查找日期信息
                date_text = self.find_date_in_element(parent)
                if date_text:
                    file_info['date_text'] = date_text
                
                # 查找描述信息
                description = parent.get_text(strip=True)
                if description and len(description) > len(title):
                    file_info['description'] = description
            
            # 查找兄弟元素中的信息
            siblings = link.find_next_siblings()
            for sibling in siblings[:3]:  # 只检查前3个兄弟元素
                sibling_text = sibling.get_text(strip=True)
                if sibling_text:
                    # 检查是否包含日期
                    if self.parse_date_from_text(sibling_text):
                        file_info['date_text'] = sibling_text
                    
                    # 检查是否包含文件大小
                    if re.search(r'\d+\s*(KB|MB|GB)', sibling_text, re.I):
                        file_info['size'] = sibling_text
        
        # 设置最终的标题和文件名
        if title:
            file_info['title'] = title
            file_info['filename'] = self.extract_filename_from_title(title)
        
        return file_info

    def extract_file_info_from_table_row(self, row) -> Dict:
        """
        从表格行中提取文件信息
        
        Args:
            row: 表格行元素
            
        Returns:
            文件信息字典
        """
        file_info = {
            'title': '',
            'filename': '',
            'date_text': '',
            'description': '',
            'size': ''
        }
        
        cells = row.find_all(['td', 'th'])
        
        # 尝试识别表格列的含义
        for i, cell in enumerate(cells):
            cell_text = cell.get_text(strip=True)
            
            if i == 0:  # 通常第一列是文件名或标题
                file_info['title'] = cell_text
                file_info['filename'] = self.extract_filename_from_title(cell_text)
            elif self.parse_date_from_text(cell_text):  # 包含日期的列
                file_info['date_text'] = cell_text
            elif re.search(r'\d+\s*(KB|MB|GB)', cell_text, re.I):  # 包含文件大小的列
                file_info['size'] = cell_text
            elif len(cell_text) > 10:  # 可能是描述列
                file_info['description'] = cell_text
        
        return file_info

    def find_date_in_element(self, element) -> str:
        """
        在元素中查找日期信息
        
        Args:
            element: HTML元素
            
        Returns:
            找到的日期文本
        """
        text = element.get_text(strip=True)
        
        # 查找常见的日期模式
        date_patterns = [
            r'\d{4}[/-]\d{1,2}[/-]\d{1,2}',
            r'\d{4}年\d{1,2}月\d{1,2}日',
            r'\d{4}年\d{1,2}月',
            r'\d{4}/\d{1,2}/\d{1,2}',
        ]
        
        for pattern in date_patterns:
            match = re.search(pattern, text)
            if match:
                return match.group()
        
        return ''

    def extract_filename_from_title(self, title: str) -> str:
        """
        从标题中提取文件名
        
        Args:
            title: 标题文本
            
        Returns:
            文件名
        """
        # 清理文件名
        filename = re.sub(r'[<>:"/\\|?*\[\]{}]', '', title.strip())
        
        # 处理特殊字符
        filename = filename.replace('/', '_').replace(' ', '_')
        
        # 确保文件名有扩展名
        if not re.search(r'\.[a-zA-Z0-9]+$', filename):
            filename += '.pdf'  # 默认为PDF
        
        return filename

    def test_asp_download_link(self, url: str) -> bool:
        """
        测试ASP下载链接是否有效
        
        Args:
            url: ASP下载链接
            
        Returns:
            是否为有效的下载链接
        """
        try:
            # 发送HEAD请求检查链接
            response = self.session.head(url, timeout=10, verify=False)
            
            if response.status_code == 200:
                content_type = response.headers.get('content-type', '').lower()
                content_length = int(response.headers.get('content-length', 0))
                
                # 检查是否为文件下载
                if any(ct in content_type for ct in ['application/', 'binary/', 'octet-stream']):
                    self.logger.info(f"ASP链接有效: content-type={content_type}, size={content_length}")
                    return True
                elif content_length > 100000:  # 大于100KB可能是文件
                    self.logger.info(f"ASP链接可能有效（大文件）: size={content_length}")
                    return True
                else:
                    self.logger.warning(f"ASP链接可能无效: content-type={content_type}, size={content_length}")
                    return False
            else:
                self.logger.error(f"ASP链接访问失败: status={response.status_code}")
                return False
                
        except Exception as e:
            self.logger.error(f"测试ASP链接失败: {e}")
            return False

    def sanitize_folder_name(self, name: str) -> str:
        """
        清理文件夹名称
        
        Args:
            name: 原始名称
            
        Returns:
            清理后的名称
        """
        # 移除或替换特殊字符
        name = re.sub(r'[<>:"/\\|?*]', '_', name.strip())
        name = re.sub(r'\s+', '_', name)
        return name

    def get_unique_filename(self, filepath: Path) -> Path:
        """
        获取唯一的文件名，如果文件已存在则添加数字后缀
        
        Args:
            filepath: 原始文件路径
            
        Returns:
            唯一的文件路径
        """
        if not filepath.exists():
            return filepath
        
        # 分离文件名和扩展名
        stem = filepath.stem
        suffix = filepath.suffix
        parent = filepath.parent
        
        # 添加数字后缀
        counter = 1
        while True:
            new_name = f"{stem}_{counter:02d}{suffix}"
            new_filepath = parent / new_name
            if not new_filepath.exists():
                return new_filepath
            counter += 1
            
            # 防止无限循环
            if counter > 999:
                return filepath

    def download_file(self, url: str, filepath: Path, file_info: Dict) -> bool:
        """
        下载文件（增强版，支持文件验证）
        
        Args:
            url: 文件URL
            filepath: 保存路径
            file_info: 文件信息
            
        Returns:
            是否下载成功
        """
        try:
            # 时间过滤
            if not self.is_file_recent(file_info):
                self.logger.info(f"文件时间过早，跳过: {file_info.get('title', filepath.name)}")
                self.stats['filtered_by_date'] += 1
                return True
            
            # 检查URL是否已处理过
            if url in self.processed_urls:
                self.logger.info(f"URL已处理过，跳过: {file_info.get('title', filepath.name)}")
                self.stats['skipped_files'] += 1
                return True
            
            # 生成更好的文件名
            if file_info.get('title'):
                better_filename = self.extract_filename_from_title(file_info['title'])
                new_filepath = filepath.parent / better_filename
                filepath = self.get_unique_filename(new_filepath)
            
            # 检查文件是否已存在
            if filepath.exists():
                self.logger.info(f"文件已存在，跳过: {filepath.name}")
                self.stats['skipped_files'] += 1
                return True
            
            # 创建目录
            filepath.parent.mkdir(parents=True, exist_ok=True)
            
            # 下载文件
            self.logger.info(f"开始下载: {file_info.get('title', filepath.name)} -> {filepath.name}")
            self.logger.info(f"请求URL: {url}")
            
            response = self.safe_request(url)
            if not response:
                return False
            
            # 检查响应内容类型
            content_type = response.headers.get('content-type', '').lower()
            content_length = int(response.headers.get('content-length', 0))
            content_disposition = response.headers.get('content-disposition', '')
            
            # 从Content-Disposition头中提取真实文件名
            real_filename = None
            if content_disposition:
                import urllib.parse
                # 解析Content-Disposition头
                if 'filename=' in content_disposition:
                    filename_part = content_disposition.split('filename=')[1]
                    if filename_part.startswith('"'):
                        # 带引号的文件名
                        real_filename = filename_part.split('"')[1]
                    else:
                        # 不带引号的文件名，可能有URL编码
                        real_filename = filename_part.split(';')[0].strip()
                        real_filename = urllib.parse.unquote(real_filename)
                
                if real_filename:
                    self.logger.info(f"从响应头获取真实文件名: {real_filename}")
                    # 更新文件路径
                    new_filepath = filepath.parent / real_filename
                    filepath = self.get_unique_filename(new_filepath)
            
            # 验证是否为有效的文件下载
            if 'text/html' in content_type and content_length < 50000:  # 小于50KB的HTML可能是错误页面
                self.logger.warning(f"响应可能是错误页面而非文件: content-type={content_type}, length={content_length}")
                # 检查内容是否包含错误信息
                content_preview = response.content[:1000].decode('utf-8', errors='ignore')
                if any(keyword in content_preview.lower() for keyword in ['error', '错误', '404', '403', 'not found']):
                    self.logger.error(f"响应包含错误信息，跳过下载: {file_info.get('title', filepath.name)}")
                    self.stats['failed_files'] += 1
                    return False
            
            # 保存文件
            with open(filepath, 'wb') as f:
                f.write(response.content)
            
            file_size = filepath.stat().st_size
            
            # 验证下载的文件
            if file_size < 512:  # 小于512字节的文件可能有问题
                self.logger.warning(f"下载的文件过小，可能无效: {filepath.name} ({file_size} bytes)")
                # 检查文件内容
                try:
                    with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                        content = f.read(500)
                        if any(keyword in content.lower() for keyword in ['error', '错误', '404', '403', 'not found', '<html']):
                            self.logger.error(f"文件内容包含错误信息，删除文件: {filepath.name}")
                            filepath.unlink()  # 删除无效文件
                            self.stats['failed_files'] += 1
                            return False
                except:
                    pass  # 如果不是文本文件，忽略内容检查
            
            # 如果是大文件，显示更友好的大小信息
            if file_size > 1024 * 1024:  # 大于1MB
                size_str = f"{file_size/1024/1024:.2f} MB"
            elif file_size > 1024:  # 大于1KB
                size_str = f"{file_size/1024:.1f} KB"
            else:
                size_str = f"{file_size} bytes"
            
            self.logger.info(f"下载成功: {filepath.name} ({size_str})")
            
            # 记录处理的URL
            self.processed_urls.add(url)
            
            # 添加到新文件列表
            self.new_files.append({
                'type': 'PDF' if filepath.suffix.lower() == '.pdf' else '文档',
                'title': file_info.get('title', filepath.stem),
                'path': str(filepath),
                'url': url,
                'size': file_size,
                'date': file_info.get('date_text', '')
            })
            
            self.stats['downloaded_files'] += 1
            return True
            
        except Exception as e:
            self.logger.error(f"下载失败 {url}: {e}")
            self.stats['failed_files'] += 1
            return False

    def crawl_module(self, module_name: str, module_config: Dict):
        """
        爬取指定模块
        
        Args:
            module_name: 模块名称
            module_config: 模块配置
        """
        self.logger.info(f"开始爬取模块: {module_name}")
        
        folder_name = module_config['folder_name']
        has_subcategories = module_config['has_subcategories']
        
        module_dir = self.base_dir / folder_name
        module_dir.mkdir(parents=True, exist_ok=True)
        
        if has_subcategories:
            # 处理有子分类的模块
            subcategories = module_config['subcategories']
            
            # 首先尝试从主分类页面获取下载
            if 'main_url' in module_config:
                main_url = self.base_url + module_config['main_url']
                self.logger.info(f"检查主分类页面: {main_url}")
                
                main_downloads = self.parse_download_links(main_url)
                if main_downloads:
                    self.logger.info(f"主分类页面找到 {len(main_downloads)} 个下载")
                    for download in main_downloads:
                        self.stats['total_files'] += 1
                        
                        # 检查是否符合时间过滤条件
                        if not self.is_file_recent(download):
                            self.stats['filtered_by_date'] += 1
                            self.logger.info(f"文件因日期过滤而跳过: {download.get('title', '未知文件')}")
                            continue
                        
                        # 智能分类文件
                        smart_category = self.classify_file_by_name(
                            download.get('filename', ''), 
                            download.get('title', '')
                        )
                        
                        if smart_category and smart_category != folder_name:
                            # 文件应该归类到其他目录
                            target_dir = self.base_dir / smart_category
                            target_dir.mkdir(parents=True, exist_ok=True)
                            self.logger.info(f"根据文件名智能分类: {download.get('title', '')} -> {smart_category}")
                        else:
                            target_dir = module_dir
                        
                        # 生成临时文件路径用于下载
                        temp_filename = f"temp_{int(time.time())}_{download.get('url', '').split('fid=')[-1]}.tmp"
                        temp_filepath = target_dir / temp_filename
                        
                        self.download_file(download['url'], temp_filepath, download)
                        time.sleep(1)
            
            # 然后处理子分类
            for subcat_name, subcat_url in subcategories.items():
                self.logger.info(f"处理子分类: {subcat_name}")
                
                # 创建子分类目录
                subcat_dir = module_dir / self.sanitize_folder_name(subcat_name)
                subcat_dir.mkdir(parents=True, exist_ok=True)
                
                # 构建完整URL
                full_url = self.base_url + subcat_url
                
                # 爬取子分类页面
                downloads = self.parse_download_links(full_url)
                
                for download in downloads:
                    self.stats['total_files'] += 1
                    
                    # 检查是否符合时间过滤条件
                    if not self.is_file_recent(download):
                        self.stats['filtered_by_date'] += 1
                        self.logger.info(f"文件因日期过滤而跳过: {download.get('title', '未知文件')}")
                        continue
                    
                    # 生成临时文件路径用于下载
                    temp_filename = f"temp_{int(time.time())}_{download.get('url', '').split('fid=')[-1]}.tmp"
                    temp_filepath = subcat_dir / temp_filename
                    
                    self.download_file(download['url'], temp_filepath, download)
                    time.sleep(1)
        else:
            # 处理没有子分类的模块
            module_url = self.base_url + module_config['url']
            downloads = self.parse_download_links(module_url)
            
            for download in downloads:
                self.stats['total_files'] += 1
                
                # 检查是否符合时间过滤条件
                if not self.is_file_recent(download):
                    self.stats['filtered_by_date'] += 1
                    self.logger.info(f"文件因日期过滤而跳过: {download.get('title', '未知文件')}")
                    continue
                
                # 生成临时文件路径用于下载
                temp_filename = f"temp_{int(time.time())}_{download.get('url', '').split('fid=')[-1]}.tmp"
                temp_filepath = module_dir / temp_filename
                
                self.download_file(download['url'], temp_filepath, download)
                time.sleep(1)

    def save_progress(self):
        """保存爬取进度"""
        progress_file = self.base_dir / 'crawl_progress.json'
        progress_data = {
            'timestamp': time.time(),
            'stats': self.stats,
            'cutoff_date': self.cutoff_date.isoformat(),
            'completed_modules': []
        }
        
        with open(progress_file, 'w', encoding='utf-8') as f:
            json.dump(progress_data, f, ensure_ascii=False, indent=2)

    def run(self):
        """运行爬虫"""
        if self.monitor_mode:
            return self.run_monitor_mode()
            
        self.logger.info("开始爬取丰炜科技档案下载中心")
        self.logger.info(f"保存目录: {self.base_dir}")
        self.logger.info(f"时间过滤: 只下载 {self.cutoff_date} 之后的文件")
        
        start_time = time.time()
        
        try:
            # 爬取所有模块
            for module_name, module_config in self.modules.items():
                self.logger.info(f"=" * 50)
                self.crawl_module(module_name, module_config)
                time.sleep(2)  # 模块间暂停
                
        except KeyboardInterrupt:
            self.logger.info("用户中断爬取")
        except Exception as e:
            self.logger.error(f"爬取过程中出现错误: {e}")
        finally:
            # 保存进度和统计信息
            self.save_progress()
            
            # 保存已处理的URLs
            self.save_processed_urls()
            
            # 打印统计信息
            end_time = time.time()
            duration = end_time - start_time
            
            self.logger.info("=" * 50)
            self.logger.info("爬取完成!")
            self.logger.info(f"总耗时: {duration:.2f} 秒")
            self.logger.info(f"总文件数: {self.stats['total_files']}")
            self.logger.info(f"下载成功: {self.stats['downloaded_files']}")
            self.logger.info(f"跳过文件: {self.stats['skipped_files']}")
            self.logger.info(f"时间过滤: {self.stats['filtered_by_date']}")
            self.logger.info(f"下载失败: {self.stats['failed_files']}")
            self.logger.info(f"保存目录: {self.base_dir}")
            
            # 发送通知
            if self.new_files:
                self.send_notifications()

    def run_monitor_mode(self):
        """
        运行监控模式 - 只检测新文件，不下载
        """
        self.logger.info("🔍 开始运行监控模式 - 检测新文件")
        self.logger.info(f"监控目录: {self.base_dir}")
        self.logger.info(f"时间过滤: 只关注 {self.cutoff_date} 之后的文件")
        
        start_time = time.time()
        
        try:
            # 监控所有模块
            for module_name, module_config in self.modules.items():
                self.logger.info(f"=" * 50)
                self.logger.info(f"🔍 监控模块: {module_name}")
                
                # 获取文件列表但不下载
                files = self.get_module_files(module_name, module_config)
                
                # 检查文件变化
                for file_info in files:
                    if self.is_file_recent(file_info):
                        change_status = self.check_file_changes(file_info)
                        if change_status == 'new':
                            self.logger.info(f"  🆕 发现新文件: {file_info['title']}")
                        elif change_status == 'updated':
                            self.logger.info(f"  🔄 文件已更新: {file_info['title']}")
                
                time.sleep(1)  # 减少延时
                
        except KeyboardInterrupt:
            self.logger.info("用户中断监控")
        except Exception as e:
            self.logger.error(f"监控过程中出现错误: {e}")
        finally:
            # 保存文件历史
            self.save_file_history()
            
            # 发送监控通知
            if self.detected_new_files or self.detected_updated_files:
                self.send_monitor_notifications()
            else:
                self.logger.info("✅ 监控完成，未发现新文件或更新")
                
            end_time = time.time()
            duration = end_time - start_time
            self.logger.info("=" * 50)
            self.logger.info(f"监控完成! 耗时: {duration:.2f} 秒")
            self.logger.info(f"新文件: {len(self.detected_new_files)} 个")
            self.logger.info(f"更新文件: {len(self.detected_updated_files)} 个")

    def get_module_files(self, module_name, module_config):
        """获取模块的文件列表，不下载"""
        files = []
        try:
            has_subcategories = module_config['has_subcategories']
            
            if has_subcategories:
                # 处理有子分类的模块 - 先检查主页面
                if 'main_url' in module_config:
                    main_url = self.base_url + module_config['main_url']
                    self.logger.info(f"检查主分类页面: {main_url}")
                    main_files = self.parse_download_links(main_url)
                    for file_info in main_files:
                        file_info['module'] = module_name
                        files.append(file_info)
                
                # 处理子分类
                subcategories = module_config.get('subcategories', {})
                for subcategory, sub_config in subcategories.items():
                    self.logger.info(f"处理子分类: {subcategory}")
                    if isinstance(sub_config, dict) and 'url' in sub_config:
                        sub_url = self.base_url + sub_config['url']
                    else:
                        # 如果配置是字符串，直接作为URL
                        sub_url = self.base_url + str(sub_config)
                    sub_files = self.parse_download_links(sub_url)
                    for file_info in sub_files:
                        file_info['module'] = module_name
                        file_info['subcategory'] = subcategory
                        files.append(file_info)
            else:
                # 处理没有子分类的模块
                main_url = self.base_url + module_config['main_url']
                files = self.parse_download_links(main_url)
                for file_info in files:
                    file_info['module'] = module_name
                    
        except Exception as e:
            self.logger.error(f"获取 {module_name} 文件列表失败: {e}")
        
        return files

    def send_monitor_notifications(self):
        """发送监控模式的通知"""
        try:
            new_count = len(self.detected_new_files)
            updated_count = len(self.detected_updated_files)
            
            if new_count == 0 and updated_count == 0:
                return
                
            # 控制台通知
            self.logger.info(f"\n🔔 文件变化检测通知:")
            self.logger.info("=" * 60)
            
            if new_count > 0:
                self.logger.info(f"🆕 发现 {new_count} 个新文件:")
                for file_info in self.detected_new_files[:5]:
                    self.logger.info(f"  📄 {file_info['title']} ({file_info['module']})")
                if new_count > 5:
                    self.logger.info(f"  ... 还有 {new_count - 5} 个新文件")
            
            if updated_count > 0:
                self.logger.info(f"🔄 发现 {updated_count} 个文件更新:")
                for file_info in self.detected_updated_files[:5]:
                    self.logger.info(f"  📄 {file_info['title']} ({file_info['module']})")
                if updated_count > 5:
                    self.logger.info(f"  ... 还有 {updated_count - 5} 个更新文件")
            
            # 钉钉通知
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            message = f"""🔔 丰炜科技 文件监控报告

📊 检测统计:
  新增文件: {new_count} 个
  更新文件: {updated_count} 个
  总变化: {new_count + updated_count} 个

📂 监控目录: {self.base_dir}
⏰ 检测时间: {current_time}
🕒 时间筛选: 只关注2024年11月之后的文件

💡 提示: 运行正常下载模式可获取这些新文件"""
            
            # 发送钉钉通知
            self.send_dingtalk_notification(message)
            
        except Exception as e:
            self.logger.error(f"发送监控通知失败: {e}")

    def send_dingtalk_notification(self, message):
        """发送钉钉通知"""
        try:
            timestamp = str(round(time.time() * 1000))
            string_to_sign = f'{timestamp}\n{self.SECRET}'
            hmac_code = hmac.new(self.SECRET.encode(), string_to_sign.encode(), hashlib.sha256).digest()
            sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))

            url = f'https://oapi.dingtalk.com/robot/send?access_token={self.ACCESS_TOKEN}&timestamp={timestamp}&sign={sign}'
            headers = {'Content-Type': 'application/json'}
            data = {
                "msgtype": "text",
                "text": {"content": message},
                "at": {"isAtAll": False}
            }

            response = requests.post(url, json=data, headers=headers)
            self.logger.info(f"钉钉通知响应：{response.status_code} {response.text}")
            return response.status_code == 200
        except Exception as e:
            self.logger.error(f"钉钉通知发送失败: {e}")
            return False

    def send_notifications(self):
        """发送新增文件通知"""
        try:
            if not self.new_files:
                return
            
            # 控制台通知
            self.logger.info(f"\n🎉 爬取完成通知:")
            self.logger.info("=" * 60)
            self.logger.info(f"📊 发现 {len(self.new_files)} 个新文件 (2024.11之后):")
            
            # 按类型统计
            type_counts = {}
            for file_info in self.new_files:
                file_type = file_info['type']
                type_counts[file_type] = type_counts.get(file_type, 0) + 1
            
            for file_type, count in type_counts.items():
                self.logger.info(f"  📁 {file_type}: {count} 个")
            
            self.logger.info(f"\n📂 最新文件预览:")
            for file_info in self.new_files[:5]:  # 显示前5个
                size_str = f" ({file_info['size']/1024:.1f}KB)" if 'size' in file_info else ""
                date_str = f" [{file_info['date']}]" if file_info.get('date') else ""
                self.logger.info(f"  📄 {file_info['title']}{size_str}{date_str}")
            
            if len(self.new_files) > 5:
                self.logger.info(f"  ... 还有 {len(self.new_files) - 5} 个文件")
                
            self.logger.info(f"\n💾 所有文件已保存至: {self.base_dir}")
        
            # 钉钉通知
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            total_files = len(self.new_files)
            success_rate = 100.0 if self.stats['failed_files'] == 0 else (self.stats['downloaded_files'] / (self.stats['downloaded_files'] + self.stats['failed_files'])) * 100
            
            message = f"""✅ 丰炜科技 档案下载爬取成功，请及时审核

📊 下载统计:
  成功下载: {total_files} 个文件 (2024.11+)
  总识别文件: {self.stats['total_files']} 个
  时间过滤: {self.stats['filtered_by_date']} 个
  成功率: {success_rate:.1f}%

📁 文件存放路径: {self.base_dir}
⏰ 完成时间: {current_time}
🕒 时间筛选: 只下载2024年11月之后的文件"""
            
            # 发送钉钉通知
            self.send_dingtalk_notification(message)
            
        except Exception as e:
            self.logger.error(f"发送通知失败: {e}")


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='丰炜科技档案下载爬虫')
    parser.add_argument('--base-dir', type=str, help='下载文件保存的基础目录')
    parser.add_argument('--monitor', action='store_true', help='运行监控模式，只检测新文件不下载')
    parser.add_argument('--modules', nargs='+', help='指定要爬取的模块，如：软件 型录 CE证书')
    parser.add_argument('--no-date-filter', action='store_true', help='禁用时间过滤，下载所有文件')
    
    args = parser.parse_args()
    
    spider = VigorSpider(base_dir=args.base_dir, monitor_mode=args.monitor)
    
    # 如果指定了模块，只处理这些模块
    if args.modules:
        # 临时修改模块配置
        selected_modules = {}
        for module_name in args.modules:
            if module_name in spider.modules:
                selected_modules[module_name] = spider.modules[module_name]
            else:
                print(f"警告: 未找到模块 '{module_name}'")
                print(f"可用模块: {list(spider.modules.keys())}")
        
        if selected_modules:
            spider.modules = selected_modules
        else:
            print("错误: 没有找到任何有效模块")
            return
    
    # 如果禁用时间过滤
    if args.no_date_filter:
        spider.cutoff_date = date(2000, 1, 1)  # 设置一个很早的日期
    
    spider.run()


if __name__ == "__main__":
    main()
