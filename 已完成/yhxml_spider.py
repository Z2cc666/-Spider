#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
北京友方金泰（岛电）下载中心爬虫
网站：http://www.yhxml.com/articles.php?classid=3
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
from datetime import datetime

class YHXMLSpider:
    def __init__(self, base_dir: str = None):
        """
        初始化爬虫
        
        Args:
            base_dir: 下载文件保存的基础目录，默认为当前代码文件夹
        """
        if base_dir is None:
            # 默认使用服务器指定目录
            base_dir = "/srv/downloads/approved/岛电"
        self.base_url = "http://www.yhxml.com"
        self.base_dir = Path(base_dir)
        self.session = requests.Session()
        
        # 设置请求头
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
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
        
        # 判断是否为首次运行
        self.is_first_run = not (self.base_dir / 'processed_urls.pkl').exists()
        
        # 下载统计
        self.stats = {
            'total_files': 0,
            'downloaded_files': 0,
            'failed_files': 0,
            'skipped_files': 0
        }
        
        # 模块配置
        self.modules = {
            '产品选型指南': {
                'classid': 30,
                'has_subcategories': False,
                'folder_name': '产品选型指南'
            },
            '产品资料下载': {
                'classid': 36,
                'has_subcategories': True,  # 有产品分类
                'folder_name': '产品资料下载'
            },
            '软件下载': {
                'classid': 34,
                'has_subcategories': False,
                'folder_name': '软件下载'
            },
            '产品规格书': {
                'classid': 40,
                'has_subcategories': False,
                'folder_name': '产品规格书'
            },
            '停产产品': {
                'classid': 37,
                'has_subcategories': True,  # 有多个子产品分类
                'folder_name': '停产产品'
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
            response = self.session.get(url, timeout=timeout)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            self.logger.error(f"请求失败 {url}: {e}")
            return None

    def parse_download_page(self, url: str) -> List[Dict]:
        """
        解析下载页面，提取下载链接
        
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
        
        # 方法1：查找包含下载链接的列表项
        # 首先尝试查找所有可能的列表项
        list_items = []
        
        # 查找所有包含"下载"链接的元素
        download_links = soup.find_all('a', text=lambda x: x and '下载' in x)
        for link in download_links:
            # 向上查找包含该链接的列表项
            item = link.find_parent(['li', 'div', 'td'])
            if item and item not in list_items:
                list_items.append(item)
        
        # 如果没有找到，尝试查找所有列表项
        if not list_items:
            list_items = soup.find_all(['li', 'div'], class_=lambda x: x and any(keyword in str(x).lower() for keyword in ['item', 'list', 'download']))
        
        for item in list_items:
            # 查找下载链接
            download_link = item.find('a', href=True)
            if not download_link:
                continue
                
            href = download_link.get('href', '')
            link_text = download_link.get_text(strip=True)
            
            # 检查是否是下载链接
            if any(keyword in link_text for keyword in ['下载', 'download']):
                if href.startswith('http') or href.startswith('/'):
                    full_url = href if href.startswith('http') else urljoin(self.base_url, href)
                    
                    # 从列表项中提取标题（排除下载链接文本）
                    title = self.extract_title_from_list_item(item, download_link)
                    
                    if title:
                        downloads.append({
                            'title': title,
                            'url': full_url,
                            'filename': self.extract_filename_from_title(title)
                        })
        
        # 方法2：如果方法1没有找到，回退到原来的方法
        if not downloads:
            download_links = soup.find_all('a', href=True)
            
            for link in download_links:
                href = link.get('href', '')
                text = link.get_text(strip=True)
                
                # 过滤下载链接（包含下载相关关键词）
                if any(keyword in text for keyword in ['下载', 'download', '说明书', '规格', '软件', '协议', '流程图']):
                    if href.startswith('http') or href.startswith('/'):
                        full_url = href if href.startswith('http') else urljoin(self.base_url, href)
                        
                        # 尝试从父元素或兄弟元素获取更详细的标题信息
                        detailed_title = self.extract_detailed_title(link, soup)
                        
                        downloads.append({
                            'title': detailed_title if detailed_title else text,
                            'url': full_url,
                            'filename': self.extract_filename_from_title(detailed_title if detailed_title else text)
                        })
        
        self.logger.info(f"在 {url} 找到 {len(downloads)} 个下载链接")
        return downloads

    def extract_title_from_list_item(self, item, download_link) -> str:
        """
        从列表项中提取标题，排除下载链接文本
        
        Args:
            item: 列表项元素
            download_link: 下载链接元素
            
        Returns:
            标题文本
        """
        # 获取整个列表项的文本，但排除下载链接的文本
        item_text = item.get_text(strip=True)
        link_text = download_link.get_text(strip=True)
        
        # 移除下载链接文本，得到标题
        if link_text in item_text:
            title = item_text.replace(link_text, '').strip()
            if title:
                return title
        
        # 如果上面的方法不行，尝试查找特定的文本元素
        # 查找包含产品名称的文本节点
        text_nodes = item.find_all(text=True, recursive=True)
        for text in text_nodes:
            text = text.strip()
            if text and text != link_text and len(text) > 3:
                # 检查是否包含产品相关信息
                if any(keyword in text for keyword in ['岛电', '产品', '选型', '指南', 'SRS', 'SR', 'FP', 'MR', 'SD', 'HCM', 'PAC', '说明书', '规格', '软件', '协议', '流程图']):
                    return text
        
        # 最后尝试：查找父级元素中的标题
        parent = item.parent
        if parent:
            # 查找标题标签
            title_elem = parent.find(['h1', 'h2', 'h3', 'h4', 'h5', 'h6'])
            if title_elem:
                title_text = title_elem.get_text(strip=True)
                if title_text and title_text != link_text:
                    return title_text
        
        return ""

    def extract_detailed_title(self, link, soup) -> str:
        """
        尝试从链接的上下文获取更详细的标题信息
        
        Args:
            link: 下载链接元素
            soup: BeautifulSoup对象
            
        Returns:
            详细的标题信息
        """
        # 方法1：查找父级元素中的标题
        parent = link.parent
        if parent:
            # 查找父级元素中的标题标签
            title_elem = parent.find(['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'strong', 'b'])
            if title_elem:
                title_text = title_elem.get_text(strip=True)
                if title_text and len(title_text) > 2:
                    return title_text
            
            # 查找父级元素中的其他文本内容
            parent_text = parent.get_text(strip=True)
            if parent_text and len(parent_text) > 10:
                # 提取有意义的文本片段
                lines = [line.strip() for line in parent_text.split('\n') if line.strip()]
                for line in lines:
                    if len(line) > 5 and any(keyword in line for keyword in ['SRS', 'SR', 'FP', 'MR', 'SD', 'HCM', 'PAC', '说明书', '规格', '软件', '协议', '流程图']):
                        return line
        
        # 方法2：查找兄弟元素中的标题
        siblings = link.find_previous_siblings()
        for sibling in siblings[:3]:  # 只检查前3个兄弟元素
            if sibling.name in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'strong', 'b', 'p']:
                sibling_text = sibling.get_text(strip=True)
                if sibling_text and len(sibling_text) > 5:
                    return sibling_text
        
        # 方法3：查找页面中的表格或列表结构
        # 如果链接在表格中，尝试获取行或列的标题
        table = link.find_parent('table')
        if table:
            # 查找当前行
            row = link.find_parent('tr')
            if row:
                cells = row.find_all(['td', 'th'])
                for cell in cells:
                    cell_text = cell.get_text(strip=True)
                    if cell_text and len(cell_text) > 5 and any(keyword in cell_text for keyword in ['SRS', 'SR', 'FP', 'MR', 'SD', 'HCM', 'PAC']):
                        return cell_text
        
        return ""

    def extract_filename_from_title(self, title: str, module_name: str = "") -> str:
        """
        从标题中提取文件名，保持原始格式
        
        Args:
            title: 标题文本
            module_name: 模块名称（当前未使用，保持接口兼容性）
            
        Returns:
            文件名
        """
        # 保持原始标题格式，只移除文件系统不允许的特殊字符
        filename = re.sub(r'[<>:"/\\|?*\[\]{}]', '', title.strip())
        
        # 确保文件名以.pdf结尾
        if not filename.lower().endswith('.pdf'):
            filename += '.pdf'
            
        return filename

    def get_subcategories(self, classid: int) -> List[Dict]:
        """
        获取子分类信息
        
        Args:
            classid: 分类ID
            
        Returns:
            子分类列表
        """
        url = f"{self.base_url}/articles.php?classid={classid}"
        response = self.safe_request(url)
        if not response:
            return []
        
        soup = BeautifulSoup(response.content, 'html.parser')
        subcategories = []
        
        # 查找左侧导航菜单中的子分类
        nav_links = soup.find_all('a', href=True)
        
        for link in nav_links:
            href = link.get('href', '')
            text = link.get_text(strip=True)
            
            # 匹配子分类链接模式
            if 'classid=' in href and text and len(text) > 1:
                # 提取classid
                match = re.search(r'classid=(\d+)', href)
                if match:
                    sub_classid = int(match.group(1))
                    if sub_classid != classid:  # 排除当前分类
                        full_url = href if href.startswith('http') else urljoin(self.base_url, href)
                        subcategories.append({
                            'name': text,
                            'classid': sub_classid,
                            'url': full_url
                        })
        
        # 去重
        seen = set()
        unique_subcategories = []
        for sub in subcategories:
            key = (sub['name'], sub['classid'])
            if key not in seen:
                seen.add(key)
                unique_subcategories.append(sub)
        
        self.logger.info(f"找到 {len(unique_subcategories)} 个子分类")
        return unique_subcategories

    def download_file(self, url: str, filepath: Path, title: str = "", module_name: str = "") -> bool:
        """
        下载文件（增量版本）
        
        Args:
            url: 文件URL
            filepath: 保存路径
            title: 文件标题
            module_name: 模块名称，用于生成更有意义的文件名
            
        Returns:
            是否下载成功
        """
        try:
            # 检查URL是否已处理过（优先检查，避免重复下载）
            if url in self.processed_urls:
                self.logger.info(f"URL已处理过，跳过: {title or '未知文件'}")
                self.stats['skipped_files'] += 1
                return True
            
            # 如果提供了title，重新生成更好的文件名
            if title and title.strip():
                better_filename = self.extract_filename_from_title(title, module_name)
                new_filepath = filepath.parent / better_filename
                
                # 检查原始文件名是否已存在（不自动生成编号）
                if new_filepath.exists():
                    self.logger.info(f"文件已存在，跳过: {new_filepath.name}")
                    # 即使跳过，也要记录URL已处理，避免重复检查
                    self.processed_urls.add(url)
                    self.stats['skipped_files'] += 1
                    return True
                
                filepath = new_filepath
            else:
                # 检查原始路径是否已存在
                if filepath.exists():
                    self.logger.info(f"文件已存在，跳过: {filepath.name}")
                    # 即使跳过，也要记录URL已处理
                    self.processed_urls.add(url)
                    self.stats['skipped_files'] += 1
                    return True
            
            # 创建目录
            filepath.parent.mkdir(parents=True, exist_ok=True)
            
            # 下载文件
            self.logger.info(f"开始下载: {title or filepath.name} -> {filepath.name}")
            response = self.safe_request(url)
            if not response:
                return False
            
            # 保存文件
            with open(filepath, 'wb') as f:
                f.write(response.content)
            
            file_size = filepath.stat().st_size
            self.logger.info(f"下载成功: {filepath.name} ({file_size} bytes)")
            
            # 记录处理的URL
            self.processed_urls.add(url)
            
            # 添加到新文件列表
            self.new_files.append({
                'type': 'PDF' if filepath.suffix.lower() == '.pdf' else '文档',
                'title': title or filepath.stem,
                'path': str(filepath),
                'url': url,
                'size': file_size
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
        
        classid = module_config['classid']
        folder_name = module_config['folder_name']
        has_subcategories = module_config['has_subcategories']
        
        module_dir = self.base_dir / folder_name
        module_dir.mkdir(parents=True, exist_ok=True)
        
        if has_subcategories:
            # 处理有子分类的模块
            if module_name == '产品资料下载':
                self.crawl_product_data_download(classid, module_dir)
            elif module_name == '停产产品':
                self.crawl_discontinued_products(classid, module_dir)
        else:
            # 处理没有子分类的模块（支持翻页）
            if module_name == '产品规格书':
                # 产品规格书需要翻页处理
                self.crawl_module_with_pagination(classid, module_dir, module_name)
            else:
                # 其他模块按原来的方式处理
                url = f"{self.base_url}/articles.php?classid={classid}"
                downloads = self.parse_download_page(url)
                
                for download in downloads:
                    self.stats['total_files'] += 1
                    # 使用title来生成更好的文件名，不再依赖原始filename
                    title = download.get('title', '文档')
                    # 创建一个临时路径，实际路径会在download_file中重新生成
                    temp_filepath = module_dir / "temp.pdf"
                    
                    self.download_file(download['url'], temp_filepath, title, module_name)
                    time.sleep(1)  # 避免请求过快

    def crawl_product_data_download(self, classid: int, base_dir: Path):
        """
        爬取产品资料下载模块（按产品分类保存）
        
        Args:
            classid: 分类ID
            base_dir: 基础目录
        """
        self.logger.info("爬取产品资料下载模块")
        
        # 明确定义所有产品分类的classid，基于你提供的HTML结构
        known_product_categories = [
            {'name': 'SRS1/3/4/5', 'classid': 27},
            {'name': 'SRS11A/12A/13A/14A', 'classid': 14},
            {'name': 'SR91/92/93/94', 'classid': 15},
            {'name': 'FP93', 'classid': 16},
            {'name': 'MR13', 'classid': 17},
            {'name': 'SR82/83/84', 'classid': 18},
            {'name': 'FP33/34', 'classid': 19},
            {'name': 'SR23A', 'classid': 20},
            {'name': 'FP23A', 'classid': 21},
            {'name': '模块型调节器', 'classid': 22},  # 这个之前被漏掉了
            {'name': 'SD24A', 'classid': 28},
            {'name': 'SD17', 'classid': 33},
            {'name': 'HCM人机界面', 'classid': 29},
            {'name': 'PAC26/35/36/46', 'classid': 31}
        ]
        
        self.logger.info(f"将爬取 {len(known_product_categories)} 个产品分类")
        
        # 依次爬取每个产品分类
        for category in known_product_categories:
            self.logger.info(f"开始爬取产品分类: {category['name']} (classid={category['classid']})")
            
            # 创建产品分类目录
            category_dir = base_dir / self.sanitize_folder_name(category['name'])
            category_dir.mkdir(parents=True, exist_ok=True)
            
            # 构建分类URL
            category_url = f"{self.base_url}/articles.php?classid={category['classid']}"
            
            # 爬取该分类下的所有页面（包括翻页）
            self.crawl_category_pages(category_url, category_dir, category['name'])
            
            time.sleep(2)  # 分类间暂停
    
    def crawl_category_pages(self, category_url: str, category_dir: Path, category_name: str):
        """
        爬取指定产品分类的所有页面（包括翻页）
        
        Args:
            category_url: 分类页面URL
            category_dir: 分类目录
            category_name: 分类名称
        """
        page_num = 1
        current_url = category_url
        
        while current_url:
            self.logger.info(f"爬取 {category_name} 第 {page_num} 页: {current_url}")
            
            # 解析当前页面
            downloads = self.parse_download_page(current_url)
            
            if not downloads:
                self.logger.info(f"{category_name} 第 {page_num} 页没有找到下载链接")
                break
            
            # 下载当前页面的文件
            for download in downloads:
                self.stats['total_files'] += 1
                title = download.get('title', '文档')
                
                # 创建一个临时路径，实际路径会在download_file中重新生成
                temp_filepath = category_dir / "temp.pdf"
                
                self.download_file(download['url'], temp_filepath, title, category_name)
                time.sleep(1)  # 避免请求过快
            
            # 查找下一页链接
            next_page_url = self.find_next_page(current_url)
            if next_page_url and next_page_url != current_url:
                current_url = next_page_url
                page_num += 1
                time.sleep(2)  # 页面间暂停
            else:
                break
        
        self.logger.info(f"完成爬取产品分类: {category_name}，共 {page_num} 页")
    
    def find_next_page(self, current_url: str) -> Optional[str]:
        """
        查找下一页链接
        
        Args:
            current_url: 当前页面URL
            
        Returns:
            下一页URL或None
        """
        response = self.safe_request(current_url)
        if not response:
            return None
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # 方法1：查找岛电网站特有的分页导航
        # 查找包含"当前页:X/Y/总记录:Z条"的分页信息
        pagination_text = soup.find(text=lambda x: x and '当前页:' in x and '总记录:' in x)
        if pagination_text:
            self.logger.info(f"找到分页信息: {pagination_text.strip()}")
            
            # 提取当前页和总页数
            page_match = re.search(r'当前页:(\d+)/(\d+)', pagination_text)
            if page_match:
                current_page = int(page_match.group(1))
                total_pages = int(page_match.group(2))
                
                if current_page < total_pages:
                    # 构建下一页URL
                    next_page = current_page + 1
                    if 'p=' in current_url:
                        # 替换页码参数
                        next_url = re.sub(r'p=\d+', f'p={next_page}', current_url)
                    else:
                        # 添加页码参数
                        separator = '&' if '?' in current_url else '?'
                        next_url = f"{current_url}{separator}p={next_page}"
                    
                    self.logger.info(f"构建下一页URL: {next_url}")
                    return next_url
        
        # 方法2：查找分页导航链接
        pagination = soup.find(['div', 'ul'], class_=lambda x: x and any(keyword in str(x).lower() for keyword in ['page', 'pagination', 'nav']))
        
        if pagination:
            # 查找"下一页"或">"链接
            next_links = pagination.find_all('a', text=lambda x: x and any(keyword in x for keyword in ['下一页', '>', 'next', '»']))
            
            for link in next_links:
                href = link.get('href', '')
                if href and href != current_url:
                    if href.startswith('http'):
                        return href
                    elif href.startswith('/'):
                        return urljoin(self.base_url, href)
                    else:
                        return urljoin(current_url, href)
        
        # 方法3：查找数字页码链接
        page_links = soup.find_all('a', href=True)
        current_page_num = self.extract_page_number(current_url)
        
        for link in page_links:
            href = link.get('href', '')
            text = link.get_text(strip=True)
            
            # 检查是否是数字页码
            if text.isdigit():
                link_page_num = int(text)
                if link_page_num == current_page_num + 1:
                    if href.startswith('http'):
                        return href
                    elif href.startswith('/'):
                        return urljoin(self.base_url, href)
                    else:
                        return urljoin(current_url, href)
        
        return None
    
    def extract_page_number(self, url: str) -> int:
        """
        从URL中提取页码
        
        Args:
            url: URL字符串
            
        Returns:
            页码，默认为1
        """
        # 尝试从URL参数中提取页码
        match = re.search(r'[?&]page=(\d+)', url)
        if match:
            return int(match.group(1))
        
        # 尝试从路径中提取页码
        match = re.search(r'/(\d+)(?:\.html?)?$', url)
        if match:
            return int(match.group(1))
        
        return 1

    def crawl_module_with_pagination(self, classid: int, base_dir: Path, module_name: str):
        """
        爬取需要翻页的模块（如产品规格书）
        
        Args:
            classid: 分类ID
            base_dir: 基础目录
            module_name: 模块名称
        """
        self.logger.info(f"开始爬取带翻页的模块: {module_name}")
        
        page_num = 1
        current_url = f"{self.base_url}/articles.php?classid={classid}"
        total_downloads = 0
        
        while current_url:
            self.logger.info(f"爬取 {module_name} 第 {page_num} 页: {current_url}")
            
            # 解析当前页面
            downloads = self.parse_download_page(current_url)
            
            if not downloads:
                self.logger.info(f"{module_name} 第 {page_num} 页没有找到下载链接")
                break
            
            # 下载当前页面的文件
            for download in downloads:
                self.stats['total_files'] += 1
                title = download.get('title', '文档')
                
                # 创建一个临时路径，实际路径会在download_file中重新生成
                temp_filepath = base_dir / "temp.pdf"
                
                if self.download_file(download['url'], temp_filepath, title, module_name):
                    total_downloads += 1
                
                time.sleep(1)  # 避免请求过快
            
            # 查找下一页链接
            next_page_url = self.find_next_page(current_url)
            if next_page_url and next_page_url != current_url:
                current_url = next_page_url
                page_num += 1
                time.sleep(2)  # 页面间暂停
            else:
                break
        
        self.logger.info(f"完成爬取模块: {module_name}，共 {page_num} 页，{total_downloads} 个文件")

    def crawl_discontinued_products(self, classid: int, base_dir: Path):
        """
        爬取停产产品模块（多层级结构）
        
        Args:
            classid: 分类ID
            base_dir: 基础目录
        """
        self.logger.info("爬取停产产品模块")
        
        # 明确定义停产产品的分类ID和名称
        discontinued_products = [
            {'name': 'SD16A', 'classid': 23, 'url': 'http://www.yhxml.com/articles.php?classid=23'},
            {'name': 'SR1/3/4', 'classid': 38, 'url': 'http://www.yhxml.com/articles.php?classid=38'},
            {'name': 'SR253', 'classid': 41, 'url': 'http://www.yhxml.com/articles.php?classid=41'},
            {'name': 'FP21', 'classid': 42, 'url': 'http://www.yhxml.com/articles.php?classid=42'}
        ]
        
        for subcat in discontinued_products:
            self.logger.info(f"处理停产产品子分类: {subcat['name']}")
            
            subcat_dir = base_dir / self.sanitize_folder_name(subcat['name'])
            subcat_dir.mkdir(parents=True, exist_ok=True)
            
            # 爬取子分类页面
            downloads = self.parse_download_page(subcat['url'])
            
            for download in downloads:
                self.stats['total_files'] += 1
                # 使用title来生成更好的文件名
                title = download.get('title', '文档')
                # 创建一个临时路径，实际路径会在download_file中重新生成
                temp_filepath = subcat_dir / "temp.pdf"
                
                self.download_file(download['url'], temp_filepath, title, '停产产品')
                time.sleep(1)

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

    def save_progress(self):
        """保存爬取进度"""
        progress_file = self.base_dir / 'crawl_progress.json'
        progress_data = {
            'timestamp': time.time(),
            'stats': self.stats,
            'completed_modules': []
        }
        
        with open(progress_file, 'w', encoding='utf-8') as f:
            json.dump(progress_data, f, ensure_ascii=False, indent=2)

    def run(self):
        """运行爬虫"""
        self.logger.info("开始爬取北京友方金泰下载中心")
        self.logger.info(f"保存目录: {self.base_dir}")
        
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
            self.logger.info(f"下载失败: {self.stats['failed_files']}")
            self.logger.info(f"保存目录: {self.base_dir}")
            
            # 发送通知
            if self.new_files:
                self.send_notifications()

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
            self.logger.info(f"📊 发现 {len(self.new_files)} 个新文件:")
            
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
                self.logger.info(f"  📄 {file_info['title']}{size_str}")
            
            if len(self.new_files) > 5:
                self.logger.info(f"  ... 还有 {len(self.new_files) - 5} 个文件")
                
            self.logger.info(f"\n💾 所有文件已保存至: {self.base_dir}")
        
            # 钉钉通知
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            total_files = len(self.new_files)
            success_rate = 100.0 if self.stats['failed_files'] == 0 else (self.stats['downloaded_files'] / (self.stats['downloaded_files'] + self.stats['failed_files'])) * 100
            
            if self.is_first_run:
                # 第一次全量爬取通知
                message = f"""✅ 友方金泰 爬取成功，请及时审核

📊 下载统计:
  成功下载: {total_files} 个文件
  总文件数: {self.stats['total_files']} 个文件
  成功率: {success_rate:.1f}%

📁 文件存放路径: {self.base_dir}
⏰ 完成时间: {current_time}"""
            else:
                # 增量爬取通知
                message = f"""✅ 友方金泰 增量爬取成功，请及时审核

📊 下载统计:
  成功下载: {total_files} 个文件
  总文件数: {self.stats['total_files']} 个文件
  成功率: {success_rate:.1f}%
文件明细："""
                
                # 添加文件明细
                for file_info in self.new_files:
                    # 构建相对路径（从基础目录开始）
                    relative_path = file_info['path'].replace(str(self.base_dir) + '/', '')
                    message += f"\n{relative_path}"
                
                message += f"""

📁 文件存放路径: {self.base_dir}
⏰ 完成时间: {current_time}"""
            
            # 发送钉钉通知
            self.send_dingtalk_notification(message)
            
        except Exception as e:
            self.logger.error(f"发送通知失败: {e}")


def main():
    """主函数"""
    # 可以通过命令行参数指定保存目录，默认为当前目录下的"友方金泰下载"文件夹
    base_dir = sys.argv[1] if len(sys.argv) > 1 else None
    
    spider = YHXMLSpider(base_dir)
    spider.run()


if __name__ == "__main__":
    main()
