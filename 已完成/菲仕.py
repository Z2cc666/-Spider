#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
菲仕科技 (Physis) 产品资料下载爬虫
网站：https://www.physis.com.cn/ProductCenter3992/index.aspx?lcid=31
功能：
1. 爬取所有产品列表
2. 进入每个产品页面下载规格参数PDF
3. 下载资料下载模块的所有文件（型录手册、3D图纸等）
"""

import os
import sys
import time
import requests
from urllib.parse import urljoin, urlparse, quote
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
from datetime import datetime, date, timedelta
try:
    import html2text
    from weasyprint import HTML, CSS
    HTML2TEXT_AVAILABLE = True
    WEASYPRINT_AVAILABLE = True
except ImportError:
    HTML2TEXT_AVAILABLE = False
    WEASYPRINT_AVAILABLE = False
    print("⚠️ html2text 或 weasyprint 未安装，PDF生成功能将使用简化版本")

from io import BytesIO
import tempfile

class PhysisSpider:
    def __init__(self, base_dir: str = None, monitor_mode: bool = False, category_filter: str = None):
        """
        初始化菲仕科技爬虫
        
        Args:
            base_dir: 下载文件保存的基础目录
            monitor_mode: 监控模式，只检测新文件而不下载
            category_filter: 类别过滤器，如果指定则只爬取该类别的产品
        """
        if base_dir is None:
            # 默认使用服务器标准目录
            base_dir = "/srv/downloads/approved/菲仕"
        
        self.base_url = "https://www.physis.com.cn"
        self.base_dir = Path(base_dir)
        self.session = requests.Session()
        self.monitor_mode = monitor_mode
        self.category_filter = category_filter
        
        # 设置请求头
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
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
        
        # 下载统计
        self.stats = {
            'total_products': 0,
            'processed_products': 0,
            'downloaded_files': 0,
            'failed_files': 0,
            'skipped_files': 0
        }
        
        # 产品中心URL
        self.product_center_url = "/ProductCenter3992/index.aspx?lcid=31"
        
        # 下载中心URLs
        self.download_center_urls = [
            "/DownloadCenter/list.aspx?lcid=8",   # 型录手册
            "/DownloadCenter/list.aspx?lcid=18",  # 3D图纸  
            "/DownloadCenter/list.aspx?lcid=7",   # 操作说明书
            "/DownloadCenter/list.aspx?lcid=6",   # 驱动器配置软件
        ]
        
        # 日期过滤：2024年11月后
        self.filter_date = datetime(2024, 11, 1)

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
            response = self.session.get(url, timeout=timeout, verify=False)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            self.logger.error(f"请求失败 {url}: {e}")
            return None

    def get_product_categories(self) -> List[Dict]:
        """
        获取产品分类列表
        
        Returns:
            产品分类列表
        """
        categories = []
        
        # 基于网站实际结构，菲仕科技的产品分类
        category_mapping = {
            '伺服电机系列': {
                'subcategories': [
                    'Ultract Ⅲ系列标准交流永磁同步伺服电机',
                    'TK系列永磁同步力矩伺服电机', 
                    'XT系列直驱力矩电机',
                    'Express系列交流永磁同步伺服电机'
                ]
            },
            '伺服驱动器系列': {
                'subcategories': [
                    'PH590系列高性能通用变频器',
                    'PD120系列高性能书本型网络化伺服驱动器',
                    'AxN-PD系列多传伺服驱动器',
                    'PH600系列高性能电液伺服驱动器',
                    'AxN系列全数字交流伺服驱动器',
                    'AxN-DC系列共直流母线多轴驱动器',
                    'PH300系列高性能闭环矢量驱动器'
                ]
            },
            'OSAI控制系统': {
                'subcategories': [
                    'OSAI数控系统',
                    'OSAI操作面板',
                    '工业PC（IPC）',
                    '输入/输出模块'
                ]
            },
            '直流无刷电机系列': {
                'subcategories': [
                    'BLDC内转子系列直流无刷电机',
                    'BLDC外转子系列直流无刷电机'
                ]
            },
            '新能源汽车电驱系列': {
                'subcategories': [
                    '三合一动力总成',
                    '主驱电机控制器',
                    '新能源汽车主驱电机',
                    '减速器'
                ]
            }
        }
        
        return category_mapping

    def get_all_products(self) -> List[Dict]:
        """
        从产品中心获取所有产品列表
        
        Returns:
            产品列表
        """
        products = []
        
        try:
            # 访问产品中心页面
            product_center_url = self.base_url + self.product_center_url
            response = self.safe_request(product_center_url)
            if not response:
                return products
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # 查找产品链接 - 基于实际页面结构分析
            # 产品通常在导航菜单或产品网格中
            product_links = soup.find_all('a', href=True)
            
            for link in product_links:
                href = link.get('href', '')
                text = link.get_text(strip=True)
                
                # 匹配产品页面的URL模式
                if ('ProductCenter3992/info.aspx' in href and 
                    'itemid=' in href and 
                    text and len(text) > 2):
                    
                    # 构建完整URL
                    if href.startswith('/'):
                        full_url = self.base_url + href
                    elif href.startswith('http'):
                        full_url = href
                    else:
                        full_url = urljoin(product_center_url, href)
                    
                    # 尝试分类产品
                    category = self.classify_product(text)
                    
                    products.append({
                        'name': text,
                        'url': full_url,
                        'category': category
                    })
                    
        except Exception as e:
            self.logger.error(f"获取产品列表失败: {e}")
        
        return products

    def classify_product(self, product_name: str) -> str:
        """
        根据产品名称分类产品
        
        Args:
            product_name: 产品名称
            
        Returns:
            产品分类
        """
        product_name_lower = product_name.lower()
        
        # 根据产品名称关键词分类
        if any(keyword in product_name_lower for keyword in ['ultract', 'tk系列', 'xt系列', 'express', '伺服电机']):
            return '伺服电机系列'
        elif any(keyword in product_name_lower for keyword in ['ph590', 'pd120', 'axn', 'ph600', 'ph300', '驱动器', '变频器']):
            return '伺服驱动器系列'
        elif any(keyword in product_name_lower for keyword in ['osai', '数控', 'ipc', '控制']):
            return 'OSAI控制系统'
        elif any(keyword in product_name_lower for keyword in ['bldc', '直流', '无刷']):
            return '直流无刷电机系列'
        elif any(keyword in product_name_lower for keyword in ['新能源', '汽车', '电驱', '动力总成']):
            return '新能源汽车电驱系列'
        else:
            return '其他产品'

    def download_spec_params_as_pdf(self, product_url: str, product_name: str, save_dir: Path) -> bool:
        """
        下载产品的规格参数为PDF
        
        Args:
            product_url: 产品页面URL
            product_name: 产品名称
            save_dir: 保存目录
            
        Returns:
            是否下载成功
        """
        try:
            response = self.safe_request(product_url)
            if not response:
                return False
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # 查找规格参数部分
            spec_section = soup.find('div', class_='box_item box4')
            if not spec_section:
                # 尝试其他可能的选择器
                spec_section = soup.find('div', string=re.compile('规格参数'))
                if spec_section:
                    spec_section = spec_section.find_parent('div')
            
            if not spec_section:
                self.logger.warning(f"未找到规格参数部分: {product_name}")
                return False
            
            # 提取规格参数内容
            spec_content = ""
            
            # 查找规格参数的图片
            spec_images = spec_section.find_all('img')
            if spec_images:
                spec_content += f"<h1>{product_name} - 规格参数</h1>\n"
                for img in spec_images:
                    img_src = img.get('src')
                    if img_src:
                        # 构建完整的图片URL
                        if img_src.startswith('/'):
                            img_url = self.base_url + img_src
                        elif img_src.startswith('http'):
                            img_url = img_src
                        else:
                            img_url = urljoin(product_url, img_src)
                        
                        spec_content += f'<img src="{img_url}" style="max-width: 100%; margin: 10px 0;" />\n'
            
            # 如果有文本内容，也包含进来
            spec_text = spec_section.get_text(strip=True)
            if spec_text and not spec_images:
                spec_content += f"<h1>{product_name} - 规格参数</h1>\n"
                spec_content += f"<pre>{spec_text}</pre>\n"
            
            if not spec_content:
                self.logger.warning(f"规格参数内容为空: {product_name}")
                return False
            
            # 生成PDF文件名
            pdf_filename = f"{self.sanitize_filename(product_name)}_规格参数.pdf"
            pdf_path = save_dir / pdf_filename
            
            # 检查文件是否已存在
            if pdf_path.exists():
                self.logger.info(f"规格参数PDF已存在，跳过: {pdf_filename}")
                return True
            
            # 生成PDF
            if WEASYPRINT_AVAILABLE:
                # 使用weasyprint生成PDF
                html_content = f"""
                <!DOCTYPE html>
                <html>
                <head>
                    <meta charset="utf-8">
                    <title>{product_name} - 规格参数</title>
                    <style>
                        body {{ font-family: "Microsoft YaHei", Arial, sans-serif; margin: 20px; }}
                        h1 {{ color: #333; text-align: center; }}
                        img {{ max-width: 100%; height: auto; }}
                        pre {{ white-space: pre-wrap; }}
                    </style>
                </head>
                <body>
                    {spec_content}
                </body>
                </html>
                """
                
                HTML(string=html_content, base_url=self.base_url).write_pdf(str(pdf_path))
            else:
                # 如果没有weasyprint，保存为HTML文件
                html_filename = f"{self.sanitize_filename(product_name)}_规格参数.html"
                html_path = save_dir / html_filename
                
                html_content = f"""
                <!DOCTYPE html>
                <html>
                <head>
                    <meta charset="utf-8">
                    <title>{product_name} - 规格参数</title>
                    <style>
                        body {{ font-family: "Microsoft YaHei", Arial, sans-serif; margin: 20px; }}
                        h1 {{ color: #333; text-align: center; }}
                        img {{ max-width: 100%; height: auto; }}
                        pre {{ white-space: pre-wrap; }}
                    </style>
                </head>
                <body>
                    {spec_content}
                </body>
                </html>
                """
                
                with open(html_path, 'w', encoding='utf-8') as f:
                    f.write(html_content)
                
                pdf_path = html_path  # 更新路径引用
                self.logger.info(f"规格参数HTML生成成功（PDF库不可用）: {html_filename}")
            
            self.logger.info(f"规格参数PDF生成成功: {pdf_filename}")
            
            # 记录到新文件列表
            self.new_files.append({
                'type': 'PDF',
                'title': f"{product_name}_规格参数",
                'path': str(pdf_path),
                'url': product_url,
                'size': pdf_path.stat().st_size
            })
            
            self.stats['downloaded_files'] += 1
            return True
            
        except Exception as e:
            self.logger.error(f"生成规格参数PDF失败 {product_name}: {e}")
            self.stats['failed_files'] += 1
            return False

    def download_product_resources(self, product_url: str, product_name: str, save_dir: Path) -> int:
        """
        下载产品的资料下载模块文件
        
        Args:
            product_url: 产品页面URL
            product_name: 产品名称
            save_dir: 保存目录
            
        Returns:
            下载成功的文件数量
        """
        downloaded_count = 0
        
        try:
            response = self.safe_request(product_url)
            if not response:
                return 0
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # 方法1: 查找资料下载部分 - 新的选择器策略
            download_section = None
            
            # 尝试多种方式找到资料下载部分
            selectors_to_try = [
                'div.auto.auto_1500',
                'div[class*="auto"]',
                'div[id*="download"]',
                'div[class*="download"]'
            ]
            
            for selector in selectors_to_try:
                sections = soup.select(selector)
                for section in sections:
                    if '资料下载' in section.get_text():
                        download_section = section
                        break
                if download_section:
                    break
            
            # 如果还没找到，尝试直接搜索文本
            if not download_section:
                download_texts = soup.find_all(text=re.compile('资料下载'))
                for text in download_texts:
                    parent = text.parent
                    while parent and parent.name != 'body':
                        if parent.name == 'div':
                            download_section = parent
                            break
                        parent = parent.parent
                    if download_section:
                        break
            
            if not download_section:
                self.logger.warning(f"未找到资料下载部分: {product_name}")
                
                # 方法2: 直接查找下载链接（基于实际页面结构）
                # 查找所有可能的下载链接
                all_links = soup.find_all('a', href=True)
                valid_downloads = []
                
                for link in all_links:
                    href = link.get('href', '')
                    text = link.get_text(strip=True)
                    
                    # 检查是否是下载链接（基于href模式或文本内容）
                    if (href and text and 
                        (any(ext in href.lower() for ext in ['.pdf', '.doc', '.zip', '.rar']) or
                         any(keyword in text for keyword in ['下载', '文档', '手册', '图纸', '参数']) or
                         'download' in href.lower() or
                         'attachment' in href.lower())):
                        
                        # 构建完整URL
                        if href.startswith('/'):
                            file_url = self.base_url + href
                        elif href.startswith('http'):
                            file_url = href
                        else:
                            file_url = urljoin(product_url, href)
                        
                        # 检查是否已处理过
                        if file_url not in self.processed_urls:
                            valid_downloads.append((file_url, text))
                
                # 下载找到的文件
                if valid_downloads:
                    self.logger.info(f"通过直接链接搜索找到 {len(valid_downloads)} 个下载文件")
                    for file_url, title in valid_downloads:
                        if self.download_file(file_url, title, save_dir):
                            downloaded_count += 1
                            self.processed_urls.add(file_url)
                        time.sleep(1)
                
                return downloaded_count
            
            # 方法3: 解析找到的资料下载部分
            self.logger.info(f"找到资料下载部分: {product_name}")
            
            # 查找所有下载分类（型录手册、3D图纸等）
            tab_sections = download_section.find_all('ul', class_='ul clearfix')
            
            # 如果没有找到ul结构，尝试其他结构
            if not tab_sections:
                # 查找所有列表项或下载链接
                download_items = download_section.find_all(['li', 'a'], href=True)
                if download_items:
                    tab_sections = [download_section]  # 将整个section作为一个tab处理
            
            for tab_section in tab_sections:
                # 获取分类名称
                category_name = tab_section.get('data-name', '产品资料')
                
                # 查找该分类下的所有下载链接
                download_links = tab_section.find_all('a', href=True)
                
                # 如果没有找到a标签，尝试查找其他可能的下载元素
                if not download_links:
                    # 查找带有下载相关文本的元素
                    possible_links = tab_section.find_all(text=re.compile(r'[A-Z0-9_-]+\.[a-z]{2,4}$'))
                    for text in possible_links:
                        # 为纯文本文件名创建下载链接（可能需要根据实际情况调整）
                        parent = text.parent
                        if parent and parent.name == 'li':
                            # 尝试构建下载URL
                            filename = text.strip()
                            # 这里可能需要根据菲仕网站的实际下载URL模式调整
                            potential_url = f"{self.base_url}/download/{filename}"
                            download_links.append(type('obj', (object,), {
                                'get': lambda self, attr, default='': filename if attr == 'text' else potential_url if attr == 'href' else default,
                                'get_text': lambda strip=False: filename
                            })())
                
                # 先收集有效的下载链接
                valid_downloads = []
                for link in download_links:
                    href = link.get('href', '') if hasattr(link, 'get') else ''
                    title = link.get_text(strip=True) if hasattr(link, 'get_text') else str(link)
                    
                    if href and title:
                        # 构建完整URL
                        if href.startswith('/'):
                            file_url = self.base_url + href
                        elif href.startswith('http'):
                            file_url = href
                        else:
                            file_url = urljoin(product_url, href)
                        
                        # 检查是否已处理过
                        if file_url not in self.processed_urls:
                            valid_downloads.append((file_url, title))
                
                # 只有在有有效下载文件时才创建目录
                if valid_downloads:
                    category_dir = save_dir / self.sanitize_filename(category_name)
                    category_dir.mkdir(parents=True, exist_ok=True)
                    
                    # 下载文件
                    for file_url, title in valid_downloads:
                        if self.download_file(file_url, title, category_dir):
                            downloaded_count += 1
                            self.processed_urls.add(file_url)
                        else:
                            self.logger.info(f"文件已处理过，跳过: {title}")
                        
                        time.sleep(1)  # 避免请求过快
            
        except Exception as e:
            self.logger.error(f"下载产品资料失败 {product_name}: {e}")
        
        return downloaded_count

    def clean_filename(self, filename: str) -> str:
        """
        清理文件名，移除非法字符
        
        Args:
            filename: 原始文件名
            
        Returns:
            清理后的文件名
        """
        # 移除非法字符
        import re
        cleaned = re.sub(r'[<>:"/\\|?*]', '_', filename)
        cleaned = cleaned.strip()
        
        # 如果文件名太长，截断它
        if len(cleaned) > 200:
            cleaned = cleaned[:200]
            
        return cleaned

    def download_file(self, url: str, filename: str, save_dir: Path) -> bool:
        """
        下载单个文件
        
        Args:
            url: 文件URL
            filename: 文件名
            save_dir: 保存目录
            
        Returns:
            是否下载成功
        """
        try:
            # 清理文件名
            clean_filename = self.sanitize_filename(filename)
            
            # 如果URL中有文件扩展名，使用URL中的扩展名
            url_path = urlparse(url).path
            if '.' in url_path:
                url_ext = os.path.splitext(url_path)[1]
                if url_ext and not clean_filename.endswith(url_ext):
                    clean_filename += url_ext
            
            file_path = save_dir / clean_filename
            
            # 检查文件是否已存在
            if file_path.exists():
                self.logger.info(f"文件已存在，跳过: {clean_filename}")
                return True
            
            # 下载文件
            self.logger.info(f"开始下载: {clean_filename}")
            response = self.safe_request(url)
            if not response:
                return False
            
            # 保存文件
            with open(file_path, 'wb') as f:
                f.write(response.content)
            
            file_size = file_path.stat().st_size
            
            self.logger.info(f"下载成功: {clean_filename} ({file_size} bytes)")
            
            # 记录到新文件列表
            self.new_files.append({
                'type': 'PDF' if file_path.suffix.lower() == '.pdf' else '文档',
                'title': filename,
                'path': str(file_path),
                'url': url,
                'size': file_size
            })
            
            self.stats['downloaded_files'] += 1
            return True
            
        except Exception as e:
            self.logger.error(f"下载文件失败 {url}: {e}")
            self.stats['failed_files'] += 1
            return False

    def sanitize_filename(self, filename: str) -> str:
        """
        清理文件名，移除特殊字符
        
        Args:
            filename: 原始文件名
            
        Returns:
            清理后的文件名
        """
        # 移除或替换特殊字符
        filename = re.sub(r'[<>:"/\\|?*\[\]{}]', '_', filename.strip())
        filename = re.sub(r'\s+', '_', filename)
        return filename

    def process_product(self, product_info: Dict) -> bool:
        """
        处理单个产品
        
        Args:
            product_info: 产品信息
            
        Returns:
            是否处理成功
        """
        product_name = product_info['name']
        product_url = product_info['url']
        category = product_info['category']
        
        try:
            self.logger.info(f"处理产品: {category} -> {product_name}")
            
            # 检查URL是否已处理过
            if product_url in self.processed_urls:
                self.logger.info(f"产品已处理过，跳过: {product_name}")
                return True
            
            # 创建临时目录来测试是否有内容
            category_dir = self.base_dir / self.sanitize_filename(category)
            product_dir = category_dir / self.sanitize_filename(product_name)
            
            # 先检查是否有内容需要下载，不立即创建目录
            has_content = False
            
            # 1. 检查规格参数PDF
            self.logger.info(f"检查规格参数PDF: {product_name}")
            spec_success = False
            try:
                # 先不传递目录，只检查是否有内容
                response = self.safe_request(product_url)
                if response:
                    soup = BeautifulSoup(response.content, 'html.parser')
                    spec_section = soup.find('div', class_='box_item box4')
                    if not spec_section:
                        spec_section = soup.find('div', string=re.compile('规格参数'))
                        if spec_section:
                            spec_section = spec_section.find_parent('div')
                    
                    if spec_section:
                        # 有规格参数内容
                        has_content = True
                        spec_success = True
            except Exception as e:
                self.logger.warning(f"检查规格参数时出错: {e}")
            
            # 2. 检查资料下载模块
            self.logger.info(f"检查产品资料: {product_name}")
            resource_count = 0
            try:
                response = self.safe_request(product_url)
                if response:
                    soup = BeautifulSoup(response.content, 'html.parser')
                    download_section = soup.find('div', class_='auto auto_1500')
                    if download_section and '资料下载' in download_section.get_text():
                        pass
                    else:
                        download_sections = soup.find_all('div', string=re.compile('资料下载'))
                        if download_sections:
                            download_section = download_sections[0].find_parent('div')
                    
                    if download_section:
                        tab_sections = download_section.find_all('ul', class_='ul clearfix')
                        for tab_section in tab_sections:
                            download_links = tab_section.find_all('a', href=True)
                            for link in download_links:
                                href = link.get('href', '')
                                title = link.get_text(strip=True)
                                if href and title:
                                    # 有资料下载内容
                                    has_content = True
                                    break
                            if has_content:
                                break
            except Exception as e:
                self.logger.warning(f"检查产品资料时出错: {e}")
            
            # 只有在确实有内容时才创建目录并下载
            if has_content:
                product_dir.mkdir(parents=True, exist_ok=True)
                
                # 执行实际下载
                if spec_success:
                    self.logger.info(f"下载规格参数PDF: {product_name}")
                    self.download_spec_params_as_pdf(product_url, product_name, product_dir)
                
                self.logger.info(f"下载产品资料: {product_name}")
                resource_count = self.download_product_resources(product_url, product_name, product_dir)
                
                self.logger.info(f"产品 {product_name} 处理完成，下载 {resource_count} 个资料文件")
            else:
                self.logger.info(f"产品 {product_name} 没有可下载的内容，跳过目录创建")
            
            # 记录已处理的URL
            self.processed_urls.add(product_url)
            self.stats['processed_products'] += 1
            
            return True
            
        except Exception as e:
            self.logger.error(f"处理产品失败 {product_name}: {e}")
            return False

    def save_progress(self):
        """保存爬取进度"""
        progress_file = self.base_dir / 'crawl_progress.json'
        progress_data = {
            'timestamp': time.time(),
            'stats': self.stats,
            'completed_products': self.stats['processed_products']
        }
        
        with open(progress_file, 'w', encoding='utf-8') as f:
            json.dump(progress_data, f, ensure_ascii=False, indent=2)

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
            
            message = f"""✅ 菲仕科技 产品资料下载爬取成功，请及时审核

📊 下载统计:
  成功下载: {total_files} 个文件
  处理产品: {self.stats['processed_products']} 个
  总产品数: {self.stats['total_products']} 个
  成功率: {success_rate:.1f}%

📁 文件存放路径: {self.base_dir}
⏰ 完成时间: {current_time}
🔧 包含内容: 规格参数PDF、型录手册、3D图纸等"""
            
            # 发送钉钉通知
            self.send_dingtalk_notification(message)
            
        except Exception as e:
            self.logger.error(f"发送通知失败: {e}")

    def run(self):
        """运行爬虫"""
        self.logger.info("开始爬取菲仕科技产品资料")
        self.logger.info(f"保存目录: {self.base_dir}")
        
        start_time = time.time()
        
        try:
            # 获取所有产品
            self.logger.info("获取产品列表...")
            products = self.get_all_products()
            self.stats['total_products'] = len(products)
            
            if not products:
                self.logger.warning("没有找到任何产品")
                return
            
            self.logger.info(f"找到 {len(products)} 个产品")
            
            # 按分类统计
            category_counts = {}
            for product in products:
                category = product['category']
                category_counts[category] = category_counts.get(category, 0) + 1
            
            for category, count in category_counts.items():
                self.logger.info(f"  {category}: {count} 个产品")
            
            # 如果指定了类别过滤器，过滤产品
            if self.category_filter:
                filtered_products = [p for p in products if p['category'] == self.category_filter]
                self.logger.info(f"类别过滤器: {self.category_filter}")
                self.logger.info(f"过滤后产品数量: {len(filtered_products)}")
                products = filtered_products
                self.stats['total_products'] = len(products)
            
            # 处理每个产品
            for i, product in enumerate(products):
                self.logger.info(f"=" * 50)
                self.logger.info(f"进度: {i+1}/{len(products)} - {product['category']}")
                
                self.process_product(product)
                time.sleep(2)  # 产品间暂停
                
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
            self.logger.info(f"总产品数: {self.stats['total_products']}")
            self.logger.info(f"处理产品: {self.stats['processed_products']}")
            self.logger.info(f"下载成功: {self.stats['downloaded_files']}")
            self.logger.info(f"下载失败: {self.stats['failed_files']}")
            self.logger.info(f"保存目录: {self.base_dir}")
            
            # 发送通知
            if self.new_files:
                self.send_notifications()

    def parse_download_center_page(self, url: str) -> List[Dict]:
        """
        解析下载中心页面，提取文件信息
        
        Args:
            url: 下载中心页面URL
            
        Returns:
            文件信息列表
        """
        files = []
        
        try:
            response = self.safe_request(url)
            if not response:
                return files
                
            soup = BeautifulSoup(response.content, 'html.parser')
            content = response.text
            
            self.logger.info(f"正在解析页面: {url}")
            self.logger.info(f"页面内容长度: {len(content)} 字符")
            
            # 检查页面类型
            if 'ProductCenter' in content and 'DownloadCenter' not in content:
                self.logger.warning(f"收到的是产品中心页面而不是下载中心页面: {url}")
                return files
            
            # 检查是否是空页面或无访问权限
            if '暂无数据' in content or '无权限' in content or len(content) < 1000:
                self.logger.warning(f"页面可能为空或无访问权限: {url}")
                return files
            
            # 方法1: 查找 TextList002208 容器
            text_list = soup.find('div', class_='TextList002208')
            if text_list:
                # 查找所有的文件条目 (dl 元素)
                file_items = text_list.find_all('dl', class_='dl')
                
                for item in file_items:
                    try:
                        # 在每个 dl 中查找 dt 元素
                        dt = item.find('dt', class_='dt')
                        if not dt:
                            continue
                            
                        # 提取各个字段的 span 元素
                        spans = dt.find_all('span')
                        if len(spans) < 6:  # 名称、版本、格式、大小、日期、下载
                            continue
                            
                        name = spans[0].get_text(strip=True)
                        version = spans[1].get_text(strip=True)
                        file_format = spans[2].get_text(strip=True)
                        size = spans[3].get_text(strip=True)
                        date_str = spans[4].get_text(strip=True)
                        
                        # 查找下载链接
                        download_link = None
                        download_span = spans[5]
                        link_tag = download_span.find('a', href=True)
                        if link_tag:
                            href = link_tag.get('href')
                            if href:
                                download_link = urljoin(self.base_url, href)
                        
                        # 解析日期
                        file_date = None
                        if date_str:
                            try:
                                # 尝试不同的日期格式
                                for date_format in ['%Y-%m-%d', '%Y.%m.%d', '%Y/%m/%d']:
                                    try:
                                        file_date = datetime.strptime(date_str, date_format)
                                        break
                                    except ValueError:
                                        continue
                                
                                # 如果还不行，尝试解析更复杂的格式
                                if not file_date and len(date_str) >= 8:
                                    # 尝试只提取日期部分
                                    import re
                                    date_match = re.search(r'(\d{4}[-./]\d{1,2}[-./]\d{1,2})', date_str)
                                    if date_match:
                                        clean_date = date_match.group(1).replace('.', '-').replace('/', '-')
                                        file_date = datetime.strptime(clean_date, '%Y-%m-%d')
                                    
                            except Exception as e:
                                self.logger.warning(f"日期解析失败 {date_str}: {e}")
                        
                        # 只收集2024年11月后的文件
                        if file_date and file_date >= self.filter_date and download_link and name:
                            files.append({
                                'name': name,
                                'version': version,
                                'format': file_format,
                                'size': size,
                                'date': file_date,
                                'date_str': date_str,
                                'download_url': download_link,
                                'source_url': url
                            })
                            self.logger.info(f"找到符合条件的文件: {name} ({date_str})")
                            
                    except Exception as e:
                        self.logger.warning(f"解析文件条目失败: {e}")
                        continue
            
            # 如果没有找到 TextList002208，尝试其他方法
            if not files:
                # 方法1: 查找表格
                tables = soup.find_all('table')
                for table in tables:
                    rows = table.find_all('tr')
                    
                    for row in rows[1:]:  # 跳过表头
                        cells = row.find_all(['td', 'th'])
                        
                        if len(cells) >= 4:  # 至少需要名称、格式、大小、日期
                            try:
                                name = cells[0].get_text(strip=True)
                                
                                # 尝试不同的列顺序
                                if len(cells) >= 6:  # 名称 版本 格式 大小 日期 下载
                                    version = cells[1].get_text(strip=True)
                                    file_format = cells[2].get_text(strip=True) 
                                    size = cells[3].get_text(strip=True)
                                    date_str = cells[4].get_text(strip=True)
                                    download_cell = cells[5]
                                elif len(cells) >= 5:  # 名称 格式 大小 日期 下载
                                    version = ""
                                    file_format = cells[1].get_text(strip=True)
                                    size = cells[2].get_text(strip=True)
                                    date_str = cells[3].get_text(strip=True)
                                    download_cell = cells[4]
                                else:
                                    continue
                                
                                # 查找下载链接
                                download_link = None
                                link_tag = download_cell.find('a')
                                if link_tag and link_tag.get('href'):
                                    download_link = urljoin(self.base_url, link_tag.get('href'))
                                
                                # 解析日期
                                file_date = None
                                if date_str:
                                    try:
                                        # 尝试不同的日期格式
                                        for date_format in ['%Y-%m-%d', '%Y.%m.%d', '%Y/%m/%d', '%Y-%m-%d %H:%M:%S']:
                                            try:
                                                file_date = datetime.strptime(date_str, date_format)
                                                break
                                            except ValueError:
                                                continue
                                        
                                        # 如果还不行，尝试解析更复杂的格式
                                        if not file_date and len(date_str) >= 8:
                                            # 尝试只提取日期部分
                                            import re
                                            date_match = re.search(r'(\d{4}[-./]\d{1,2}[-./]\d{1,2})', date_str)
                                            if date_match:
                                                clean_date = date_match.group(1).replace('.', '-').replace('/', '-')
                                                file_date = datetime.strptime(clean_date, '%Y-%m-%d')
                                            
                                    except Exception as e:
                                        self.logger.warning(f"日期解析失败 {date_str}: {e}")
                                
                                # 只收集2024年11月后的文件
                                if file_date and file_date >= self.filter_date and download_link and name:
                                    files.append({
                                        'name': name,
                                        'version': version,
                                        'format': file_format,
                                        'size': size,
                                        'date': file_date,
                                        'date_str': date_str,
                                        'download_url': download_link,
                                        'source_url': url
                                    })
                                    self.logger.info(f"找到符合条件的文件: {name} ({date_str})")
                                    
                            except Exception as e:
                                self.logger.warning(f"解析文件行失败: {e}")
                                continue
            
            self.logger.info(f"从页面 {url} 解析到 {len(files)} 个符合条件的文件")
            return files
            
        except Exception as e:
            self.logger.error(f"解析下载中心页面失败 {url}: {e}")
            return files

    def get_download_center_files(self) -> List[Dict]:
        """
        获取所有下载中心的文件列表
        
        Returns:
            所有文件信息列表
        """
        all_files = []
        
        for download_url in self.download_center_urls:
            self.logger.info(f"正在爬取下载中心: {download_url}")
            
            # 获取分类名称
            category_map = {
                "lcid=8": "型录手册",
                "lcid=18": "3D图纸", 
                "lcid=7": "操作说明书",
                "lcid=6": "驱动器配置软件"
            }
            
            category = "未知分类"
            for key, value in category_map.items():
                if key in download_url:
                    category = value
                    break
            
            # 爬取所有页面
            page = 1
            while True:
                if page == 1:
                    page_url = self.base_url + download_url
                else:
                    page_url = self.base_url + download_url + f"&page={page}"
                
                self.logger.info(f"正在爬取 {category} 第{page}页: {page_url}")
                
                files = self.parse_download_center_page(page_url)
                if not files:
                    break
                
                # 添加分类信息
                for file_info in files:
                    file_info['category'] = category
                
                all_files.extend(files)
                
                # 检查是否有下一页
                # 这里需要检查页面是否有分页链接
                response = self.safe_request(page_url)
                if response:
                    soup = BeautifulSoup(response.content, 'html.parser')
                    # 查找"下一页"链接或页码链接
                    next_page_found = False
                    
                    # 查找分页链接
                    pagination_links = soup.find_all('a', href=True)
                    for link in pagination_links:
                        href = link.get('href', '')
                        if f'page={page + 1}' in href or '下一页' in link.get_text():
                            next_page_found = True
                            break
                    
                    if not next_page_found:
                        break
                else:
                    break
                
                page += 1
                time.sleep(1)  # 分页间暂停
            
            self.logger.info(f"{category} 爬取完成，共找到 {len([f for f in all_files if f.get('category') == category])} 个文件")
            time.sleep(2)  # 分类间暂停
        
        return all_files

    def download_download_center_file(self, file_info: Dict, download_dir: Path) -> bool:
        """
        下载下载中心的文件
        
        Args:
            file_info: 文件信息
            download_dir: 下载目录
            
        Returns:
            下载是否成功
        """
        try:
            category = file_info.get('category', '未知分类')
            name = file_info.get('name', '未知文件')
            file_format = file_info.get('format', '')
            download_url = file_info.get('download_url')
            
            if not download_url:
                self.logger.warning(f"文件没有下载链接: {name}")
                return False
            
            # 创建分类目录
            category_dir = download_dir / category
            category_dir.mkdir(parents=True, exist_ok=True)
            
            # 生成文件名
            safe_name = self.clean_filename(name)
            if file_format and not safe_name.lower().endswith(f'.{file_format.lower()}'):
                filename = f"{safe_name}.{file_format.lower()}"
            else:
                filename = safe_name
            
            file_path = category_dir / filename
            
            # 检查文件是否已存在
            if file_path.exists():
                self.logger.info(f"文件已存在，跳过下载: {filename}")
                return True
            
            # 下载文件
            self.logger.info(f"正在下载: {filename}")
            
            response = self.session.get(download_url, stream=True, timeout=30)
            response.raise_for_status()
            
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            file_size = file_path.stat().st_size
            self.logger.info(f"下载成功: {filename} ({file_size} bytes)")
            
            # 记录到新文件列表
            self.new_files.append({
                'type': file_format.upper() if file_format else 'FILE',
                'title': name,
                'path': str(file_path),
                'url': download_url,
                'size': file_size,
                'category': category,
                'date': file_info.get('date_str', '')
            })
            
            self.stats['downloaded_files'] += 1
            return True
            
        except Exception as e:
            self.logger.error(f"下载文件失败 {file_info.get('name', '未知')}: {e}")
            self.stats['failed_files'] += 1
            return False

    def check_download_center_updates(self):
        """
        检查下载中心更新并下载新文件
        """
        self.logger.info("=" * 50)
        self.logger.info("开始检查下载中心更新...")
        
        # 创建下载中心目录
        download_center_dir = self.base_dir / "下载中心"
        download_center_dir.mkdir(parents=True, exist_ok=True)
        
        # 获取所有文件
        all_files = self.get_download_center_files()
        
        if not all_files:
            self.logger.info("未找到符合条件的新文件")
            return
        
        self.logger.info(f"找到 {len(all_files)} 个符合条件的文件（2024年11月后）")
        
        # 按分类统计
        category_counts = {}
        for file_info in all_files:
            category = file_info.get('category', '未知')
            category_counts[category] = category_counts.get(category, 0) + 1
        
        for category, count in category_counts.items():
            self.logger.info(f"  {category}: {count} 个文件")
        
        # 下载所有文件
        for i, file_info in enumerate(all_files):
            self.logger.info(f"进度: {i+1}/{len(all_files)} - {file_info.get('category', '未知')}")
            self.logger.info(f"文件: {file_info.get('name', '未知')} ({file_info.get('date_str', '')})")
            
            success = self.download_download_center_file(file_info, download_center_dir)
            if success:
                self.logger.info("下载成功")
            else:
                self.logger.warning("下载失败")
            
            time.sleep(1)  # 文件间暂停
        
        self.logger.info("下载中心检查完成")


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='菲仕科技产品资料下载爬虫')
    parser.add_argument('--base-dir', type=str, help='下载文件保存的基础目录')
    parser.add_argument('--monitor', action='store_true', help='运行监控模式，只检测新文件不下载')
    parser.add_argument('--download-center', action='store_true', help='只爬取下载中心（2024年11月后的文件）')
    parser.add_argument('--products-only', action='store_true', help='只爬取产品中心，不包括下载中心')
    parser.add_argument('--osai-only', action='store_true', help='只爬取OSAI控制系统类别的产品')
    parser.add_argument('--category', type=str, help='指定要爬取的产品类别')
    
    args = parser.parse_args()
    
    # 确定类别过滤器
    category_filter = None
    if args.osai_only:
        category_filter = 'OSAI控制系统'
    elif args.category:
        category_filter = args.category
    
    spider = PhysisSpider(base_dir=args.base_dir, monitor_mode=args.monitor, category_filter=category_filter)
    
    if args.download_center:
        # 只爬取下载中心
        spider.check_download_center_updates()
    elif args.products_only:
        # 只爬取产品中心  
        spider.run()
    else:
        # 同时爬取产品中心和下载中心
        spider.run()
        spider.check_download_center_updates()


if __name__ == "__main__":
    main()
