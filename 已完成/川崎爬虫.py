#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import json
import pickle
import platform
import re
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin, urlparse, unquote
import requests
import chardet

# Selenium imports
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import TimeoutException, NoSuchElementException, ElementClickInterceptedException
from bs4 import BeautifulSoup

class KawasakiSpider:
    def __init__(self):
        # 基础配置
        self.base_url = "https://kawasakirobotics.cn"
        
        # 服务器固定路径（按规范要求），本地测试使用当前目录
        if os.path.exists("/srv/downloads/approved/"):
            self.base_dir = "/srv/downloads/approved/川崎"
        else:
            # 本地测试环境
            self.base_dir = os.path.join(os.getcwd(), "downloads", "川崎")
        
        # 确保目录存在
        os.makedirs(self.base_dir, exist_ok=True)
        
        self.processed_urls = self.load_processed_urls()
        self.new_files = []
        self.debug = True
        
        # 判断是否为首次运行（全量爬取）
        self.is_first_run = not os.path.exists(os.path.join(self.base_dir, 'processed_urls.pkl'))
        
        # 钉钉通知配置
        self.ACCESS_TOKEN = "1a431b6482e59ec652a6eab37705d50fd282dc426f0b829b3eb9a9c1b277ed24"
        self.SECRET = "SECc5e2168011dd97d4c511e06832d172f31635ef635771668e4e6003fce23c07bb"
        
        # 初始化WebDriver
        self.driver = None
        self.init_webdriver()
        
        # 主要爬取模块 - 基于HTML结构分析
        self.main_modules = [
            {
                'name': '机器人',
                'url': 'https://kawasakirobotics.cn/products-robots/',
                'categories': [
                    {'name': '中小型通用机器人~80kg负载', 'url': 'https://kawasakirobotics.cn/robots-category/small-medium-payloads/'},
                    {'name': '大型通用机器人~300kg负载', 'url': 'https://kawasakirobotics.cn/robots-category/large-payloads/'},
                    {'name': '超大型通用机器人~1,500kg负载', 'url': 'https://kawasakirobotics.cn/robots-category/extra-large-payloads/'},
                    {'name': '协作机器人', 'url': 'https://kawasakirobotics.cn/robots-category/dual-arm-scara/'},
                    {'name': '码垛机器人', 'url': 'https://kawasakirobotics.cn/robots-category/palletizing/'},
                    {'name': '高速分拣机器人', 'url': 'https://kawasakirobotics.cn/robots-category/pick-and-place/'},
                    {'name': '医药机器人', 'url': 'https://kawasakirobotics.cn/robots-category/medical/'},
                    {'name': '焊接/切割机器人', 'url': 'https://kawasakirobotics.cn/robots-category/arc-welding/'},
                    {'name': '喷涂机器人', 'url': 'https://kawasakirobotics.cn/robots-category/painting/'},
                    {'name': '晶圆搬运机器人', 'url': 'https://kawasakirobotics.cn/robots-category/wafer/'}
                ]
            },
            {
                'name': '控制柜',
                'url': 'https://kawasakirobotics.cn/controllers-category/',
                'categories': [
                    {'name': 'F 控制柜', 'url': 'https://kawasakirobotics.cn/controllers-category/f-controllers/'},
                    {'name': 'E 控制柜', 'url': 'https://kawasakirobotics.cn/controllers-category/e-controllers/'},
                    {
                        'name': '防爆 E控制柜', 
                        'url': 'https://kawasakirobotics.cn/controllers-category/explosion-proof-e-controllers/',
                                            'subcategories': [
                        {'name': 'E25亚洲_E35美洲_E45欧洲', 'url': 'https://kawasakirobotics.cn/controllers-category/explosion-proof-e-controllers/'}
                    ]
                    }
                ]
            },
            {
                'name': '其他产品',
                'url': 'https://kawasakirobotics.cn/others-category/',
                'categories': [
                    {'name': '编程工具', 'url': 'https://kawasakirobotics.cn/others-category/programming-tool/'},
                    {'name': '视觉选配', 'url': 'https://kawasakirobotics.cn/others-category/vision-option/'},
                    {'name': '安全监控选配', 'url': 'https://kawasakirobotics.cn/others-category/safety/'},
                    {'name': '监控与管护工具', 'url': 'https://kawasakirobotics.cn/others-category/operation-maintenance-monitoring-tool/'}
                ]
            },
            {
                'name': 'K-AddOn',
                'url': 'https://kawasakirobotics.cn/products-kaddon/',
                'categories': [
                    {'name': 'K-AddOn 产品', 'url': 'https://kawasakirobotics.cn/products-kaddon/'},
                    {'name': 'K-AddOn 软件', 'url': 'https://kawasakirobotics.cn/products-kaddon/software/'},
                    {'name': 'K-AddOn 硬件', 'url': 'https://kawasakirobotics.cn/products-kaddon/hardware/'}
                ]
            }
        ]
        
    def init_webdriver(self):
        """初始化Chrome WebDriver"""
        try:
            # 检测系统架构
            system = platform.system()
            machine = platform.machine()
            
            # 确定chromedriver路径
            # 在服务器上查找chromedriver目录
            if os.path.exists("/srv/crawler/chromedriver_downloads"):
                chromedriver_dir = "/srv/crawler/chromedriver_downloads"
            else:
                chromedriver_dir = os.path.join(os.getcwd(), "chromedriver_downloads")
            
            if system == "Darwin":  # macOS
                if machine == "arm64":
                    chromedriver_path = os.path.join(chromedriver_dir, "chromedriver_mac-arm64", "chromedriver-mac-arm64", "chromedriver")
                else:
                    chromedriver_path = os.path.join(chromedriver_dir, "chromedriver_mac-x64", "chromedriver-mac-x64", "chromedriver")
            elif system == "Linux":
                chromedriver_path = os.path.join(chromedriver_dir, "chromedriver_linux64", "chromedriver-linux64", "chromedriver")
            elif system == "Windows":
                chromedriver_path = os.path.join(chromedriver_dir, "chromedriver_win64", "chromedriver-win64", "chromedriver.exe")
            else:
                raise Exception(f"不支持的操作系统: {system}")
            
            if not os.path.exists(chromedriver_path):
                raise Exception(f"ChromeDriver未找到: {chromedriver_path}")
            
            # 设置Chrome选项
            chrome_options = Options()
            chrome_options.add_argument('--headless')  # 无头模式
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--window-size=1920,1080')
            chrome_options.add_argument('--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
            
            # 初始化WebDriver
            service = Service(executable_path=chromedriver_path)
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            self.driver.implicitly_wait(10)
            
            self.log("✅ Chrome WebDriver初始化成功")
            
        except Exception as e:
            self.log(f"❌ WebDriver初始化失败: {str(e)}")
            raise
    
    def log(self, message):
        """日志记录"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{timestamp}] {message}")
    
    def load_processed_urls(self):
        """加载已处理的URL列表"""
        processed_file = os.path.join(self.base_dir, 'processed_urls.pkl')
        if os.path.exists(processed_file):
            try:
                with open(processed_file, 'rb') as f:
                    return pickle.load(f)
            except:
                return set()
        return set()
    
    def save_processed_urls(self):
        """保存已处理的URL列表"""
        processed_file = os.path.join(self.base_dir, 'processed_urls.pkl')
        with open(processed_file, 'wb') as f:
            pickle.dump(self.processed_urls, f)
    
    def visit_page(self, url, retry_count=3):
        """访问页面并返回BeautifulSoup对象"""
        for attempt in range(retry_count):
            try:
                self.log(f"🔄 访问页面 (尝试{attempt+1}/{retry_count}): {url}")
                self.driver.get(url)
                
                # 等待页面加载完成
                WebDriverWait(self.driver, 30).until(
                    lambda driver: driver.execute_script("return document.readyState") == "complete"
                )
                
                # 额外等待JavaScript执行
                time.sleep(3)
                
                # 尝试滚动页面加载更多内容
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)
                
                # 获取页面源码
                page_source = self.driver.page_source
                soup = BeautifulSoup(page_source, 'html.parser')
                
                return soup
                
            except Exception as e:
                self.log(f"❌ 页面访问失败 (尝试{attempt+1}): {str(e)}")
                time.sleep(5)
        
        self.log(f"❌ 页面访问完全失败: {url}")
        return None
    
    def find_product_links(self, soup, page_url):
        """从分类页面中查找产品链接"""
        products = []
        
        try:
            # 查找产品网格中的产品链接
            product_columns = soup.find_all('div', class_='wp-block-column krobot-pattern-products__item')
            self.log(f"🔍 找到 {len(product_columns)} 个产品列")
            
            for column in product_columns:
                try:
                    # 查找产品标题
                    title_element = column.find('h2', class_='krobot-pattern-products__title')
                    if title_element:
                        product_title = title_element.get_text().strip()
                        
                        # 查找"更多的"按钮链接
                        more_button = column.find('a', class_='wp-block-button__link')
                        if more_button and '更多的' in more_button.get_text():
                            product_url = more_button.get('href', '')
                            if product_url:
                                # 构建完整URL
                                if not product_url.startswith('http'):
                                    product_url = urljoin(self.base_url, product_url)
                                
                                # 查找产品规格信息
                                specs = {}
                                spec_list = column.find('ul', class_='krobot-pattern-products__info')
                                if spec_list:
                                    spec_items = spec_list.find_all('li')
                                    for item in spec_items:
                                        strong = item.find('strong')
                                        if strong:
                                            key = strong.get_text().strip()
                                            # 获取strong标签后的文本
                                            value = item.get_text().replace(key, '').strip()
                                            specs[key] = value
                                
                                products.append({
                                    'title': product_title,
                                    'url': product_url,
                                    'specs': specs,
                                    'type': 'product_page'
                                })
                                
                                self.log(f"   ✅ 找到产品: {product_title} -> {product_url}")
                
                except Exception as e:
                    self.log(f"   ❌ 处理产品列时出错: {str(e)}")
                    continue
            
            if products:
                self.log(f"📦 在页面中找到 {len(products)} 个产品")
            else:
                self.log(f"❌ 页面中未找到产品: {page_url}")
                
        except Exception as e:
            self.log(f"❌ 查找产品链接时出错: {str(e)}")
        
        return products
    
    def find_download_links(self, soup, page_url):
        """从产品详情页面中查找下载链接"""
        downloads = []
        
        try:
            # 查找资料下载区域
            download_section = soup.find('div', class_='product-download product-section entry-content')
            if download_section:
                self.log(f"🔍 找到资料下载区域")
                
                # 查找下载列表
                download_list = download_section.find('ul', class_='product-download__list')
                if download_list:
                    download_items = download_list.find_all('li', class_='product-download__item')
                    
                    for item in download_items:
                        try:
                            # 获取文件类型描述
                            text_element = item.find('span', class_='product-download__text')
                            file_type = text_element.get_text().strip() if text_element else "未知类型"
                            
                            # 获取下载链接
                            download_link = item.find('a', class_='product-download__btn')
                            if download_link:
                                href = download_link.get('href', '')
                                if href:
                                    # 构建完整URL
                                    if not href.startswith('http'):
                                        full_url = urljoin(page_url, href)
                                    else:
                                        full_url = href
                                    
                                    # 从URL获取文件名
                                    filename = os.path.basename(urlparse(full_url).path)
                                    if not filename:
                                        filename = f"{file_type}_{int(time.time())}.pdf"
                                    
                                    downloads.append({
                                        'title': file_type,
                                        'url': full_url,
                                        'filename': filename,
                                        'type': 'direct_download'
                                    })
                                    
                                    self.log(f"   ✅ 找到下载: {file_type} -> {full_url}")
                        
                        except Exception as e:
                            self.log(f"   ❌ 处理下载项时出错: {str(e)}")
                            continue
                
                if downloads:
                    self.log(f"📎 在页面中找到 {len(downloads)} 个下载文件")
                else:
                    self.log(f"❌ 资料下载区域中未找到下载链接")
            else:
                self.log(f"❌ 页面中未找到资料下载区域: {page_url}")
                
        except Exception as e:
            self.log(f"❌ 查找下载链接时出错: {str(e)}")
        
        return downloads
    
    def download_file(self, url, filename, base_folder, file_type_category=None):
        """下载文件到指定文件夹"""
        try:
            # 如果指定了文件类型分类，创建对应的子文件夹
            if file_type_category:
                target_folder = os.path.join(base_folder, file_type_category)
                # 只在有文件时创建分类文件夹
                os.makedirs(target_folder, exist_ok=True)
                self.log(f"📁 创建分类文件夹: {target_folder}")
            else:
                target_folder = base_folder
            
            file_path = os.path.join(target_folder, filename)
            
            # 检查文件是否已存在且大小合理（避免下载损坏的文件）
            if os.path.exists(file_path):
                file_size = os.path.getsize(file_path)
                if file_size > 1024:  # 如果文件大于1KB，认为是有效文件
                    self.log(f"📁 文件已存在，跳过: {filename}")
                    return True
                else:
                    self.log(f"🔄 文件存在但大小异常({file_size}字节)，重新下载: {filename}")
                    os.remove(file_path)
            
            # 下载文件
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
                'Referer': self.base_url
            }
            
            response = requests.get(url, headers=headers, stream=True, timeout=30)
            response.raise_for_status()
            
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            file_size = os.path.getsize(file_path)
            self.log(f"✅ 下载成功: {filename} -> {target_folder} ({file_size} bytes)")
            
            self.new_files.append({
                'filename': filename,
                'path': file_path,
                'url': url,
                'size': file_size,
                'category': file_type_category
            })
            
            return True
            
        except Exception as e:
            self.log(f"❌ 下载失败 {filename}: {str(e)}")
            return False
    
    def generate_clean_filename(self, title, download_filename="", url=""):
        """生成清洁的文件名"""
        try:
            # 如果有指定的下载文件名，优先使用
            if download_filename:
                return download_filename
            
            # 否则从标题生成文件名
            clean_title = re.sub(r'[^\w\s\-\u4e00-\u9fff]', '', title)
            clean_title = re.sub(r'\s+', '_', clean_title.strip())
            
            # 截断过长的文件名
            if len(clean_title) > 100:
                clean_title = clean_title[:100]
            
            # 从URL获取文件扩展名
            if url:
                parsed_url = urlparse(url)
                path = parsed_url.path
                ext = os.path.splitext(path)[1]
                
                if not ext:
                    ext = '.pdf'  # 默认为PDF
            else:
                ext = '.pdf'
            
            return f"{clean_title}{ext}"
            
        except Exception as e:
            self.log(f"⚠️ 文件名生成失败: {str(e)}")
            return f"document_{int(time.time())}.pdf"
    
    def extract_product_model(self, product_title):
        """从产品标题中提取产品型号"""
        try:
            # 改进的川崎机器人型号模式 - 更精确的匹配
            patterns = [
                # RS系列 - 更精确的匹配
                r'RS\d{3}[NLHX]?',  # RS003N, RS003L, RS003H, RS003X等
                r'RS\d{3}[NLHX]?\s*[NLHX]',  # 处理空格分隔的情况
                
                # BA系列
                r'BA\d{3}[NLHX]?',  # BA006N, BA006L, BA006H等
                r'BA\d{3}[NLHX]?\s*[NLHX]',
                
                # MS系列
                r'MS\d{3}[NLHX]?',  # MS005N, MS005L, MS005H等
                r'MS\d{3}[NLHX]?\s*[NLHX]',
                
                # VS系列
                r'VS\d{3}[NLHX]?',  # VS050N, VS050L, VS050H等
                r'VS\d{3}[NLHX]?\s*[NLHX]',
                
                # KJ系列
                r'KJ\d{3}[NLHX]?',  # KJ264N, KJ264L等
                r'KJ\d{3}[NLHX]?\s*[NLHX]',
                
                # F系列控制柜
                r'F\d{3}[NLHX]?',   # F001N, F001L, F001H等
                r'F\d{3}[NLHX]?\s*[NLHX]',
                
                # E系列控制柜
                r'E\d{3}[NLHX]?',   # E001N, E001L, E001H等
                r'E\d{3}[NLHX]?\s*[NLHX]',
                
                # 其他可能的型号格式
                r'[A-Z]{1,3}\d{2,4}[NLHX]?',  # 通用模式
                r'[A-Z]{1,3}\d{2,4}\s*[NLHX]',  # 带空格分隔
            ]
            
            # 首先尝试精确匹配
            for pattern in patterns:
                match = re.search(pattern, product_title, re.IGNORECASE)
                if match:
                    model = match.group().upper()
                    # 标准化型号格式
                    model = self.standardize_model_name(model)
                    self.log(f"🏷️ 提取到型号: {model} (从标题: {product_title})")
                    return model
            
            # 如果没有找到标准型号，尝试从URL路径中提取
            # 这通常更可靠，因为URL通常包含准确的型号信息
            if hasattr(self, 'current_url') and self.current_url:
                url_model = self.extract_model_from_url(self.current_url)
                if url_model:
                    self.log(f"🏷️ 从URL提取到型号: {url_model}")
                    return url_model
            
            # 最后的后备方案 - 使用标题的前几个字符
            clean_title = re.sub(r'[^\w\s\u4e00-\u9fff]', '', product_title)
            words = clean_title.split()
            if words:
                for word in words:
                    if len(word) >= 2 and not word.isdigit():
                        # 尝试从单词中提取型号信息
                        word_model = self.extract_model_from_word(word)
                        if word_model:
                            return word_model
                        return word[:10]  # 限制长度
            
            # 最终后备方案
            fallback_name = f"产品_{int(time.time())}"
            self.log(f"⚠️ 无法提取型号，使用后备名称: {fallback_name}")
            return fallback_name
            
        except Exception as e:
            self.log(f"⚠️ 产品型号提取失败: {str(e)}")
            return f"产品_{int(time.time())}"
    
    def standardize_model_name(self, model):
        """标准化型号名称，确保格式一致"""
        try:
            # 移除多余的空格
            model = re.sub(r'\s+', '', model)
            
            # 确保型号格式正确
            # 例如：RS003N, RS003L, RS003H, RS003X
            if re.match(r'^[A-Z]{1,3}\d{2,4}[NLHX]?$', model):
                return model
            
            # 如果格式不正确，尝试修复
            # 提取基础型号和后缀
            base_match = re.match(r'^([A-Z]{1,3}\d{2,4})([NLHX]?)$', model)
            if base_match:
                base, suffix = base_match.groups()
                return f"{base}{suffix}"
            
            return model
            
        except Exception as e:
            self.log(f"⚠️ 型号标准化失败: {str(e)}")
            return model
    
    def extract_model_from_url(self, url):
        """从URL中提取产品型号"""
        try:
            # 解析URL路径
            parsed_url = urlparse(url)
            path_parts = parsed_url.path.strip('/').split('/')
            
            # 查找包含型号的路径段
            for part in path_parts:
                # 尝试匹配型号模式
                model_match = re.search(r'([A-Z]{1,3}\d{2,4}[NLHX]?)', part, re.IGNORECASE)
                if model_match:
                    model = model_match.group().upper()
                    return self.standardize_model_name(model)
            
            return None
            
        except Exception as e:
            self.log(f"⚠️ 从URL提取型号失败: {str(e)}")
            return None
    
    def extract_model_from_word(self, word):
        """从单词中提取型号信息"""
        try:
            # 尝试匹配型号模式
            model_match = re.search(r'([A-Z]{1,3}\d{2,4}[NLHX]?)', word, re.IGNORECASE)
            if model_match:
                model = model_match.group().upper()
                return self.standardize_model_name(model)
            
            return None
            
        except Exception as e:
            return None
    
    def categorize_file_type(self, file_type, filename, url):
        """根据网页上显示的文件类型分类名称来分类文件"""
        try:
            # 直接使用网页上显示的分类名称，进行标准化处理
            file_type_clean = file_type.strip()
            
            # 手册类 - 包含"手册"关键词
            if '手册' in file_type_clean:
                return '手册'
            
            # CAD类 - 包含"CAD"关键词
            if 'CAD' in file_type_clean:
                return 'CAD'
            
            # 规格类 - 包含"规格"关键词
            if '规格' in file_type_clean:
                return '规格书'
            
            # 软件类 - 包含"软件"关键词
            if '软件' in file_type_clean:
                return '软件'
            
            # 视频类 - 包含"视频"关键词
            if '视频' in file_type_clean:
                return '其他文档'
            
            # 其他文档类 - 包含"文档"、"资料"等关键词
            if any(keyword in file_type_clean for keyword in ['文档', '资料', '文件']):
                return '其他文档'
            
            # 如果都不匹配，根据常见类型进行智能分类
            if any(keyword in file_type_clean.lower() for keyword in ['manual', 'handbook', 'guide', 'instruction']):
                return '手册'
            elif any(keyword in file_type_clean.lower() for keyword in ['drawing', 'model', 'step', 'dxf', 'dwg']):
                return 'CAD'
            elif any(keyword in file_type_clean.lower() for keyword in ['spec', 'specification', 'parameter', 'technical']):
                return '规格书'
            elif any(keyword in file_type_clean.lower() for keyword in ['software', 'program', 'exe', 'msi']):
                return '软件'
            elif any(keyword in file_type_clean.lower() for keyword in ['video', 'demo', 'tutorial']):
                return '其他文档'
            
            # 默认分类为其他文档
            return '其他文档'
            
        except Exception as e:
            self.log(f"⚠️ 文件类型分类失败: {str(e)}")
            return '其他文档'
    
    def create_product_folder_structure(self, product_model, base_folder):
        """为产品创建文件夹结构"""
        try:
            # 创建产品主文件夹
            product_folder = os.path.join(base_folder, product_model)
            os.makedirs(product_folder, exist_ok=True)
            
            # 不预先创建子分类文件夹，只在有文件时创建
            self.log(f"📁 创建产品文件夹: {product_folder}")
            
            return product_folder
            
        except Exception as e:
            self.log(f"❌ 创建产品文件夹结构失败: {str(e)}")
            return base_folder
    
    def process_product_page(self, product_url, product_title, folder_path):
        """处理产品详情页面，下载相关资料"""
        try:
            self.log(f"🔍 处理产品页面: {product_title}")
            
            if product_url in self.processed_urls:
                self.log(f"⏭️ 跳过已处理产品: {product_title}")
                return
            
            # 记录当前URL，用于型号提取
            self.current_url = product_url
            
            soup = self.visit_page(product_url)
            if not soup:
                return
            
            # 提取产品型号
            product_model = self.extract_product_model(product_title)
            self.log(f"🏷️ 产品型号: {product_model}")
            
            # 查找下载链接
            downloads = self.find_download_links(soup, product_url)
            
            if downloads:
                self.log(f"🚀 开始处理 {len(downloads)} 个文件到产品文件夹: {product_model}")
                
                for download in downloads:
                    try:
                        title = download['title']
                        url = download['url']
                        filename = download.get('filename', '')
                        
                        # 生成文件名
                        clean_filename = self.generate_clean_filename(title, filename, url)
                        
                        # 分类文件
                        file_type_category = self.categorize_file_type(title, filename, url)
                        
                        # 创建产品文件夹结构
                        product_folder = self.create_product_folder_structure(product_model, folder_path)
                        
                        # 下载文件到对应的分类文件夹
                        self.download_file(url, clean_filename, product_folder, file_type_category)
                        
                        time.sleep(1)  # 下载间隔
                        
                    except Exception as e:
                        self.log(f"❌ 处理下载项时出错: {str(e)}")
                        continue
            else:
                self.log(f"⚠️ 产品页面中未找到下载文件: {product_title}")
            
            # 标记为已处理
            self.processed_urls.add(product_url)
            
        except Exception as e:
            self.log(f"❌ 处理产品页面时出错: {str(e)}")
    
    def process_category_page(self, module_name, category):
        """处理分类页面"""
        category_name = category['name']
        category_url = category['url']
        
        if category_url in self.processed_urls:
            self.log(f"⏭️ 跳过已处理分类: {category_name}")
            return
        
        self.log(f"📋 处理分类: {module_name} -> {category_name}")
        
        # 检查是否有子分类
        if 'subcategories' in category:
            self.log(f"🔍 发现子分类，开始处理子分类")
            # 创建父分类文件夹，所有子分类的内容都放在这个文件夹下
            safe_category_name = category_name.replace('/', '_').replace('\\', '_')
            folder_path = os.path.join(self.base_dir, module_name, safe_category_name)
            
            for subcategory in category['subcategories']:
                self.process_subcategory_page(module_name, category_name, subcategory, folder_path)
                time.sleep(2)  # 子分类间延迟
        else:
            # 处理普通分类页面
            soup = self.visit_page(category_url)
            if not soup:
                return
            
            # 查找产品链接
            products = self.find_product_links(soup, category_url)
            
            if products:
                # 创建模块目录，按照顶级模块名称分类
                safe_category_name = category_name.replace('/', '_').replace('\\', '_')
                folder_path = os.path.join(self.base_dir, module_name, safe_category_name)
                
                self.log(f"🚀 开始处理 {len(products)} 个产品到: {folder_path}")
                
                for product in products:
                    try:
                        product_title = product['title']
                        product_url = product['url']
                        
                        # 处理产品详情页面
                        self.process_product_page(product_url, product_title, folder_path)
                        
                        time.sleep(2)  # 产品间延迟
                        
                    except Exception as e:
                        self.log(f"❌ 处理产品时出错: {str(e)}")
                        continue
            else:
                self.log(f"⚠️ 分类页面中未找到产品: {category_name}")
        
        # 标记为已处理
        self.processed_urls.add(category_url)
    
    def process_subcategory_page(self, module_name, parent_category_name, subcategory, folder_path):
        """处理子分类页面"""
        subcategory_name = subcategory['name']
        subcategory_url = subcategory['url']
        
        if subcategory_url in self.processed_urls:
            self.log(f"⏭️ 跳过已处理子分类: {subcategory_name}")
            return
        
        self.log(f"🔍 处理子分类: {module_name} -> {parent_category_name} -> {subcategory_name}")
        
        soup = self.visit_page(subcategory_url)
        if not soup:
            return
        
        # 查找产品链接
        products = self.find_product_links(soup, subcategory_url)
        
        if products:
            self.log(f"🚀 开始处理 {len(products)} 个产品到: {folder_path}")
            
            for product in products:
                try:
                    product_title = product['title']
                    product_url = product['url']
                    
                    # 处理产品详情页面，所有子分类的产品都放在同一个父分类文件夹下
                    self.process_product_page(product_url, product_title, folder_path)
                    
                    time.sleep(2)  # 产品间延迟
                    
                except Exception as e:
                    self.log(f"❌ 处理产品时出错: {str(e)}")
                    continue
        else:
            self.log(f"⚠️ 子分类页面中未找到产品: {subcategory_name}")
        
        # 标记为已处理
        self.processed_urls.add(subcategory_url)
    
    def process_main_page(self, module):
        """处理主页面（可能包含一些通用下载）"""
        module_name = module['name']
        module_url = module['url']
        
        if module_url in self.processed_urls:
            self.log(f"⏭️ 跳过已处理主页: {module_name}")
            return
        
        self.log(f"🌐 处理主页: {module_name}")
        
        soup = self.visit_page(module_url)
        if not soup:
            return
        
        # 查找主页面的下载链接
        downloads = self.find_download_links(soup, module_url)
        
        if downloads:
            # 创建主模块目录，保存通用文件到对应的产品分类
            folder_path = os.path.join(self.base_dir, module_name, "通用资料")
            
            self.log(f"🚀 处理主页 {len(downloads)} 个文件到: {folder_path}")
            
            for download in downloads:
                try:
                    title = download['title']
                    url = download['url']
                    filename = download.get('filename', '')
                    
                    # 生成文件名
                    clean_filename = self.generate_clean_filename(title, filename, url)
                    
                    # 下载文件
                    self.download_file(url, clean_filename, folder_path)
                    
                    time.sleep(1)
                    
                except Exception as e:
                    self.log(f"❌ 处理下载项时出错: {str(e)}")
                    continue
        
        # 标记为已处理
        self.processed_urls.add(module_url)
    
    def send_dingtalk_notification(self, message):
        """发送钉钉通知"""
        try:
            # 这里需要配置你的钉钉机器人webhook地址
            webhook_url = os.environ.get('DINGTALK_WEBHOOK', '')
            
            if not webhook_url:
                self.log("⚠️ 未配置钉钉webhook，跳过通知")
                return
            
            data = {
                "msgtype": "text",
                "text": {
                    "content": message
                }
            }
            
            response = requests.post(webhook_url, json=data, timeout=10)
            
            if response.status_code == 200:
                self.log("✅ 钉钉通知发送成功")
            else:
                self.log(f"❌ 钉钉通知发送失败: {response.status_code}")
                
        except Exception as e:
            self.log(f"❌ 钉钉通知发送出错: {str(e)}")
    
    def run(self):
        """运行爬虫"""
        start_time = datetime.now()
        
        try:
            self.log("🚀 开始运行川崎机器人文档爬虫")
            
            # 爬取每个主要模块
            for module in self.main_modules:
                module_name = module['name']
                self.log(f"📂 开始处理模块: {module_name}")
                
                # 处理主页面
                self.process_main_page(module)
                time.sleep(2)
                
                # 处理各个分类
                for category in module['categories']:
                    self.process_category_page(module_name, category)
                    time.sleep(3)  # 分类间延迟
                
                self.log(f"✅ 完成模块: {module_name}")
                time.sleep(5)  # 模块间延迟
            
            # 保存进度
            self.save_processed_urls()
            
            # 统计结果
            end_time = datetime.now()
            duration = end_time - start_time
            total_files = len(self.new_files)
            
            self.log(f"🎉 爬取完成！共下载 {total_files} 个新文件，耗时 {duration}")
            
            # 发送钉钉通知
            if self.new_files:
                notification_message = f"""川崎机器人爬虫完成！
📊 爬取统计：
• 新文件数量：{total_files}
• 爬取耗时：{duration}
• 完成时间：{end_time.strftime('%Y-%m-%d %H:%M:%S')}

📁 新下载文件（前10个）："""
                
                for i, file_info in enumerate(self.new_files[:10]):
                    notification_message += f"\n{i+1}. {file_info['filename']} ({file_info['size']} bytes)"
                
                if len(self.new_files) > 10:
                    notification_message += f"\n... 还有 {len(self.new_files) - 10} 个文件"
                
                self.send_dingtalk_notification(notification_message)
                
                self.log("📁 新下载的文件:")
                for file_info in self.new_files[:10]:
                    self.log(f"   📄 {file_info['filename']} ({file_info['size']} bytes)")
                
                if len(self.new_files) > 10:
                    self.log(f"   ... 还有 {len(self.new_files) - 10} 个文件")
            else:
                notification_message = f"""川崎机器人爬虫完成！
📊 本次未发现新文件
• 爬取耗时：{duration}
• 完成时间：{end_time.strftime('%Y-%m-%d %H:%M:%S')}"""
                
                self.send_dingtalk_notification(notification_message)
                self.log("ℹ️ 本次爬取未发现新文件")
            
        except Exception as e:
            error_message = f"川崎机器人爬虫运行出错：{str(e)}"
            self.log(f"❌ {error_message}")
            self.send_dingtalk_notification(error_message)
            
        finally:
            # 关闭WebDriver
            if self.driver:
                self.driver.quit()
                self.log("🔒 WebDriver已关闭")

    def validate_and_fix_folder_names(self):
        """验证和修复现有文件夹的命名问题"""
        try:
            self.log("🔍 开始验证和修复文件夹命名...")
            
            # 遍历川崎目录下的所有文件夹
            for root, dirs, files in os.walk(self.base_dir):
                for dir_name in dirs:
                    dir_path = os.path.join(root, dir_name)
                    
                    # 检查是否是产品型号文件夹（包含RS、BA、MS等前缀）
                    if re.match(r'^[A-Z]{1,3}\d{2,4}[NLHX]?$', dir_name, re.IGNORECASE):
                        # 这是产品型号文件夹，验证命名
                        corrected_name = self.standardize_model_name(dir_name)
                        
                        if corrected_name != dir_name:
                            # 需要重命名
                            corrected_path = os.path.join(root, corrected_name)
                            
                            # 检查目标路径是否已存在
                            if os.path.exists(corrected_path):
                                self.log(f"⚠️ 目标路径已存在，跳过重命名: {dir_name} -> {corrected_name}")
                                continue
                            
                            try:
                                os.rename(dir_path, corrected_path)
                                self.log(f"✅ 重命名文件夹: {dir_name} -> {corrected_name}")
                            except Exception as e:
                                self.log(f"❌ 重命名失败 {dir_name}: {str(e)}")
                    
                    # 检查是否是控制柜型号文件夹
                    elif re.match(r'^[EF]\d{3}[NLHX]?$', dir_name, re.IGNORECASE):
                        corrected_name = self.standardize_model_name(dir_name)
                        
                        if corrected_name != dir_name:
                            corrected_path = os.path.join(root, corrected_name)
                            
                            if os.path.exists(corrected_path):
                                self.log(f"⚠️ 目标路径已存在，跳过重命名: {dir_name} -> {corrected_name}")
                                continue
                            
                            try:
                                os.rename(dir_path, corrected_path)
                                self.log(f"✅ 重命名控制柜文件夹: {dir_name} -> {corrected_name}")
                            except Exception as e:
                                self.log(f"❌ 重命名失败 {dir_name}: {str(e)}")
            
            self.log("✅ 文件夹命名验证和修复完成")
            
        except Exception as e:
            self.log(f"❌ 文件夹命名验证失败: {str(e)}")
    
    def get_folder_statistics(self):
        """获取文件夹统计信息"""
        try:
            stats = {
                'total_products': 0,
                'rs_series': 0,
                'ba_series': 0,
                'ms_series': 0,
                'vs_series': 0,
                'kj_series': 0,
                'f_controllers': 0,
                'e_controllers': 0,
                'other': 0,
                'naming_issues': []
            }
            
            # 遍历川崎目录
            for root, dirs, files in os.walk(self.base_dir):
                for dir_name in dirs:
                    dir_path = os.path.join(root, dir_name)
                    
                    # 检查是否是产品型号文件夹
                    if re.match(r'^[A-Z]{1,3}\d{2,4}[NLHX]?$', dir_name, re.IGNORECASE):
                        stats['total_products'] += 1
                        
                        # 分类统计
                        if dir_name.upper().startswith('RS'):
                            stats['rs_series'] += 1
                        elif dir_name.upper().startswith('BA'):
                            stats['ba_series'] += 1
                        elif dir_name.upper().startswith('MS'):
                            stats['ms_series'] += 1
                        elif dir_name.upper().startswith('VS'):
                            stats['vs_series'] += 1
                        elif dir_name.upper().startswith('KJ'):
                            stats['kj_series'] += 1
                        elif dir_name.upper().startswith('F'):
                            stats['f_controllers'] += 1
                        elif dir_name.upper().startswith('E'):
                            stats['e_controllers'] += 1
                        else:
                            stats['other'] += 1
                        
                        # 检查命名问题
                        if not re.match(r'^[A-Z]{1,3}\d{2,4}[NLHX]?$', dir_name):
                            stats['naming_issues'].append(dir_name)
            
            return stats
            
        except Exception as e:
            self.log(f"❌ 获取文件夹统计失败: {str(e)}")
            return None

def test_single_category(category_url=None):
    """测试单个分类的爬取功能"""
    spider = KawasakiSpider()
    
    try:
        # 默认测试URL
        test_url = category_url or "https://kawasakirobotics.cn/robots-category/small-medium-payloads/"
        
        spider.log(f"🧪 测试单分类爬取功能")
        spider.log(f"📋 测试URL: {test_url}")
        
        # 创建测试分类配置
        test_category = {
            'name': '测试分类',
            'url': test_url
        }
        
        # 处理测试分类
        spider.process_category_page("测试模块", test_category)
        
        if spider.new_files:
            spider.log(f"✅ 测试成功！找到 {len(spider.new_files)} 个文件")
            for file_info in spider.new_files:
                spider.log(f"   📄 {file_info['filename']}")
        else:
            spider.log(f"⚠️ 测试完成，但未找到新文件")
        
    except Exception as e:
        spider.log(f"❌ 测试失败: {str(e)}")
    finally:
        if spider.driver:
            spider.driver.quit()

def test_single_product(product_url=None):
    """测试单个产品的爬取功能"""
    spider = KawasakiSpider()
    
    try:
        # 默认测试URL
        test_url = product_url or "https://kawasakirobotics.cn/products-robots/rs080n/"
        
        spider.log(f"🧪 测试单产品爬取功能")
        spider.log(f"🔍 测试URL: {test_url}")
        
        # 测试产品页面处理
        spider.process_product_page(test_url, "RS080N测试", os.path.join(spider.base_dir, "测试"))
        
        if spider.new_files:
            spider.log(f"✅ 测试成功！找到 {len(spider.new_files)} 个文件")
            for file_info in spider.new_files:
                spider.log(f"   📄 {file_info['filename']}")
        else:
            spider.log(f"⚠️ 测试完成，但未找到新文件")
        
    except Exception as e:
        spider.log(f"❌ 测试失败: {str(e)}")
    finally:
        if spider.driver:
            spider.driver.quit()

if __name__ == "__main__":
    import sys
    
    # 检查命令行参数
    if len(sys.argv) > 1:
        if sys.argv[1] == "test":
            # 测试模式
            test_url = sys.argv[2] if len(sys.argv) > 2 else None
            test_single_category(test_url)
        elif sys.argv[1] == "test_product":
            # 产品测试模式
            test_url = sys.argv[2] if len(sys.argv) > 2 else None
            test_single_product(test_url)
        elif sys.argv[1] == "fix_folders":
            # 修复文件夹命名模式
            spider = KawasakiSpider()
            spider.validate_and_fix_folder_names()
        elif sys.argv[1] == "stats":
            # 显示文件夹统计信息
            spider = KawasakiSpider()
            stats = spider.get_folder_statistics()
            if stats:
                print("\n📊 川崎爬虫文件夹统计信息:")
                print(f"总产品数量: {stats['total_products']}")
                print(f"RS系列: {stats['rs_series']}")
                print(f"BA系列: {stats['ba_series']}")
                print(f"MS系列: {stats['ms_series']}")
                print(f"VS系列: {stats['vs_series']}")
                print(f"KJ系列: {stats['kj_series']}")
                print(f"F控制柜: {stats['f_controllers']}")
                print(f"E控制柜: {stats['e_controllers']}")
                print(f"其他: {stats['other']}")
                
                if stats['naming_issues']:
                    print(f"\n⚠️ 命名问题文件夹:")
                    for issue in stats['naming_issues']:
                        print(f"  - {issue}")
                else:
                    print("\n✅ 所有文件夹命名正确")
        else:
            print("用法:")
            print("  python 川崎爬虫.py                    # 正常运行爬虫")
            print("  python 川崎爬虫.py test [url]        # 测试单个分类")
            print("  python 川崎爬虫.py test_product [url] # 测试单个产品")
            print("  python 川崎爬虫.py fix_folders        # 修复文件夹命名")
            print("  python 川崎爬虫.py stats              # 显示文件夹统计")
    else:
        # 正常运行
        spider = KawasakiSpider()
        spider.run()
