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

class NordSeleniumSpiderV2:
    def __init__(self):
        # 基础配置
        self.base_url = "https://www.nord.cn"
        
        # 服务器固定路径（按规范要求），本地测试使用当前目录
        if os.path.exists("/srv/downloads/approved/"):
            self.base_dir = "/srv/downloads/approved/诺德"
        else:
            # 本地测试环境
            self.base_dir = os.path.join(os.getcwd(), "downloads", "诺德")
        
        # 确保目录存在
        os.makedirs(self.base_dir, exist_ok=True)
        
        self.processed_urls = self.load_processed_urls()
        self.new_files = []
        self.debug = True
        
        # 判断是否为首次运行（全量爬取）
        self.is_first_run = not os.path.exists(os.path.join(self.base_dir, 'processed_urls.pkl'))
        
        # 初始化WebDriver
        self.driver = None
        self.init_webdriver()
        
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
    
    def should_download_language(self, language):
        """判断是否应该下载该语言的文档"""
        if not language or language == "未知语言":
            return True  # 对于未知语言的文档，默认下载（可能是英文或中文）
        
        # 只下载英语和中文文档
        target_languages = [
            '英语', 'English', 'EN', 'en',
            '中文', '汉语', 'Chinese', 'ZH', 'zh', 'CN', 'cn',
            '美式英语', 'American English', 'US English'
        ]
        
        return any(target_lang in language for target_lang in target_languages)
    
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
    
    def get_main_categories(self):
        """获取主要产品分类"""
        try:
            # 为了确保电机分类能被爬取，直接使用备用分类
            self.log("🔍 直接使用备用分类（包含电机分类）")
            return self.get_fallback_categories()
            
        except Exception as e:
            self.log(f"❌ 获取分类失败: {str(e)}，使用备用分类")
            return self.get_fallback_categories()
    
    def get_fallback_categories(self):
        """备用分类列表"""
        return [
            {
                'name': '减速电机',
                'url': 'https://www.nord.cn/cn/products/geared-motors/geared-motors.jsp'
            },
            {
                'name': '驱动电子设备',
                'url': 'https://www.nord.cn/cn/products/drive-electronics/drive-electronics.jsp'
            },
            {
                'name': '工业齿轮箱',
                'url': 'https://www.nord.cn/cn/products/industrial-gear-units/industrial-gear-units.jsp'
            },
            {
                'name': '电机',
                'url': 'https://www.nord.cn/cn/products/motors/motors.jsp'
            }
        ]
    
    def find_product_links(self, soup, base_url):
        """查找产品链接"""
        product_links = []
        
        try:
            # 方法1: 查找产品卡片中的主要链接
            product_groups = soup.find_all('article', class_='product-group')
            
            for article in product_groups:
                # 从header中的h3 > a获取产品名和链接
                header = article.find('header')
                if header:
                    h3_link = header.find('h3').find('a') if header.find('h3') else None
                    if h3_link:
                        href = h3_link.get('href', '')
                        text = h3_link.get_text().strip()
                        
                        if href and text:
                            # 构建完整URL
                            if href.startswith('/'):
                                full_url = urljoin(self.base_url, href)
                            elif not href.startswith('http'):
                                # 相对路径，需要基于当前页面URL构建
                                full_url = urljoin(base_url, href)
                            else:
                                full_url = href
                            
                            product_links.append({
                                'name': text,
                                'url': full_url
                            })
                            self.log(f"   ✅ 找到产品: {text} -> {href}")
            
            # 方法2: 如果没找到产品卡片，使用原来的方法作为备用
            if not product_links:
                self.log("🔄 使用备用方法查找产品链接")
                all_links = soup.find_all('a', href=True)
                
                for link in all_links:
                    href = link.get('href', '')
                    text = link.get_text().strip()
                    
                    # 过滤条件：
                    # 1. 是产品相关链接
                    # 2. 有实际文本内容
                    # 3. 不是分类页面本身
                    if (href.startswith('/cn/products/') and 
                        len(text) > 2 and 
                        text not in ['产品展示', '减速电机', '电机', '工业齿轮箱', '产品详情'] and
                        not href.endswith('geared-motors.jsp') and
                        not href.endswith('motors.jsp') and
                        not href.endswith('drive-electronics.jsp')):
                        
                        full_url = urljoin(base_url, href)
                        
                        # 避免重复
                        if not any(p['url'] == full_url for p in product_links):
                            product_links.append({
                                'name': text,
                                'url': full_url
                            })
            
            self.log(f"🔍 找到 {len(product_links)} 个产品链接")
            
        except Exception as e:
            self.log(f"❌ 查找产品链接时出错: {str(e)}")
        
        return product_links
    
    def find_sub_products(self, soup, base_url):
        """查找子产品链接"""
        sub_products = []
        
        try:
            # 方法1: 查找"产品详情"按钮链接 - 针对DuoDrive等概览页面
            detail_links = soup.find_all('a', string=lambda text: text and '产品详情' in text.strip())
            
            for link in detail_links:
                href = link.get('href', '')
                if href:
                    # 构建完整URL
                    if href.startswith('/'):
                        full_url = urljoin(self.base_url, href)
                    elif not href.startswith('http'):
                        full_url = urljoin(base_url, href)
                    else:
                        full_url = href
                    
                    # 从页面标题获取产品名称
                    product_name = "产品详情"
                    title_tag = soup.find('h1')
                    if title_tag:
                        product_name = title_tag.get_text().strip() + "_详情"
                    
                    sub_products.append({
                        'name': product_name,
                        'url': full_url
                    })
                    self.log(f"   ✅ 找到产品详情页: {product_name} -> {href}")
            
            # 方法2: 在overview页面查找其他子产品链接
            if not sub_products:
                all_links = soup.find_all('a', href=True)
                
                for link in all_links:
                    href = link.get('href', '')
                    text = link.get_text().strip()
                    
                    # 查找子产品链接（通常以产品名开头）
                    if (href.startswith('/cn/products/') and 
                        len(text) > 2 and
                        not href.endswith('-overview.jsp') and  # 不是概览页面
                        not href.endswith('geared-motors.jsp') and  # 不是主分类页面
                        text not in ['产品展示', '减速电机', '电机', '工业齿轮箱']):
                        
                        full_url = urljoin(base_url, href)
                        
                        # 从URL路径提取更准确的产品名称
                        product_name = self.extract_product_name_from_url(href, text)
                        
                        # 避免重复
                        if not any(sp['url'] == full_url for sp in sub_products):
                            sub_products.append({
                                'name': product_name,
                                'url': full_url
                            })
                            self.log(f"   ✅ 找到子产品: {product_name} -> {href}")
            
            self.log(f"🔍 找到 {len(sub_products)} 个子产品")
            
        except Exception as e:
            self.log(f"❌ 查找子产品时出错: {str(e)}")
        
        return sub_products
    
    def extract_product_name_from_url(self, href, fallback_text):
        """从URL路径中提取产品名称"""
        try:
            # 从URL路径中提取产品名称
            path_parts = href.strip('/').split('/')
            
            # 寻找具体的产品文件名
            if len(path_parts) > 0:
                product_file = path_parts[-1]
                
                # 去掉.jsp扩展名
                if product_file.endswith('.jsp'):
                    product_file = product_file[:-4]
                
                # 根据特定的产品模式提取名称
                if 'unicase' in product_file.lower():
                    if 'bevel' in product_file.lower():
                        return "UNICASE-伞齿轮减速电机"
                    elif 'helical' in product_file.lower():
                        return "UNICASE-斜齿轮减速电机"
                    else:
                        return "UNICASE减速电机"
                        
                elif 'nordbloc1' in product_file.lower() or 'nordbloc.1' in product_file.lower():
                    if 'bevel' in product_file.lower():
                        return "NORDBLOC.1-伞齿轮减速电机"
                    elif 'helical' in product_file.lower():
                        return "NORDBLOC.1-斜齿轮减速电机"
                    else:
                        return "NORDBLOC.1减速电机"
                        
                elif 'standard-helical' in product_file.lower():
                    return "标准斜齿轮减速电机"
                    
                elif 'duodrive' in product_file.lower():
                    return "DuoDrive"
                    
                # 如果没有匹配到特定模式，尝试从URL中提取有意义的部分
                else:
                    # 将连字符替换为空格，首字母大写
                    formatted_name = product_file.replace('-', ' ').title()
                    if formatted_name and len(formatted_name) > 2:
                        return formatted_name
            
            # 如果URL提取失败，使用fallback_text
            return fallback_text + "_详情"
            
        except Exception as e:
            # 如果出错，返回fallback文本
            return fallback_text + "_详情"
    
    def get_actual_product_name(self, url):
        """访问产品页面获取真实的产品名称"""
        try:
            self.log(f"🔍 获取产品真实名称: {url}")
            
            soup = self.visit_page(url)
            if not soup:
                return None
            
            # 等待页面加载
            time.sleep(1)
            
            # 重新获取最新的页面内容
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            
            # 方法1: 查找header中的h3标题
            header_h3 = soup.find('header', class_='row')
            if header_h3:
                h3_tag = header_h3.find('h3')
                if h3_tag:
                    product_name = h3_tag.get_text().strip()
                    if product_name:
                        self.log(f"   📋 从header h3获取: {product_name}")
                        return product_name
            
            # 方法2: 查找普通的h3标题
            h3_tags = soup.find_all('h3')
            for h3 in h3_tags:
                text = h3.get_text().strip()
                # 确保是产品名称（包含关键词）
                if any(keyword in text for keyword in ['UNICASE', 'NORDBLOC', '减速', '电机', '齿轮']):
                    self.log(f"   📋 从h3标签获取: {text}")
                    return text
            
            # 方法3: 查找h1标题
            h1_tag = soup.find('h1')
            if h1_tag:
                product_name = h1_tag.get_text().strip()
                if product_name:
                    self.log(f"   📋 从h1获取: {product_name}")
                    return product_name
            
            # 方法4: 查找页面标题
            title_tag = soup.find('title')
            if title_tag:
                title_text = title_tag.get_text().strip()
                # 从标题中提取产品名称（去掉网站名等）
                if ' - ' in title_text:
                    product_name = title_text.split(' - ')[0].strip()
                    if product_name:
                        self.log(f"   📋 从title获取: {product_name}")
                        return product_name
            
            self.log(f"   ⚠️ 未能获取到产品名称")
            return None
            
        except Exception as e:
            self.log(f"   ❌ 获取产品名称时出错: {str(e)}")
            return None
    
    def click_downloads_tab(self):
        """点击下载标签页"""
        try:
            # 查找下载标签页
            downloads_tab = self.driver.find_element(By.CSS_SELECTOR, 
                'a[href="#downloads"], #downloads-tab, [aria-controls="downloads"]')
            
            if downloads_tab:
                # 滚动到标签页位置
                self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", downloads_tab)
                time.sleep(1)
                
                # 点击下载标签页
                downloads_tab.click()
                time.sleep(2)
                self.log("✅ 成功点击下载标签页")
                return True
                
        except Exception as e:
            self.log(f"⚠️ 点击下载标签页失败: {str(e)}")
            return False

    def expand_accordion_sections(self):
        """展开手风琴式折叠内容"""
        try:
            # 查找所有可能的折叠按钮
            collapse_buttons = self.driver.find_elements(By.CSS_SELECTOR, 
                '[data-toggle="collapse"], .btn.pl-accordion__btn, .collapsed')
            
            self.log(f"🔍 找到 {len(collapse_buttons)} 个可能的折叠按钮")
            
            for button in collapse_buttons:
                try:
                    # 检查是否是折叠状态
                    if 'collapsed' in button.get_attribute('class') or \
                       button.get_attribute('aria-expanded') == 'false':
                        
                        # 滚动到按钮位置
                        self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", button)
                        time.sleep(0.5)
                        
                        # 点击展开
                        button.click()
                        time.sleep(1)
                        self.log(f"✅ 展开折叠区域: {button.text[:50]}...")
                        
                except Exception as e:
                    continue
                    
        except Exception as e:
            self.log(f"⚠️ 展开折叠内容时出错: {str(e)}")

    def process_document_detail_page(self, detail_url, title, category, base_folder_path):
        """处理文档资料详情页面，提取下载链接"""
        try:
            self.log(f"📋 处理文档详情页: {title}")
            
            # 访问详情页面
            soup = self.visit_page(detail_url)
            if not soup:
                self.log(f"❌ 无法访问详情页: {detail_url}")
                return []
            
            downloads = []
            
            # 查找下载表格
            download_tables = soup.find_all('table', class_=lambda x: x and 'pl-table' in x)
            
            for table in download_tables:
                # 检查表格是否包含下载相关内容
                table_text = table.get_text().lower()
                if any(keyword in table_text for keyword in ['下载', 'pdf', 'download']):
                    
                    # 查找表格中的下载链接
                    pdf_links = table.find_all('a', href=lambda x: x and 'media.nord.cn' in x and '.pdf' in x)
                    
                    for link in pdf_links:
                        href = link.get('href', '')
                        if href:
                            # 获取语言信息（如果有的话）
                            row = link.find_parent('tr')
                            language = "未知语言"
                            if row:
                                th_elements = row.find_all('th')
                                if th_elements:
                                    language = th_elements[0].get_text().strip()
                            
                            # 语言过滤：只下载英语和中文文档
                            if self.should_download_language(language):
                                # 构建文件标题
                                file_title = f"{title}_{language}" if language != "未知语言" else title
                                
                                downloads.append({
                                    'title': file_title,
                                    'url': href,
                                    'category': category,
                                    'module': '文档详情页',
                                    'language': language
                                })
                                
                                self.log(f"   ✅ 找到下载: {file_title} -> {href}")
                            else:
                                self.log(f"   ⏭️ 跳过非目标语言: {title}_{language} ({language})")
            
            # 如果没有找到表格中的链接，尝试查找所有PDF链接
            if not downloads:
                all_pdf_links = soup.find_all('a', href=lambda x: x and 'media.nord.cn' in x and '.pdf' in x)
                for link in all_pdf_links:
                    href = link.get('href', '')
                    if href:
                        # 对于没有明确语言信息的链接，默认下载（可能是英文或中文）
                        downloads.append({
                            'title': title,
                            'url': href,
                            'category': category,
                            'module': '文档详情页',
                            'language': '未知语言'
                        })
                        self.log(f"   ✅ 找到PDF链接: {title} -> {href}")
            
            # 下载找到的文件
            if downloads:
                self.log(f"🚀 在详情页中找到 {len(downloads)} 个下载文件")
                
                for download in downloads:
                    doc_category = self.get_document_category(download['category'], download['title'])
                    
                    # 如果doc_category为None，直接放到产品根目录，减少层级
                    if doc_category:
                        folder_path = os.path.join(base_folder_path, doc_category)
                    else:
                        folder_path = base_folder_path
                    
                    filename = self.generate_clean_filename(download['url'], download['title'])
                    self.download_file(download['url'], filename, folder_path)
                    time.sleep(1)  # 下载间隔
            else:
                self.log(f"⚠️ 详情页中未找到下载文件: {detail_url}")
            
            return downloads
            
        except Exception as e:
            self.log(f"❌ 处理文档详情页时出错: {str(e)}")
            return []

    def find_download_modules(self, soup, page_url):
        """查找下载模块"""
        downloads = []
        
        try:
            # 先尝试点击下载标签页
            self.click_downloads_tab()
            time.sleep(2)
            
            # 再展开所有折叠内容
            self.expand_accordion_sections()
            time.sleep(2)
            
            # 重新获取页面源码
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            
            # 方法1: 专门查找下载标签页内容
            downloads_tab = soup.find('div', {'id': 'downloads', 'role': 'tabpanel'})
            if downloads_tab:
                self.log("🎯 找到下载标签页，开始解析内容")
                
                # 查找手风琴式内容
                accordion_cards = downloads_tab.find_all('div', class_='card pl-accordion__card')
                self.log(f"🔍 找到 {len(accordion_cards)} 个下载分类")
                
                for card in accordion_cards:
                    # 获取分类名称
                    header = card.find('h3', class_='pl-accordion__card-headline')
                    category = header.get_text().strip() if header else "未知分类"
                    
                    # 查找该分类下的文档
                    teasers = card.find_all('div', class_='teaser-document')
                    self.log(f"📋 {category} 分类中找到 {len(teasers)} 个文档")
                    
                    for teaser in teasers:
                        # 获取文档标题
                        title_element = teaser.find('h4', class_='teaser-title')
                        if title_element:
                            title_link = title_element.find('a')
                            doc_title = title_link.get_text().strip() if title_link else "未知文档"
                        else:
                            doc_title = "未知文档"
                        
                        # 查找下载链接
                        download_links = teaser.find_all('a', class_='icon-download')
                        for dl_link in download_links:
                            href = dl_link.get('href', '')
                            link_text = dl_link.get_text().strip()
                            
                            if href and 'media.nord.cn' in href:
                                file_url = href if href.startswith('http') else urljoin(self.base_url, href)
                                title = f"{doc_title} - {link_text}" if link_text else doc_title
                                
                                downloads.append({
                                    'title': title,
                                    'url': file_url,
                                    'module': f'{category}分类',
                                    'category': category
                                })
                                
                        # 查找详情页链接
                        detail_links = teaser.find_all('a', class_='icon-link')
                        for detail_link in detail_links:
                            href = detail_link.get('href', '')
                            if href and not href.startswith('http'):
                                detail_url = urljoin(page_url, href)
                                downloads.append({
                                    'title': f"{doc_title} - 详情页",
                                    'url': detail_url,
                                    'module': f'{category}详情',
                                    'category': category,
                                    'is_detail_page': True
                                })
            
            # 方法2: 查找下载模块/区域（原有逻辑保留作为备用）
            download_sections = soup.find_all(['div', 'section'], class_=lambda x: x and any(
                keyword in x.lower() for keyword in ['download', 'document', 'catalog', 'brochure', 'teaser']
            ))
            
            for section in download_sections:
                # 在每个下载区域中查找文件链接
                links = section.find_all('a', href=True)
                
                for link in links:
                    href = link.get('href', '')
                    text = link.get_text().strip()
                    
                    # 查找包含文件的链接
                    if any(domain in href for domain in ['media.nord.cn', 'pdf', 'doc', 'xlsx']):
                        
                        # 获取文件类型和标题
                        file_url = href if href.startswith('http') else urljoin(self.base_url, href)
                        title = text or "文档"
                        
                        # 避免重复
                        if not any(d['url'] == file_url for d in downloads):
                            downloads.append({
                                'title': title,
                                'url': file_url,
                                'module': '下载模块'
                            })
            
            # 方法3: 直接查找所有包含下载域名的链接
            all_links = soup.find_all('a', href=True)
            for link in all_links:
                href = link.get('href', '')
                text = link.get_text().strip()
                
                if 'media.nord.cn' in href:
                    file_url = href if href.startswith('http') else urljoin(self.base_url, href)
                    title = text or "诺德文档"
                    
                    # 避免重复
                    if not any(d['url'] == file_url for d in downloads):
                        downloads.append({
                            'title': title,
                            'url': file_url,
                            'module': '直接链接'
                        })
            
            if downloads:
                self.log(f"📎 在页面中找到 {len(downloads)} 个下载文件")
                for i, download in enumerate(downloads[:10]):  # 显示前10个
                    category = download.get('category', '')
                    category_info = f" ({category})" if category else ""
                    self.log(f"   {i+1}. {download['title']}{category_info}")
            else:
                self.log(f"❌ 页面中未找到下载文件: {page_url}")
            
        except Exception as e:
            self.log(f"❌ 查找下载模块时出错: {str(e)}")
        
        return downloads
    
    def download_file(self, url, filename, folder_path):
        """下载文件"""
        try:
            # 创建目录
            os.makedirs(folder_path, exist_ok=True)
            file_path = os.path.join(folder_path, filename)
            
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
            self.log(f"✅ 下载成功: {filename} ({file_size} bytes)")
            
            self.new_files.append({
                'filename': filename,
                'path': file_path,
                'url': url,
                'size': file_size
            })
            
            return True
            
        except Exception as e:
            self.log(f"❌ 下载失败 {filename}: {str(e)}")
            return False
    
    def generate_filename(self, url, title, module_name="", category=""):
        """生成文件名"""
        try:
            # 从URL中提取文件名和扩展名
            parsed_url = urlparse(url)
            filename_from_url = os.path.basename(parsed_url.path)
            
            # 确定文件扩展名
            extension = '.pdf'  # 默认扩展名
            if filename_from_url and '.' in filename_from_url:
                ext = filename_from_url.split('.')[-1].lower()
                if ext in ['pdf', 'doc', 'docx', 'xlsx', 'png', 'jpg', 'jpeg', 'webp']:
                    extension = f'.{ext}'
            elif '.pdf' in url.lower():
                extension = '.pdf'
            elif '.doc' in url.lower():
                extension = '.doc'
            elif '.xlsx' in url.lower():
                extension = '.xlsx'
            elif '.png' in url.lower():
                extension = '.png'
            elif '.webp' in url.lower():
                extension = '.webp'
            
            # 如果URL中有有效的文件名且不是数字ID，直接使用
            if filename_from_url and '.' in filename_from_url and not filename_from_url.split('.')[0].isdigit():
                filename = unquote(filename_from_url)
                # 添加分类前缀
                if category and category not in filename:
                    filename = f"{category}_{filename}"
                return filename
            
            # 否则从标题生成文件名
            safe_title = "".join(c for c in title if c.isalnum() or c in (' ', '-', '_', '（', '）', '(', ')', '–')).strip()
            safe_title = re.sub(r'\s+', '_', safe_title)  # 将空格替换为下划线
            safe_title = safe_title[:80]  # 适当增加长度限制
            
            # 构建文件名
            filename_parts = []
            if category and category not in safe_title:
                safe_category = "".join(c for c in category if c.isalnum() or c in (' ', '-', '_')).strip()
                safe_category = re.sub(r'\s+', '_', safe_category)
                filename_parts.append(safe_category)
            
            if module_name and module_name not in safe_title:
                safe_module = "".join(c for c in module_name if c.isalnum() or c in (' ', '-', '_')).strip()
                safe_module = re.sub(r'\s+', '_', safe_module)
                filename_parts.append(safe_module)
            
            filename_parts.append(safe_title)
            
            # 组合文件名
            filename = "_".join(filter(None, filename_parts)) + extension
            filename = re.sub(r'_{2,}', '_', filename)  # 去除重复下划线
            
            return filename
            
        except Exception as e:
            self.log(f"⚠️ 文件名生成失败: {str(e)}")
            return f"document_{int(time.time())}.pdf"
    
    def filter_valid_downloads(self, downloads):
        """过滤有效的下载文件，只保留PDF文档"""
        valid_downloads = []
        for download in downloads:
            # 跳过详情页链接
            if download.get('is_detail_page', False):
                continue
                
            url = download.get('url', '')
            title = download.get('title', '').lower()
            
            # 跳过不需要的文件类型
            if any(ext in url.lower() for ext in ['.jsp', '.png', '.jpg', '.jpeg', '.webp', '.gif']):
                continue
                
            # 跳过无用的文件名和商品介绍相关内容
            skip_keywords = ['文档', '图片', 'bild', 'image', '商品介绍', '产品介绍', 'product_intro', 
                           'intro', '介绍', '概述', 'overview_intro', '产品概览']
            if any(keyword in title for keyword in skip_keywords):
                continue
                
            # 只保留PDF文件或明确的文档链接
            if '.pdf' in url.lower() or any(keyword in title for keyword in ['manual', 'catalogue', '手册', '样本', '配件', '零件', 'spare', 'parts']):
                valid_downloads.append(download)
        
        return valid_downloads
    
    def get_document_category(self, category, title):
        """根据文档分类和标题确定子文件夹名称 - 简化文件夹结构"""
        category = category.lower() if category else ''
        title = title.lower() if title else ''
        
        # 跳过商品介绍相关内容
        if any(keyword in category or keyword in title for keyword in ['商品介绍', '产品介绍', '介绍', '概述']):
            return None  # 不创建文件夹，直接跳过
        
        # 简化分类，减少嵌套层级
        # 操作手册相关
        if any(keyword in category or keyword in title for keyword in ['操作手册', 'manual', 'maintenance', '维护']):
            return '操作手册'
        
        # 备件相关  
        elif any(keyword in category or keyword in title for keyword in ['备件', 'spare', 'parts', '配件', '零件']):
            return '备件'
            
        # 选型样本相关
        elif any(keyword in category or keyword in title for keyword in ['选型样本', 'catalogue', '样本', 'catalog']):
            return '选型样本'
            
        # 宣传资料相关
        elif any(keyword in category or keyword in title for keyword in ['宣传资料', 'brochure', '宣传', 'flyer']):
            return '宣传资料'
            
        # 其他文档 - 统一放到根目录，减少层级
        else:
            return None  # 直接放到产品根目录
    
    def generate_clean_filename(self, url, title, max_length=100):
        """生成清洁的文件名，不包含分类前缀"""
        try:
            # 清理标题
            clean_title = re.sub(r'[^\w\s\-\u4e00-\u9fff]', '', title)
            clean_title = re.sub(r'\s+', '_', clean_title.strip())
            
            # 去掉一些常见的前缀
            prefixes_to_remove = ['操作手册分类_操作手册_', '备件分类_备件_', '选型样本分类_选型样本_', '宣传资料分类_宣传资料_']
            for prefix in prefixes_to_remove:
                if clean_title.startswith(prefix):
                    clean_title = clean_title[len(prefix):]
                    break
            
            # 去掉无用词汇
            words_to_remove = ['下载', '详情', '文档资料详情', '文档', '_下载_', '_详情_', '_下载', '_详情']
            for word in words_to_remove:
                clean_title = clean_title.replace(word, '')
            
            # 清理连续的下划线
            clean_title = re.sub(r'_{2,}', '_', clean_title)
            clean_title = clean_title.strip('_')
            
            # 如果标题为空，使用默认名称
            if not clean_title:
                clean_title = f"document_{int(time.time())}"
            
            # 截断过长的文件名
            if len(clean_title) > max_length:
                clean_title = clean_title[:max_length]
            
            # 从URL获取文件扩展名
            parsed_url = urlparse(url)
            path = parsed_url.path
            ext = os.path.splitext(path)[1]
            
            if not ext:
                ext = '.pdf'  # 默认为PDF
            
            return f"{clean_title}{ext}"
            
        except Exception as e:
            self.log(f"⚠️ 文件名生成失败: {str(e)}")
            return f"document_{int(time.time())}.pdf"
    
    def process_product_detail_page(self, url, category_name, product_name, parent_product=None):
        """处理产品详情页面"""
        if url in self.processed_urls:
            self.log(f"⏭️ 跳过已处理页面: {url}")
            return
        
        # 如果是子产品，显示完整的层级信息
        if parent_product:
            self.log(f"📄 处理产品详情页: {parent_product}_{product_name}")
            display_name = f"{parent_product}_{product_name}"
        else:
            self.log(f"📄 处理产品详情页: {product_name}")
            display_name = product_name
            
        soup = self.visit_page(url)
        
        if not soup:
            return
        
        # 查找下载模块
        downloads = self.find_download_modules(soup, url)
        
        # 下载文件
        if downloads:
            # 简化文件夹结构 - 减少层级嵌套
            if parent_product:
                # 将父产品和子产品名称合并，避免过深的目录结构
                combined_name = f"{parent_product}_{product_name}"
                base_folder_path = os.path.join(self.base_dir, category_name, combined_name)
            else:
                base_folder_path = os.path.join(self.base_dir, category_name, product_name)
            
            # 分离直接下载文件和详情页链接
            file_downloads = self.filter_valid_downloads(downloads)
            detail_pages = [d for d in downloads if d.get('is_detail_page', False)]
            
            self.log(f"🚀 开始下载 {len(file_downloads)} 个直接文件到: {base_folder_path}")
            if detail_pages:
                self.log(f"📋 发现 {len(detail_pages)} 个详情页链接，将逐个处理")
            
            # 处理直接下载文件
            for download in file_downloads:
                module_name = download.get('module', '')
                category = download.get('category', '')
                
                # 根据文档分类创建子文件夹（如果需要的话）
                doc_category = self.get_document_category(category, download['title'])
                
                # 如果doc_category为None，直接放到产品根目录，减少层级
                if doc_category:
                    folder_path = os.path.join(base_folder_path, doc_category)
                else:
                    folder_path = base_folder_path
                
                # 生成清洁的文件名（不包含分类前缀）
                filename = self.generate_clean_filename(download['url'], download['title'])
                self.download_file(download['url'], filename, folder_path)
                time.sleep(1)  # 下载间隔
            
            # 处理详情页链接
            for detail_page in detail_pages:
                detail_title = detail_page['title'].replace(' - 详情页', '').strip()
                category = detail_page.get('category', '')
                
                # 处理文档详情页
                self.process_document_detail_page(
                    detail_page['url'], 
                    detail_title, 
                    category, 
                    base_folder_path
                )
                time.sleep(2)  # 详情页处理间隔
        
        # 标记为已处理
        self.processed_urls.add(url)
    
    def process_product_with_fallback(self, url, category_name, product_name):
        """处理产品页面（先尝试查找子产品，无子产品则作为详情页处理）"""
        if url in self.processed_urls:
            self.log(f"⏭️ 跳过已处理页面: {url}")
            return
        
        self.log(f"🔍 分析产品页面: {product_name}")
        soup = self.visit_page(url)
        
        if not soup:
            return
        
        # 先查找子产品
        sub_products = self.find_sub_products(soup, url)
        
        if sub_products:
            # 有子产品，按概览页面处理
            self.log(f"📋 找到 {len(sub_products)} 个子产品，按概览页面处理")
            self.process_product_overview_page(url, category_name, product_name)
        else:
            # 没有子产品，按详情页面处理
            self.log(f"📄 无子产品，按详情页面处理")
            self.process_product_detail_page(url, category_name, product_name)
    
    def process_product_overview_page(self, url, category_name, product_name):
        """处理产品概览页面（可能有子产品）"""
        if url in self.processed_urls:
            self.log(f"⏭️ 跳过已处理页面: {url}")
            return
        
        self.log(f"📋 处理产品概览页: {product_name}")
        soup = self.visit_page(url)
        
        if not soup:
            return
        
        # 先处理当前页面的下载
        downloads = self.find_download_modules(soup, url)
        if downloads:
            base_folder_path = os.path.join(self.base_dir, category_name, product_name)
            
            # 分离直接下载文件和详情页链接
            file_downloads = self.filter_valid_downloads(downloads)
            detail_pages = [d for d in downloads if d.get('is_detail_page', False)]
            
            # 处理直接下载文件
            for download in file_downloads:
                category = download.get('category', '')
                doc_category = self.get_document_category(category, download['title'])
                
                # 如果doc_category为None，直接放到产品根目录，减少层级
                if doc_category:
                    folder_path = os.path.join(base_folder_path, doc_category)
                else:
                    folder_path = base_folder_path
                
                filename = self.generate_clean_filename(download['url'], download['title'])
                self.download_file(download['url'], filename, folder_path)
                time.sleep(1)
            
            # 处理详情页链接
            for detail_page in detail_pages:
                detail_title = detail_page['title'].replace(' - 详情页', '').strip()
                category = detail_page.get('category', '')
                
                # 处理文档详情页
                self.process_document_detail_page(
                    detail_page['url'], 
                    detail_title, 
                    category, 
                    base_folder_path
                )
                time.sleep(2)  # 详情页处理间隔
        
        # 查找子产品
        sub_products = self.find_sub_products(soup, url)
        
        if sub_products:
            self.log(f"🔍 在 {product_name} 中找到 {len(sub_products)} 个子产品")
            
            for i, sub_product in enumerate(sub_products[:10]):  # 限制数量
                self.log(f"🔄 处理子产品 {i+1}/{len(sub_products[:10])}: {sub_product['name']}")
                
                # 获取子产品的真实名称
                actual_name = self.get_actual_product_name(sub_product['url'])
                
                # 处理子产品的详情页 - 为每个子产品创建独立目录
                if actual_name:
                    # 使用真实名称
                    sub_folder_name = actual_name
                    self.log(f"✅ 使用真实产品名称: {actual_name}")
                else:
                    # 使用备用名称
                    clean_sub_name = sub_product['name'].replace('_详情', '').replace('详情', '').strip()
                    if not clean_sub_name:
                        clean_sub_name = f"子产品{i+1}"
                    sub_folder_name = clean_sub_name
                    self.log(f"⚠️ 使用备用名称: {sub_folder_name}")
                
                self.process_product_detail_page(sub_product['url'], category_name, sub_folder_name, parent_product=product_name)
                time.sleep(2)
        
        # 标记为已处理
        self.processed_urls.add(url)
    
    def crawl_category(self, category):
        """爬取单个分类"""
        category_name = category['name']
        category_url = category['url']
        
        self.log(f"🚀 开始爬取分类: {category_name}")
        
        soup = self.visit_page(category_url)
        if not soup:
            return
        
        # 先处理分类页面本身的下载
        downloads = self.find_download_modules(soup, category_url)
        if downloads:
            folder_path = os.path.join(self.base_dir, category_name)
            for download in downloads:
                filename = self.generate_filename(download['url'], download['title'])
                self.download_file(download['url'], filename, folder_path)
                time.sleep(1)
        
        # 查找产品链接
        product_links = self.find_product_links(soup, category_url)
        
        if product_links:
            self.log(f"📋 在 {category_name} 分类中找到 {len(product_links)} 个产品")
            
            for i, product in enumerate(product_links[:10]):  # 限制数量避免过度爬取
                self.log(f"🔄 处理产品 {i+1}/{len(product_links[:10])}: {product['name']}")
                
                # 判断是概览页面还是详情页面
                if 'overview' in product['url']:
                    # 概览页面，可能有子产品
                    self.process_product_overview_page(product['url'], category_name, product['name'])
                else:
                    # 先尝试作为概览页面处理（查找子产品）
                    # 如果没有子产品，则作为详情页面处理
                    self.process_product_with_fallback(product['url'], category_name, product['name'])
                
                time.sleep(3)  # 产品间延迟
        
        self.log(f"✅ 完成分类: {category_name}")
    
    def run(self):
        """运行爬虫"""
        try:
            self.log("🚀 开始运行诺德文档爬虫 (V2版本)")
            
            # 获取主要分类
            categories = self.get_main_categories()
            
            if not categories:
                self.log("❌ 未找到任何分类，退出")
                return
            
            self.log(f"📋 找到 {len(categories)} 个主要分类")
            
            # 爬取每个分类
            for category in categories:
                self.crawl_category(category)
                time.sleep(5)  # 分类间延迟
            
            # 保存进度
            self.save_processed_urls()
            
            # 统计结果
            total_files = len(self.new_files)
            self.log(f"🎉 爬取完成！共下载 {total_files} 个新文件")
            
            if self.new_files:
                self.log("📁 新下载的文件:")
                for file_info in self.new_files[:10]:  # 显示前10个
                    self.log(f"   📄 {file_info['filename']} ({file_info['size']} bytes)")
                
                if len(self.new_files) > 10:
                    self.log(f"   ... 还有 {len(self.new_files) - 10} 个文件")
            
        except Exception as e:
            self.log(f"❌ 爬虫运行出错: {str(e)}")
            
        finally:
            # 关闭WebDriver
            if self.driver:
                self.driver.quit()
                self.log("🔒 WebDriver已关闭")

def test_detail_page_processing(detail_url=None):
    """测试文档详情页处理功能"""
    spider = NordSeleniumSpiderV2()
    
    try:
        # 默认测试URL（用户提供的示例）
        test_url = detail_url or "https://www.nord.cn/cn/service/documentation/manuals/details/b1033a.jsp"
        
        spider.log(f"🧪 测试文档详情页处理功能")
        spider.log(f"📋 测试URL: {test_url}")
        
        # 创建测试目录
        test_folder = os.path.join(spider.base_dir, "测试", "详情页测试")
        
        # 处理详情页
        downloads = spider.process_document_detail_page(
            test_url, 
            "B1033A_Universal_Worm_Gear_Units_Kits", 
            "操作手册", 
            test_folder
        )
        
        if downloads:
            spider.log(f"✅ 测试成功！找到 {len(downloads)} 个下载文件")
            for download in downloads:
                spider.log(f"   📄 {download['title']} -> {download['url']}")
        else:
            spider.log(f"⚠️ 测试完成，但未找到下载文件")
        
    except Exception as e:
        spider.log(f"❌ 测试失败: {str(e)}")
    finally:
        if spider.driver:
            spider.driver.quit()

if __name__ == "__main__":
    import sys
    
    # 检查是否是测试模式
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        # 测试模式
        test_url = sys.argv[2] if len(sys.argv) > 2 else None
        test_detail_page_processing(test_url)
    else:
        # 正常运行
        spider = NordSeleniumSpiderV2()
        spider.run()
