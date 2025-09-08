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

class JTEKTSpider:
    def __init__(self):
        # 基础配置
        self.base_url = "https://www.jtektele.com.cn"
        
        # 服务器固定路径（按规范要求），本地测试使用当前目录
        if os.path.exists("/srv/downloads/approved/"):
            self.base_dir = "/srv/downloads/approved/光洋"
        else:
            # 本地测试环境
            self.base_dir = os.path.join(os.getcwd(), "downloads", "光洋")
        
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
        
        # 主要爬取模块
        self.main_modules = [
            {
                'name': '资料下载',
                'url': 'https://www.jtektele.com.cn/index.php/download',
                'categories': [
                    {'name': '产品选型样本', 'url': 'https://www.jtektele.com.cn/index.php/download/8'},
                    {'name': 'NK0/NK1系列PLC技术资料', 'url': 'https://www.jtektele.com.cn/index.php/download/9'},
                    {'name': 'SN系列PLC技术资料', 'url': 'https://www.jtektele.com.cn/index.php/download/10'},
                    {'name': 'DL05/06系列PLC技术资料', 'url': 'https://www.jtektele.com.cn/index.php/download/11'},
                    {'name': 'DL205/SZ系列PLC技术资料', 'url': 'https://www.jtektele.com.cn/index.php/download/12'},
                    {'name': 'DL405/SU系列PLC技术资料', 'url': 'https://www.jtektele.com.cn/index.php/download/13'},
                    {'name': 'SJ / CLICK系列PLC技术资料', 'url': 'https://www.jtektele.com.cn/index.php/download/46'},
                    {'name': '其它系列PLC技术资料', 'url': 'https://www.jtektele.com.cn/index.php/download/14'},
                    {'name': 'PLC共通技术资料', 'url': 'https://www.jtektele.com.cn/index.php/download/15'},
                    {'name': 'GC-A2/H00系列触摸屏技术资料', 'url': 'https://www.jtektele.com.cn/index.php/download/28'},
                    {'name': 'Cmore/EA7E系列触摸屏技术资料', 'url': 'https://www.jtektele.com.cn/index.php/download/25'},
                    {'name': 'GC/EA7EAIP系列触摸屏技术资料', 'url': 'https://www.jtektele.com.cn/index.php/download/16'},
                    {'name': '显示设定单元技术资料', 'url': 'https://www.jtektele.com.cn/index.php/download/29'},
                    {'name': '编码器相关技术资料', 'url': 'https://www.jtektele.com.cn/index.php/download/30'},
                    {'name': 'KSD-A3伺服系统技术资料', 'url': 'https://www.jtektele.com.cn/index.php/download/45'},
                    {'name': '变频器产品技术资料', 'url': 'https://www.jtektele.com.cn/index.php/download/31'},
                    {'name': '其他产品技术资料', 'url': 'https://www.jtektele.com.cn/index.php/download/32'},
                    {'name': '软件下载', 'url': 'https://www.jtektele.com.cn/index.php/download/33'},
                    {'name': '认证标志下载', 'url': 'https://www.jtektele.com.cn/index.php/download/39'}
                ]
            },
            {
                'name': '教学视频',
                'url': 'https://www.jtektele.com.cn/index.php/video',
                'categories': [
                    {'name': 'NK1系列PLC教学视频', 'url': 'https://www.jtektele.com.cn/index.php/video/36'},
                    {'name': '编码器系列产品介绍视频', 'url': 'https://www.jtektele.com.cn/index.php/video/41'},
                    {'name': 'GC-A2系列触摸屏介绍视频', 'url': 'https://www.jtektele.com.cn/index.php/video/37'},
                    {'name': 'NK0系列PLC教学视频', 'url': 'https://www.jtektele.com.cn/index.php/video/38'},
                    {'name': 'YKAN次世代HMI介绍视频', 'url': 'https://www.jtektele.com.cn/index.php/video/40'},
                    {'name': '远程I/O单元', 'url': 'https://www.jtektele.com.cn/index.php/video/42'},
                    {'name': 'JX系列PLC介绍视频', 'url': 'https://www.jtektele.com.cn/index.php/video/43'},
                    {'name': '伺服产品介绍视频', 'url': 'https://www.jtektele.com.cn/index.php/video/44'}
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
    
    def find_download_links(self, soup, page_url):
        """从页面中查找下载链接"""
        downloads = []
        
        try:
            # 方法1: 查找具有download属性的链接（主要下载区域）
            download_links = soup.find_all('a', {'download': True, 'href': True})
            self.log(f"🔍 找到 {len(download_links)} 个带download属性的链接")
            
            for link in download_links:
                href = link.get('href', '')
                download_name = link.get('download', '')
                
                # 获取显示文本
                text_element = link.find('p')
                display_text = text_element.get_text().strip() if text_element else download_name
                
                if href and display_text:
                    # 构建完整URL
                    if href.startswith('/'):
                        full_url = urljoin(self.base_url, href)
                    elif not href.startswith('http'):
                        full_url = urljoin(page_url, href)
                    else:
                        full_url = href
                    
                    downloads.append({
                        'title': display_text,
                        'url': full_url,
                        'filename': download_name,
                        'type': 'direct_download'
                    })
                    
                    self.log(f"   ✅ 找到下载: {display_text} -> {full_url}")
            
            # 方法2: 查找所有包含文件扩展名的链接
            all_links = soup.find_all('a', href=True)
            self.log(f"🔍 总共找到 {len(all_links)} 个链接")
            
            for link in all_links:
                href = link.get('href', '')
                text = link.get_text().strip()
                
                # 查找包含文档扩展名的链接
                if any(ext in href.lower() for ext in ['.pdf', '.doc', '.docx', '.xls', '.xlsx', '.zip', '.rar']):
                    full_url = href if href.startswith('http') else urljoin(page_url, href)
                    
                    # 避免重复
                    if not any(d['url'] == full_url for d in downloads):
                        downloads.append({
                            'title': text or "下载文件",
                            'url': full_url,
                            'filename': '',
                            'type': 'document_link'
                        })
                        
                        self.log(f"   ✅ 找到文档: {text} -> {full_url}")
            
            # 方法3: 查找教育视频链接（特定结构）
            video_div = soup.find('div', class_='inner_fl_video')
            if video_div:
                self.log(f"🔍 找到教育视频区域")
                video_links = video_div.find_all('a', href=True)
                
                for link in video_links:
                    href = link.get('href', '')
                    video_txt_div = link.find('div', class_='video_txt')
                    title = "教学视频"
                    
                    if video_txt_div:
                        p_tag = video_txt_div.find('p')
                        if p_tag:
                            title = p_tag.get_text().strip()
                    
                    if href and 'videoshow' in href:
                        full_url = href if href.startswith('http') else urljoin(page_url, href)
                        
                        # 避免重复
                        if not any(d['url'] == full_url for d in downloads):
                            downloads.append({
                                'title': title,
                                'url': full_url,
                                'filename': '',
                                'type': 'video_page'
                            })
                            
                            self.log(f"   ✅ 找到教育视频页面: {title} -> {full_url}")
            
            # 方法4: 查找其他视频链接（对于普通视频页面）
            if '/video/' in page_url or 'videoshow' in page_url:
                # 查找视频相关的链接
                video_links = soup.find_all('a', href=True)
                for link in video_links:
                    href = link.get('href', '')
                    text = link.get_text().strip()
                    
                    # 查找包含视频文件的链接或视频平台链接
                    if any(ext in href.lower() for ext in ['.mp4', '.avi', '.mov', '.wmv', '.flv']) or \
                       any(platform in href.lower() for platform in ['youtube', 'youku', 'bilibili']):
                        
                        full_url = href if href.startswith('http') else urljoin(page_url, href)
                        
                        # 避免重复
                        if not any(d['url'] == full_url for d in downloads):
                            downloads.append({
                                'title': text or "教学视频",
                                'url': full_url,
                                'filename': '',
                                'type': 'video_direct'
                            })
                            
                            self.log(f"   ✅ 找到直接视频: {text} -> {full_url}")
            
            # 方法5: 查找按钮或图片链接，可能隐藏下载链接
            button_links = soup.find_all(['button', 'div', 'span'], onclick=True)
            for element in button_links:
                onclick = element.get('onclick', '')
                if 'download' in onclick.lower() or 'file' in onclick.lower():
                    self.log(f"   🔍 找到可能的下载按钮: {onclick}")
            
            # 方法6: 查找iframe或embed标签（可能包含文档预览）
            iframes = soup.find_all(['iframe', 'embed'])
            for iframe in iframes:
                src = iframe.get('src', '')
                if src and any(ext in src.lower() for ext in ['.pdf', '.doc', '.docx']):
                    full_url = src if src.startswith('http') else urljoin(page_url, src)
                    if not any(d['url'] == full_url for d in downloads):
                        downloads.append({
                            'title': "嵌入文档",
                            'url': full_url,
                            'filename': '',
                            'type': 'embedded_document'
                        })
                        self.log(f"   ✅ 找到嵌入文档: {full_url}")
            
            if downloads:
                self.log(f"📎 在页面中找到 {len(downloads)} 个下载文件")
            else:
                self.log(f"❌ 页面中未找到下载文件: {page_url}")
                # 输出一些调试信息
                self.log(f"🔍 页面标题: {soup.title.string if soup.title else '无标题'}")
                forms = soup.find_all('form')
                self.log(f"🔍 页面表单数量: {len(forms)}")
                scripts = soup.find_all('script')
                self.log(f"🔍 页面脚本数量: {len(scripts)}")
            
        except Exception as e:
            self.log(f"❌ 查找下载链接时出错: {str(e)}")
        
        return downloads
    
    def find_pagination_links(self, soup, base_url):
        """查找分页链接"""
        pagination_links = []
        
        try:
            # 方法1: 查找分页导航区域
            pagination_div = soup.find('div', class_='pagination')
            if not pagination_div:
                # 尝试其他可能的分页类名
                pagination_div = soup.find('div', class_='page')
                if not pagination_div:
                    pagination_div = soup.find('ul', class_='pagination')
                    if not pagination_div:
                        pagination_div = soup.find('div', class_='pages')
            
            if pagination_div:
                self.log(f"🔍 找到分页导航区域: {pagination_div.get('class', 'unknown')}")
                
                # 查找所有分页链接
                page_links = pagination_div.find_all('a', href=True)
                
                for link in page_links:
                    href = link.get('href', '')
                    text = link.get_text().strip()
                    
                    # 过滤掉"上一页"、"下一页"等导航链接，只保留页码链接
                    if text.isdigit() and href:
                        # 构建完整的分页URL
                        if href.startswith('/'):
                            full_url = urljoin(self.base_url, href)
                        elif not href.startswith('http'):
                            full_url = urljoin(base_url, href)
                        else:
                            full_url = href
                        
                        # 避免重复
                        if full_url not in [p['url'] for p in pagination_links]:
                            pagination_links.append({
                                'page': int(text),
                                'url': full_url,
                                'text': text
                            })
                
                # 按页码排序
                pagination_links.sort(key=lambda x: x['page'])
                
                if pagination_links:
                    self.log(f"📄 找到 {len(pagination_links)} 个分页链接")
                    for page_info in pagination_links:
                        self.log(f"   📄 第{page_info['page']}页: {page_info['url']}")
                else:
                    self.log(f"⚠️ 分页导航区域中未找到有效的页码链接")
            
            # 方法2: 如果方法1失败，尝试查找所有可能的分页链接
            if not pagination_links:
                self.log(f"🔍 尝试备用方法查找分页链接")
                
                # 查找所有链接，寻找分页模式
                all_links = soup.find_all('a', href=True)
                page_patterns = []
                
                for link in all_links:
                    href = link.get('href', '')
                    text = link.get_text().strip()
                    
                    # 查找分页URL模式
                    if text.isdigit() and href:
                        # 检查是否是分页URL（包含download和数字）
                        if '/download/' in href and any(char.isdigit() for char in href):
                            # 构建完整的分页URL
                            if href.startswith('/'):
                                full_url = urljoin(self.base_url, href)
                            elif not href.startswith('http'):
                                full_url = urljoin(base_url, href)
                            else:
                                full_url = href
                            
                            # 避免重复
                            if full_url not in [p['url'] for p in pagination_links]:
                                page_patterns.append({
                                    'page': int(text),
                                    'url': full_url,
                                    'text': text
                                })
                
                # 如果找到分页模式，添加到结果中
                if page_patterns:
                    page_patterns.sort(key=lambda x: x['page'])
                    pagination_links.extend(page_patterns)
                    self.log(f"📄 备用方法找到 {len(page_patterns)} 个分页链接")
                    for page_info in page_patterns:
                        self.log(f"   📄 第{page_info['page']}页: {page_info['url']}")
            
            # 方法3: 手动构建分页URL（针对光洋网站的特殊情况）
            if not pagination_links and '/download/' in base_url:
                self.log(f"🔍 尝试手动构建分页URL")
                
                # 从基础URL中提取分类ID
                import re
                match = re.search(r'/download/(\d+)', base_url)
                if match:
                    category_id = match.group(1)
                    self.log(f"   🔍 提取到分类ID: {category_id}")
                    
                    # 尝试构建前几页的URL
                    for page_num in range(2, 6):  # 尝试2-5页
                        page_url = f"{self.base_url}/index.php/download/{category_id}/{page_num}"
                        
                        # 检查页面是否存在（简单验证）
                        try:
                            test_response = requests.head(page_url, timeout=5)
                            if test_response.status_code == 200:
                                pagination_links.append({
                                    'page': page_num,
                                    'url': page_url,
                                    'text': str(page_num)
                                })
                                self.log(f"   📄 手动构建第{page_num}页: {page_url}")
                        except:
                            continue
                    
                    if pagination_links:
                        self.log(f"📄 手动构建找到 {len(pagination_links)} 个分页链接")
            
            if not pagination_links:
                self.log(f"ℹ️ 页面中未找到分页导航")
                
        except Exception as e:
            self.log(f"❌ 查找分页链接时出错: {str(e)}")
        
        return pagination_links
    
    def process_category_with_pagination(self, module_name, category):
        """处理分类页面，包括分页内容"""
        category_name = category['name']
        category_url = category['url']
        
        if category_url in self.processed_urls:
            self.log(f"⏭️ 跳过已处理分类: {category_name}")
            return
        
        self.log(f"📋 处理分类: {module_name} -> {category_name}")
        
        # 处理第一页
        soup = self.visit_page(category_url)
        if not soup:
            return
        
        # 查找分页链接
        pagination_links = self.find_pagination_links(soup, category_url)
        
        # 创建模块目录
        safe_category_name = category_name.replace('/', '_').replace('\\', '_')
        folder_path = os.path.join(self.base_dir, module_name, safe_category_name)
        
        total_downloads = 0
        
        # 处理第一页
        self.log(f"📄 处理第1页: {category_url}")
        downloads_page1 = self.find_download_links(soup, category_url)
        if downloads_page1:
            total_downloads += len(downloads_page1)
            self.log(f"🚀 第1页找到 {len(downloads_page1)} 个文件")
            self.process_downloads(downloads_page1, folder_path)
        
        # 处理其他分页
        for page_info in pagination_links:
            page_url = page_info['url']
            page_num = page_info['page']
            
            # 检查是否已处理过此分页
            if page_url in self.processed_urls:
                self.log(f"⏭️ 跳过已处理分页: 第{page_num}页")
                continue
            
            self.log(f"📄 处理第{page_num}页: {page_url}")
            
            # 访问分页
            page_soup = self.visit_page(page_url)
            if not page_soup:
                continue
            
            # 查找分页中的下载链接
            page_downloads = self.find_download_links(page_soup, page_url)
            if page_downloads:
                total_downloads += len(page_downloads)
                self.log(f"🚀 第{page_num}页找到 {len(page_downloads)} 个文件")
                self.process_downloads(page_downloads, folder_path)
            
            # 标记分页为已处理
            self.processed_urls.add(page_url)
            
            # 分页间延迟
            time.sleep(2)
        
        if total_downloads > 0:
            self.log(f"✅ 分类 {category_name} 处理完成，共找到 {total_downloads} 个文件")
        else:
            self.log(f"⚠️ 分类 {category_name} 未找到任何文件")
        
        # 标记主分类为已处理
        self.processed_urls.add(category_url)
    
    def process_downloads(self, downloads, folder_path):
        """处理下载列表"""
        for download in downloads:
            try:
                title = download['title']
                url = download['url']
                file_type = download.get('type', 'unknown')
                
                # 处理视频页面
                if file_type == 'video_page':
                    actual_video_url = self.process_video_page(url, title, folder_path)
                    if actual_video_url and actual_video_url != url:
                        # 如果找到了实际视频URL，更新下载信息
                        url = actual_video_url
                        file_type = 'video_direct'
                
                if file_type in ['video_link', 'video_page', 'video_direct']:
                    # 保存视频信息
                    video_info = {
                        'title': title,
                        'url': url,
                        'original_url': download['url'] if url != download['url'] else url,
                        'category': os.path.basename(folder_path),
                        'module': os.path.basename(os.path.dirname(folder_path)),
                        'crawl_time': datetime.now().isoformat()
                    }
                    
                elif file_type in ['direct_download', 'document_link', 'embedded_document']:
                    # 下载文件
                    filename = self.generate_clean_filename(
                        title, 
                        download.get('filename', ''), 
                        url
                    )
                    
                    self.download_file(url, filename, folder_path)
                    
            except Exception as e:
                self.log(f"❌ 处理下载项时出错: {str(e)}")
                continue
            
            time.sleep(1)  # 下载间隔
    
    def process_video_page(self, video_url, title, folder_path=None):
        """处理视频页面，提取实际视频下载链接"""
        try:
            self.log(f"🎥 处理视频页面: {title}")
            soup = self.visit_page(video_url)
            
            if not soup:
                return None
            
            # 查找视频文件链接
            video_sources = []
            
            # 方法1: 查找video标签的src
            video_tags = soup.find_all('video')
            for video in video_tags:
                src = video.get('src', '')
                if src:
                    full_url = src if src.startswith('http') else urljoin(video_url, src)
                    video_sources.append(full_url)
                    self.log(f"   📹 找到video标签src: {full_url}")
                
                # 查找source标签 - 重点处理这种结构
                sources = video.find_all('source')
                for source in sources:
                    src = source.get('src', '')
                    if src:
                        full_url = src if src.startswith('http') else urljoin(video_url, src)
                        video_sources.append(full_url)
                        self.log(f"   📹 找到source标签src: {full_url}")
            
            # 方法2: 单独查找source标签（防止嵌套遗漏）
            all_sources = soup.find_all('source', {'type': 'video/mp4'})
            for source in all_sources:
                src = source.get('src', '')
                if src:
                    full_url = src if src.startswith('http') else urljoin(video_url, src)
                    if full_url not in video_sources:
                        video_sources.append(full_url)
                        self.log(f"   📹 找到独立source标签: {full_url}")
            
            # 方法3: 查找iframe中的视频
            iframes = soup.find_all('iframe')
            for iframe in iframes:
                src = iframe.get('src', '')
                if src and any(platform in src.lower() for platform in ['youtube', 'youku', 'bilibili']):
                    video_sources.append(src)
                    self.log(f"   📹 找到iframe视频: {src}")
            
            # 方法4: 查找JavaScript中的视频URL
            scripts = soup.find_all('script')
            for script in scripts:
                if script.string:
                    # 查找常见的视频URL模式，特别关注光洋网站的upload路径
                    import re
                    video_patterns = [
                        r'src["\']?\s*:\s*["\']([^"\']+\.(?:mp4|avi|mov|wmv|flv))["\']',
                        r'url["\']?\s*:\s*["\']([^"\']+\.(?:mp4|avi|mov|wmv|flv))["\']',
                        r'["\']([^"\']*\.(?:mp4|avi|mov|wmv|flv))["\']',
                        # 特别针对光洋网站的upload路径
                        r'["\']([^"\']*upload[^"\']*\.mp4)["\']',
                        r'["\']([^"\']*\/upload\/[^"\']*\.mp4)["\']'
                    ]
                    
                    for pattern in video_patterns:
                        matches = re.findall(pattern, script.string, re.IGNORECASE)
                        for match in matches:
                            full_url = match if match.startswith('http') else urljoin(video_url, match)
                            if full_url not in video_sources:
                                video_sources.append(full_url)
                                self.log(f"   📹 找到JS中的视频: {full_url}")
            
            # 方法5: 查找页面HTML中的upload路径视频
            import re
            page_content = str(soup)
            upload_patterns = [
                r'(["\']?[^"\']*upload[^"\']*\.mp4["\']?)',
                r'(["\']?\/upload\/[^"\']*\.mp4["\']?)',
                r'(upload\/[^"\']*\.mp4)'
            ]
            
            for pattern in upload_patterns:
                matches = re.findall(pattern, page_content, re.IGNORECASE)
                for match in matches:
                    # 清理引号
                    clean_match = match.strip('\'"')
                    full_url = clean_match if clean_match.startswith('http') else urljoin(video_url, clean_match)
                    if full_url not in video_sources and '.mp4' in full_url:
                        video_sources.append(full_url)
                        self.log(f"   📹 找到upload路径视频: {full_url}")
            
            # 去重并过滤有效的MP4链接
            valid_sources = []
            for source in video_sources:
                if source not in valid_sources and source.endswith('.mp4'):
                    valid_sources.append(source)
            
            if valid_sources:
                self.log(f"   ✅ 找到 {len(valid_sources)} 个有效视频源")
                # 直接下载视频文件
                video_source = valid_sources[0]
                filename = self.generate_clean_filename(title, url=video_source)
                
                # 创建视频下载目录
                video_dir = folder_path if folder_path else os.path.join(self.base_dir, "视频")
                
                # 下载视频文件
                success = self.download_file(video_source, filename, video_dir)
                if success:
                    return video_source
                else:
                    self.log(f"❌ 视频下载失败: {title}")
                    return None
            else:
                # 调试：保存页面内容以便分析
                if self.debug:
                    debug_file = f"debug_video_page_{int(time.time())}.html"
                    debug_path = os.path.join(self.base_dir, debug_file)
                    with open(debug_path, 'w', encoding='utf-8') as f:
                        f.write(str(soup))
                    self.log(f"   🔍 已保存页面内容到: {debug_path}")
                
                self.log(f"   ❌ 未找到视频源，返回页面链接")
                return video_url  # 如果找不到直接视频，返回页面链接
                
        except Exception as e:
            self.log(f"❌ 处理视频页面时出错: {str(e)}")
            return video_url

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
    
    def generate_clean_filename(self, title, download_filename="", url=""):
        """生成清洁的文件名"""
        try:
            # 如果有指定的下载文件名，优先使用
            if download_filename:
                return download_filename
            
            # 否则从标题生成文件名
            # 保留"/"字符，但替换其他特殊字符
            clean_title = re.sub(r'[^\w\s\-\u4e00-\u9fff\/]', '', title)
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
    

    def process_category_page(self, module_name, category):
        """处理分类页面（现在调用分页处理方法）"""
        # 使用新的分页处理方法
        self.process_category_with_pagination(module_name, category)
    
    def clear_video_module_progress(self):
        """清除视频模块的进度记录，允许重新爬取"""
        try:
            # 找到所有视频相关的URL并清除
            video_urls_to_clear = []
            
            # 教学视频模块的主页
            video_module = None
            for module in self.main_modules:
                if module['name'] == '教学视频':
                    video_module = module
                    break
            
            if video_module:
                # 添加主页面URL
                video_urls_to_clear.append(video_module['url'])
                
                # 添加所有分类URL
                for category in video_module['categories']:
                    video_urls_to_clear.append(category['url'])
                
                # 从已处理列表中移除这些URL
                for url in video_urls_to_clear:
                    if url in self.processed_urls:
                        self.processed_urls.remove(url)
                        self.log(f"🔄 清除进度记录: {url}")
                
                self.log(f"✅ 已清除 {len(video_urls_to_clear)} 个视频模块的进度记录")
                return True
            else:
                self.log("❌ 未找到教学视频模块")
                return False
                
        except Exception as e:
            self.log(f"❌ 清除视频模块进度时出错: {str(e)}")
            return False
    
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
                if download['type'] == 'video_link':
                    # 保存视频信息
                    video_info = {
                        'title': download['title'],
                        'url': download['url'],
                        'category': "通用资料",
                        'module': module_name,
                        'crawl_time': datetime.now().isoformat()
                    }

                    
                elif download['type'] in ['direct_download', 'document_link']:
                    # 下载文件
                    filename = self.generate_clean_filename(
                        download['title'], 
                        download.get('filename', ''), 
                        download['url']
                    )
                    
                    self.download_file(download['url'], filename, folder_path)
                
                time.sleep(1)
        
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
            self.log("🚀 开始运行光洋（捷太格特）文档爬虫")
            
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
                notification_message = f"""光洋（捷太格特）爬虫完成！
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
                notification_message = f"""光洋（捷太格特）爬虫完成！
📊 本次未发现新文件
• 爬取耗时：{duration}
• 完成时间：{end_time.strftime('%Y-%m-%d %H:%M:%S')}"""
                
                self.send_dingtalk_notification(notification_message)
                self.log("ℹ️ 本次爬取未发现新文件")
            
        except Exception as e:
            error_message = f"光洋（捷太格特）爬虫运行出错：{str(e)}"
            self.log(f"❌ {error_message}")
            self.send_dingtalk_notification(error_message)
            
        finally:
            # 关闭WebDriver
            if self.driver:
                self.driver.quit()
                self.log("🔒 WebDriver已关闭")

def test_single_category(category_url=None):
    """测试单个分类的爬取功能"""
    spider = JTEKTSpider()
    
    try:
        # 默认测试URL
        test_url = category_url or "https://www.jtektele.com.cn/index.php/download/8"
        
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

def test_pagination(category_url=None):
    """测试分页功能"""
    spider = JTEKTSpider()
    
    try:
        # 默认测试NK0/NK1系列PLC技术资料
        test_url = category_url or "https://www.jtektele.com.cn/index.php/download/9"
        
        spider.log(f"🧪 测试分页功能")
        spider.log(f"📋 测试URL: {test_url}")
        
        # 创建测试分类配置
        test_category = {
            'name': 'NK0/NK1系列PLC技术资料',
            'url': test_url
        }
        
        # 处理测试分类（包含分页）
        spider.process_category_with_pagination("测试模块", test_category)
        
        if spider.new_files:
            spider.log(f"✅ 分页测试成功！找到 {len(spider.new_files)} 个文件")
            for file_info in spider.new_files:
                spider.log(f"   📄 {file_info['filename']}")
        else:
            spider.log(f"⚠️ 分页测试完成，但未找到新文件")
        
    except Exception as e:
        spider.log(f"❌ 分页测试失败: {str(e)}")
    finally:
        if spider.driver:
            spider.driver.quit()

if __name__ == "__main__":
    import sys
    
    # 检查是否是测试模式
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        # 测试模式
        test_url = sys.argv[2] if len(sys.argv) > 2 else None
        test_single_category(test_url)
    elif len(sys.argv) > 1 and sys.argv[1] == "test_pagination":
        # 分页测试模式
        test_url = sys.argv[2] if len(sys.argv) > 2 else None
        test_pagination(test_url)
    elif len(sys.argv) > 1 and sys.argv[1] == "test_video":
        # 视频测试模式
        spider = JTEKTSpider()
        try:
            # 测试视频页面处理
            test_video_url = "https://www.jtektele.com.cn/index.php/videoshow/2"  # NK1工程数据读出
            spider.log("🧪 测试视频页面处理功能")
            spider.log(f"📹 测试视频URL: {test_video_url}")
            
            # 处理视频页面
            video_source = spider.process_video_page(test_video_url, "NK1系列PLC教学视频")
            
            if video_source and video_source.endswith('.mp4'):
                spider.log(f"✅ 视频测试成功！视频已下载完成")
                
            else:
                spider.log("⚠️ 视频测试完成，但未找到有效的MP4视频源")
                
        except Exception as e:
            spider.log(f"❌ 视频测试失败: {str(e)}")
        finally:
            if spider.driver:
                spider.driver.quit()
                spider.log("🔒 WebDriver已关闭")
    elif len(sys.argv) > 1 and sys.argv[1] == "videos":
        # 视频模块爬取模式
        spider = JTEKTSpider()
        try:
            spider.log("🚀 开始运行光洋（捷太格特）教学视频爬虫")
            
            # 只处理教学视频模块
            video_module = None
            for module in spider.main_modules:
                if module['name'] == '教学视频':
                    video_module = module
                    break
            
            if video_module:
                spider.log(f"📂 开始处理模块: {video_module['name']}")
                
                # 处理主页面
                spider.process_main_page(video_module)
                time.sleep(2)
                
                # 处理各个分类
                for category in video_module['categories']:
                    spider.process_category_page(video_module['name'], category)
                    time.sleep(3)  # 分类间延迟
                
                spider.log("✅ 教学视频模块处理完成")
            else:
                spider.log("❌ 未找到教学视频模块配置")
                
        except Exception as e:
            spider.log(f"❌ 视频爬取失败: {str(e)}")
        finally:
            if spider.driver:
                spider.driver.quit()
                spider.log("🔒 WebDriver已关闭")
    elif len(sys.argv) > 1 and sys.argv[1] == "videos_force":
        # 强制重新爬取视频模块模式
        spider = JTEKTSpider()
        try:
            spider.log("🚀 开始强制重新爬取光洋（捷太格特）教学视频")
            
            # 清除视频模块的进度记录
            if spider.clear_video_module_progress():
                spider.log("🔄 进度记录已清除，开始重新爬取")
                
                # 只处理教学视频模块
                video_module = None
                for module in spider.main_modules:
                    if module['name'] == '教学视频':
                        video_module = module
                        break
                
                if video_module:
                    spider.log(f"📂 开始处理模块: {video_module['name']}")
                    
                    # 处理主页面
                    spider.process_main_page(video_module)
                    time.sleep(2)
                    
                    # 处理各个分类
                    for category in video_module['categories']:
                        spider.process_category_page(video_module['name'], category)
                        time.sleep(3)  # 分类间延迟
                    
                    spider.log("✅ 教学视频模块重新爬取完成")
                else:
                    spider.log("❌ 未找到教学视频模块配置")
            else:
                spider.log("❌ 清除进度记录失败")
                
        except Exception as e:
            spider.log(f"❌ 强制视频爬取失败: {str(e)}")
        finally:
            if spider.driver:
                spider.driver.quit()
                spider.log("🔒 WebDriver已关闭")
    elif len(sys.argv) > 1 and sys.argv[1] == "nk1_pagination":
        # 专门测试NK0/NK1系列PLC技术资料的分页功能
        spider = JTEKTSpider()
        try:
            spider.log("🚀 开始测试NK0/NK1系列PLC技术资料分页功能")
            
            # 查找NK0/NK1系列PLC技术资料分类
            nk1_category = None
            for module in spider.main_modules:
                if module['name'] == '资料下载':
                    for category in module['categories']:
                        if 'NK0/NK1系列PLC技术资料' in category['name']:
                            nk1_category = category
                            break
                    if nk1_category:
                        break
            
            if nk1_category:
                spider.log(f"📂 找到分类: {nk1_category['name']}")
                spider.log(f"🔗 分类URL: {nk1_category['url']}")
                
                # 清除此分类的进度记录
                if nk1_category['url'] in spider.processed_urls:
                    spider.processed_urls.remove(nk1_category['url'])
                    spider.log("🔄 已清除分类进度记录")
                
                # 处理分类（包含分页）
                spider.process_category_with_pagination("资料下载", nk1_category)
                
                if spider.new_files:
                    spider.log(f"✅ NK1分页测试成功！共找到 {len(spider.new_files)} 个文件")
                    for file_info in spider.new_files:
                        spider.log(f"   📄 {file_info['filename']} ({file_info['size']} bytes)")
                else:
                    spider.log("⚠️ NK1分页测试完成，但未找到新文件")
            else:
                spider.log("❌ 未找到NK0/NK1系列PLC技术资料分类")
                
        except Exception as e:
            spider.log(f"❌ NK1分页测试失败: {str(e)}")
        finally:
            if spider.driver:
                spider.driver.quit()
                spider.log("�� WebDriver已关闭")
    elif len(sys.argv) > 1 and sys.argv[1] == "encoder_pagination":
        # 专门测试编码器相关技术资料的分页功能
        spider = JTEKTSpider()
        try:
            spider.log("🚀 开始测试编码器相关技术资料分页功能")
            
            # 查找编码器相关技术资料分类
            encoder_category = None
            for module in spider.main_modules:
                if module['name'] == '资料下载':
                    for category in module['categories']:
                        if '编码器相关技术资料' in category['name']:
                            encoder_category = category
                            break
                    if encoder_category:
                        break
            
            if encoder_category:
                spider.log(f"📂 找到分类: {encoder_category['name']}")
                spider.log(f"🔗 分类URL: {encoder_category['url']}")
                
                # 清除此分类的进度记录
                if encoder_category['url'] in spider.processed_urls:
                    spider.processed_urls.remove(encoder_category['url'])
                    spider.log("🔄 已清除分类进度记录")
                
                # 处理分类（包含分页）
                spider.process_category_with_pagination("资料下载", encoder_category)
                
                if spider.new_files:
                    spider.log(f"✅ 编码器分页测试成功！共找到 {len(spider.new_files)} 个文件")
                    for file_info in spider.new_files:
                        spider.log(f"   📄 {file_info['filename']} ({file_info['size']} bytes)")
                else:
                    spider.log("⚠️ 编码器分页测试完成，但未找到新文件")
            else:
                spider.log("❌ 未找到编码器相关技术资料分类")
                
        except Exception as e:
            spider.log(f"❌ 编码器分页测试失败: {str(e)}")
        finally:
            if spider.driver:
                spider.driver.quit()
                spider.log("🔒 WebDriver已关闭")
    else:
        # 正常运行
        spider = JTEKTSpider()
        spider.run()
