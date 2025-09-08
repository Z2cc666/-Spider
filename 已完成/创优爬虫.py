#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
创优产品中心爬虫
爬取产品中心下面的所有产品信息
支持钉钉通知和自动检测新文件
"""

import requests
from bs4 import BeautifulSoup
import json
import time
import os
import pickle
import hmac
import hashlib
import base64
import argparse
from datetime import datetime
from urllib.parse import urljoin, urlparse, quote_plus
import re
import urllib.request
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

class ChuangyouSpider:
    def __init__(self, limit=None, categories=None, no_webdriver=False, skip_download=False):
        self.base_url = "https://www.cuhnj.com"
        self.main_url = "https://www.cuhnj.com/href/html/prodXl"
        self.limit = limit
        self.categories = categories
        self.no_webdriver = no_webdriver
        self.skip_download = skip_download
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        
        # 服务器固定路径（按规范要求），本地测试使用当前目录
        if os.path.exists("/srv/downloads/approved/"):
            self.base_dir = "/srv/downloads/approved/创优"
            self.output_dir = os.path.join(self.base_dir, "产品数据")
            self.download_dir = os.path.join(self.base_dir, "产品资料下载")
        else:
            # 本地测试环境
            self.base_dir = os.path.join(os.getcwd(), "创优")
            self.output_dir = os.path.join(self.base_dir, "产品数据")
            self.download_dir = os.path.join(self.base_dir, "产品资料下载")
        
        # 确保目录存在
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(self.download_dir, exist_ok=True)
        
        # 钉钉配置（内置）
        self.dingtalk_config = {
            "access_token": "1a431b6482e59ec652a6eab37705d50fd282dc426f0b829b3eb9a9c1b277ed24",
            "secret": "SECc5e2168011dd97d4c511e06832d172f31635ef635771668e4e6003fce23c07bb",
            "webhook_url": "https://oapi.dingtalk.com/robot/send?access_token=1a431b6482e59ec652a6eab37705d50fd282dc426f0b829b3eb9a9c1b277ed24"
        }
        
        # 加载已处理的文件记录
        self.processed_files = self.load_processed_files()
        self.new_files = []
        
        # 判断是否为首次运行
        self.is_first_run = not os.path.exists(os.path.join(self.base_dir, 'processed_files.pkl'))
        
        # 初始化Chrome WebDriver
        self.driver = None
        self.init_webdriver()
    
    def init_webdriver(self):
        """初始化Chrome WebDriver"""
        try:
            chrome_options = Options()
            chrome_options.add_argument('--headless')  # 无头模式
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--window-size=1920,1080')
            chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')
            
            # 检查可用的ChromeDriver路径
            chromedriver_paths = [
                '/Users/z2cc/伯朗特/chromedriver_downloads/chromedriver_mac-arm64/chromedriver-mac-arm64/chromedriver',
                '/Users/z2cc/伯朗特/chromedriver_downloads/chromedriver_mac-x64/chromedriver-mac-x64/chromedriver',
                'chromedriver'  # 系统PATH中的chromedriver
            ]
            
            driver_path = None
            for path in chromedriver_paths:
                if os.path.exists(path):
                    driver_path = path
                    break
            
            if driver_path:
                service = Service(driver_path)
                self.driver = webdriver.Chrome(service=service, options=chrome_options)
                self.log("✅ Chrome WebDriver 初始化成功")
            else:
                # 尝试使用系统PATH中的chromedriver
                try:
                    self.driver = webdriver.Chrome(options=chrome_options)
                    self.log("✅ Chrome WebDriver 初始化成功（使用系统PATH）")
                except Exception:
                    self.log("⚠️ 未找到ChromeDriver，将使用requests模式")
                
        except Exception as e:
            self.log(f"⚠️ Chrome WebDriver 初始化失败: {e}，将使用requests模式")
            self.driver = None
    
    def close_webdriver(self):
        """关闭WebDriver"""
        if self.driver:
            try:
                self.driver.quit()
                self.log("✅ WebDriver 已关闭")
            except Exception as e:
                self.log(f"⚠️ 关闭WebDriver时出错: {e}")
    
    def load_processed_files(self):
        """加载已处理的文件记录"""
        processed_file = os.path.join(self.base_dir, 'processed_files.pkl')
        if os.path.exists(processed_file):
            try:
                with open(processed_file, 'rb') as f:
                    return pickle.load(f)
            except:
                return set()
        return set()
    
    def save_processed_files(self):
        """保存已处理的文件记录"""
        processed_file = os.path.join(self.base_dir, 'processed_files.pkl')
        with open(processed_file, 'wb') as f:
            pickle.dump(self.processed_files, f)
    
    def log(self, message):
        """日志记录"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{timestamp}] {message}")
    
    def send_dingtalk_notification(self, message):
        """发送钉钉通知（支持加密签名）"""
        if not self.dingtalk_config or not self.dingtalk_config.get('webhook_url'):
            self.log("⚠️ 钉钉配置未设置，跳过通知")
            return
        
        try:
            # 获取配置
            access_token = self.dingtalk_config.get('access_token', '')
            secret = self.dingtalk_config.get('secret', '')
            webhook_url = self.dingtalk_config.get('webhook_url', '')
            
            # 如果有secret，生成签名
            if secret:
                timestamp = str(round(time.time() * 1000))
                string_to_sign = f'{timestamp}\n{secret}'
                hmac_code = hmac.new(
                    secret.encode('utf-8'),
                    string_to_sign.encode('utf-8'),
                    digestmod=hashlib.sha256
                ).digest()
                sign = quote_plus(base64.b64encode(hmac_code))
                webhook_url = f"{webhook_url}&timestamp={timestamp}&sign={sign}"
            
            # 构建消息
            data = {
                "msgtype": "text",
                "text": {
                    "content": f"🤖 创优爬虫通知\n{message}"
                }
            }
            
            # 发送通知
            response = requests.post(
                webhook_url,
                json=data,
                headers={'Content-Type': 'application/json'},
                timeout=10
            )
            
            if response.status_code == 200:
                result = response.json()
                if result.get('errcode') == 0:
                    self.log("✅ 钉钉通知发送成功")
                else:
                    self.log(f"❌ 钉钉通知发送失败: {result.get('errmsg', '未知错误')}")
            else:
                self.log(f"❌ 钉钉通知HTTP错误: {response.status_code}")
                
        except Exception as e:
            self.log(f"❌ 钉钉通知发送异常: {str(e)}")
    
    def get_page(self, url, max_retries=3):
        """获取页面内容，带重试机制"""
        for attempt in range(max_retries):
            try:
                response = self.session.get(url, timeout=15)
                response.raise_for_status()
                response.encoding = 'utf-8'
                return response.text
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 502 and attempt < max_retries - 1:
                    print(f"获取页面失败 {url}: {e} (尝试 {attempt + 1}/{max_retries})")
                    time.sleep(5 * (attempt + 1))  # 递增延迟
                    continue
                else:
                    print(f"获取页面失败 {url}: {e}")
                    return None
            except Exception as e:
                if attempt < max_retries - 1:
                    print(f"获取页面失败 {url}: {e} (尝试 {attempt + 1}/{max_retries})")
                    time.sleep(3 * (attempt + 1))
                    continue
                else:
                    print(f"获取页面失败 {url}: {e}")
                    return None
        return None
    
    def parse_main_page(self):
        """解析主页面，获取所有产品模块链接"""
        print("正在解析主页面...")
        html = self.get_page(self.main_url)
        if not html:
            return []
        
        soup = BeautifulSoup(html, 'html.parser')
        modules = []
        
        # 查找产品中心模块
        # 根据提供的HTML结构，查找产品中心导航项
        nav_items = soup.find_all('li', class_='nav-item clickLi')
        product_center_nav = None
        
        for nav_item in nav_items:
            nav_link = nav_item.find('a')
            if nav_link and '产品中心' in nav_link.get_text(strip=True):
                product_center_nav = nav_item
                break
        
        if product_center_nav:
            print("找到产品中心导航项")
            # 在 submenu 下查找所有产品分类
            submenu = product_center_nav.find('div', class_='submenu submenu2')
            if submenu:
                # 获取所有产品分类（gzy-herd-li）
                category_lis = submenu.find_all('li', class_='gzy-herd-li')
                print(f"找到 {len(category_lis)} 个产品分类")
                
                for i, category_li in enumerate(category_lis, 1):
                    print(f"处理第 {i} 个产品分类...")
                    
                    # 获取分类图片和链接
                    prod_slt = category_li.find('div', class_='prod-slt')
                    category_url = ""
                    category_img_url = ""
                    category_name = f"分类{i}"
                    
                    if prod_slt:
                        category_link = prod_slt.find('a')
                        if category_link:
                            category_url = category_link.get('href', '')
                            if category_url and not category_url.startswith('http'):
                                category_url = urljoin(self.base_url, category_url)
                            
                            # 获取分类图片
                            category_img = category_link.find('img')
                            if category_img:
                                category_img_url = category_img.get('src', '')
                                if category_img_url and not category_img_url.startswith('http'):
                                    category_img_url = urljoin(self.base_url, category_img_url)
                    
                    # 获取该分类下的所有产品
                    xh_ul = category_li.find('div', class_='xh-ul left-div')
                    if xh_ul:
                        product_links = xh_ul.find_all('a', class_='gzy_product_top')
                        print(f"  分类{i}下找到 {len(product_links)} 个产品")
                        
                        for product_link in product_links:
                            product_name = product_link.get_text(strip=True)
                            product_url = product_link.get('href', '')
                            
                            if product_url and not product_url.startswith('http'):
                                product_url = urljoin(self.base_url, product_url)
                            
                            if product_name and product_url:
                                modules.append({
                                    'name': product_name,
                                    'url': product_url,
                                    'type': '产品',
                                    'category': category_name,
                                    'category_url': category_url,
                                    'category_image': category_img_url
                                })
                                print(f"  找到产品: {product_name}")
            else:
                print("未找到产品中心的submenu结构")
        else:
            print("未找到产品中心导航项")
        
        return modules
    
    def parse_product_page(self, product_info):
        """解析产品详情页，提取所有内容，每解析一个模块就立即下载"""
        print(f"    正在解析产品: {product_info['name']}")
        
        product_data = {
            'basic_info': {},
            'content_sections': {},
            'download_links': [],
            'downloaded_files': []
        }
        
        # 如果有WebDriver，使用Selenium获取动态内容
        if self.driver:
            try:
                self.driver.get(product_info['url'])
                # 等待页面加载完成
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "shop-went"))
                )
                
                # 等待Vue渲染完成
                time.sleep(3)
                
                # 获取渲染后的HTML
                html = self.driver.page_source
                soup = BeautifulSoup(html, 'html.parser')
                
            except Exception as e:
                print(f"    WebDriver获取页面失败: {e}，尝试使用requests")
                html = self.get_page(product_info['url'])
                if not html:
                    return {}
                soup = BeautifulSoup(html, 'html.parser')
        else:
            # 使用requests获取静态HTML
            html = self.get_page(product_info['url'])
            if not html:
                return {}
            soup = BeautifulSoup(html, 'html.parser')
        
        # 1. 提取基本信息
        shop_went = soup.find('div', class_='shop-went')
        if shop_went:
            # 产品标题
            h2_elem = shop_went.find('h2')
            if h2_elem:
                spans = h2_elem.find_all('span')
                if len(spans) >= 2:
                    product_data['basic_info']['model'] = spans[0].get_text(strip=True)
                    product_data['basic_info']['title'] = spans[1].get_text(strip=True)
                elif len(spans) == 1:
                    # 如果只有一个span，可能是完整的标题
                    product_data['basic_info']['title'] = spans[0].get_text(strip=True)
            
            # 所属分类 - 查找包含"所属分类："的元素
            category_b = shop_went.find('b', string=lambda text: text and '所属分类：' in text if text else False)
            if not category_b:
                # 尝试查找文本节点
                category_elements = shop_went.find_all(string=lambda text: text and '所属分类：' in text)
                if category_elements:
                    # 找到包含"所属分类："的文本后，查找其后的span元素
                    for elem in category_elements:
                        parent = elem.parent
                        if parent:
                            category_span = parent.find('span')
                            if category_span:
                                product_data['basic_info']['category'] = category_span.get_text(strip=True)
                                break
            else:
                # 直接从b标签后找span
                category_span = category_b.find('span')
                if category_span:
                    product_data['basic_info']['category'] = category_span.get_text(strip=True)
            
            # 概要信息 - 查找包含"概要信息："的元素
            summary_p = shop_went.find('p', class_='p-gy')
            if summary_p:
                # 查找p标签内的span元素
                summary_span = summary_p.find('span')
                if summary_span:
                    product_data['basic_info']['summary'] = summary_span.get_text(strip=True)
            else:
                # 备用方法：查找文本节点
                summary_elements = shop_went.find_all(string=lambda text: text and '概要信息：' in text)
                if summary_elements:
                    summary_text = summary_elements[0]
                    product_data['basic_info']['summary'] = summary_text.replace('概要信息：', '').strip()
        
        # 2. 逐个解析标签页内容，每解析一个就下载
        tabs = {
            'tab1': '产品描述',
            'tab2': '技术规格', 
            'tab3': '产品资料',
            'tab4': '视频',
            'tab5': '应用',
            'tab6': '报错信息'
        }
        
        for tab_id, tab_name in tabs.items():
            print(f"      解析模块: {tab_name}")
            
            # 应用模块特殊处理
            if tab_id == 'tab5' and tab_name == '应用':
                content = self.extract_application_content_simple(soup, product_info, product_data)
                if content:
                    product_data['content_sections'][tab_name] = content
                    print(f"      模块 {tab_name} 解析完成")
                else:
                    print(f"      模块 {tab_name} 无内容")
            else:
                tab_div = soup.find('div', id=tab_id)
                if tab_div:
                    # 提取内容并立即下载
                    content = self.extract_tab_content_with_download(tab_div, tab_name, product_info, product_data)
                    if content:
                        product_data['content_sections'][tab_name] = content
                        print(f"      模块 {tab_name} 解析完成")
                    else:
                        print(f"      模块 {tab_name} 无内容")
                else:
                    print(f"      模块 {tab_name} 未找到")
        
        return product_data
    
    def extract_application_content_simple(self, soup, product_info, product_data):
        """简单方式提取应用模块内容，不使用Selenium"""
        try:
            # 查找应用模块的div
            tab5_div = soup.find('div', id='tab5')
            if not tab5_div:
                print("        未找到应用模块div")
                return None
            
            content = {
                'text_content': '',
                'images': [],
                'videos': [],
                'download_links': []
            }
            
            # 查找shop-nt shop-ul结构
            shop_div = tab5_div.find('div', class_=['shop-nt', 'shop-ul'])
            if shop_div:
                ul_tag = shop_div.find('ul')
                if ul_tag:
                    # 遍历所有li标签
                    for li in ul_tag.find_all('li'):
                        link = li.find('a', href=True)
                        if link:
                            href = link.get('href', '')
                            link_text = link.get_text(strip=True)
                            
                            if href and link_text and href != 'javascript:;' and not href.startswith('#'):
                                # 转换为绝对URL
                                if not href.startswith('http'):
                                    href = urljoin(self.base_url, href)
                                
                                # 判断文件类型
                                is_image = any(ext in href.lower() for ext in ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp'])
                                is_video = any(ext in href.lower() for ext in ['.mp4', '.avi', '.mov', '.wmv', '.mkv', '.flv']) or '视频' in link_text
                                is_download = any(ext in href.lower() for ext in ['.pdf', '.doc', '.docx', '.zip', '.rar', '.xls', '.xlsx']) or any(keyword in link_text for keyword in ['说明书', '手册', '资料', '规格'])
                                
                                if is_image:
                                    # 处理图片链接
                                    img_info = {
                                        'url': href,
                                        'alt': link_text
                                    }
                                    content['images'].append(img_info)
                                    print(f"        找到应用图片: {link_text}")
                                    self.download_image(href, link_text, "应用", product_info, product_data)
                                
                                elif is_video:
                                    # 处理视频链接
                                    content['videos'].append(href)
                                    print(f"        找到应用视频: {link_text}")
                                    self.download_video(href, link_text, "应用", product_info, product_data)
                                
                                elif is_download:
                                    # 处理下载文件
                                    download_info = {
                                        'url': href,
                                        'title': link_text,
                                        'product_name': product_info['name'],
                                        'type': "应用",
                                        'category': product_data.get('basic_info', {}).get('category', '未分类')
                                    }
                                    content['download_links'].append(download_info)
                                    print(f"        找到应用资料: {link_text}")
                                    self.download_file(download_info)
            
            # 提取文本内容
            text_content = tab5_div.get_text(strip=True)
            content['text_content'] = text_content
            
            return content if any([content['images'], content['videos'], content['download_links'], content['text_content']]) else None
            
        except Exception as e:
            print(f"        应用模块解析失败: {str(e)}")
            return None

    def extract_application_content_selenium(self, product_info, product_data):
        """使用Selenium提取应用模块的动态内容"""
        try:
            from selenium import webdriver
            from selenium.webdriver.chrome.options import Options
            from selenium.webdriver.common.by import By
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC
            from bs4 import BeautifulSoup
            
            # 设置Chrome选项
            chrome_options = Options()
            chrome_options.add_argument('--headless')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-gpu')
            
            driver = webdriver.Chrome(options=chrome_options)
            
            try:
                # 访问产品页面
                product_url = f"https://www.cuhnj.com/href/html/prodXq1?product={product_info['id']}"
                driver.get(product_url)
                
                # 点击应用标签页
                app_tab = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, "//li[@data-tab='tab5' or contains(@onclick,'tab5')]"))
                )
                driver.execute_script("arguments[0].click();", app_tab)
                
                # 等待内容加载
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.ID, "tab5"))
                )
                
                # 获取渲染后的HTML
                page_html = driver.page_source
                soup = BeautifulSoup(page_html, 'html.parser')
                
                # 提取应用模块内容
                tab5_div = soup.find('div', id='tab5')
                if tab5_div:
                    content = {
                        'text_content': '',
                        'images': [],
                        'videos': [],
                        'download_links': []
                    }
                    
                    # 提取文本内容
                    text_content = tab5_div.get_text(strip=True)
                    content['text_content'] = text_content
                    
                    # 查找所有链接
                    all_links = tab5_div.find_all('a', href=True)
                    for link in all_links:
                        href = link.get('href', '')
                        link_text = link.get_text(strip=True)
                        
                        if href and link_text and href != 'javascript:;' and not href.startswith('#'):
                            # 转换为绝对URL
                            if not href.startswith('http'):
                                href = urljoin(self.base_url, href)
                            
                            # 判断文件类型
                            is_image = any(ext in href.lower() for ext in ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp'])
                            is_video = any(ext in href.lower() for ext in ['.mp4', '.avi', '.mov', '.wmv', '.mkv', '.flv']) or '视频' in link_text
                            is_download = any(ext in href.lower() for ext in ['.pdf', '.doc', '.docx', '.zip', '.rar', '.xls', '.xlsx']) or any(keyword in link_text for keyword in ['说明书', '手册', '资料', '规格'])
                            
                            if is_image:
                                # 处理图片链接
                                img_info = {
                                    'url': href,
                                    'alt': link_text
                                }
                                content['images'].append(img_info)
                                print(f"        找到应用图片: {link_text}")
                                self.download_image(href, link_text, "应用", product_info, product_data)
                            
                            elif is_video:
                                # 处理视频链接
                                content['videos'].append(href)
                                print(f"        找到应用视频: {link_text}")
                                self.download_video(href, link_text, "应用", product_info, product_data)
                            
                            elif is_download:
                                # 处理下载文件
                                download_info = {
                                    'url': href,
                                    'title': link_text,
                                    'product_name': product_info['name'],
                                    'type': "应用",
                                    'category': product_data.get('basic_info', {}).get('category', '未分类')
                                }
                                content['download_links'].append(download_info)
                                print(f"        找到应用资料: {link_text}")
                                self.download_file(download_info)
                    
                    return content
                
            finally:
                driver.quit()
                
        except Exception as e:
            print(f"        应用模块Selenium解析失败: {str(e)}")
            return None
    
    def parse_product_materials_selenium(self, soup, product_data, product_info):
        """使用Selenium解析产品资料标签页"""
        download_links = []
        
        # 查找产品资料下载链接
        tab3_div = soup.find('div', id='tab3')
        if tab3_div:
            print(f"      找到产品资料区域")
            
            # 查找所有的下载链接
            all_links = tab3_div.find_all('a', href=True)
            
            for link in all_links:
                href = link.get('href', '')
                link_text = link.get_text(strip=True)
                
                # 过滤有效的下载链接
                if href and link_text and href != 'javascript:;' and not href.startswith('#'):
                    # 转换为绝对URL
                    if not href.startswith('http'):
                        href = urljoin(self.base_url, href)
                    
                    # 判断是否是下载文件（PDF、DOC等）
                    is_download = (
                        href.lower().endswith('.pdf') or 
                        href.lower().endswith('.doc') or 
                        href.lower().endswith('.docx') or
                        href.lower().endswith('.zip') or
                        href.lower().endswith('.rar') or
                        '说明书' in link_text or
                        '手册' in link_text or
                        '资料' in link_text or
                        'manual' in link_text.lower() or
                        'specification' in link_text.lower()
                    )
                    
                    if is_download:
                        download_links.append({
                            'url': href,
                            'title': link_text,
                            'product_name': product_info['name'],
                            'type': '产品资料',
                            'category': product_data.get('basic_info', {}).get('category', '未分类')
                        })
                        print(f"      找到资料: {link_text} - {href}")
        
        product_data['download_links'] = download_links
    
    def parse_product_materials(self, tab_div, product_data, product_info):
        """解析产品资料标签页"""
        download_links = []
        
        # 查找产品资料下载链接
        shop_ul = tab_div.find('div', class_='shop-nt shop-ul')
        if shop_ul:
            ul_elem = shop_ul.find('ul')
            if ul_elem:
                li_elements = ul_elem.find_all('li')
                
                for li in li_elements:
                    link = li.find('a')
                    if link:
                        download_url = link.get('href', '')
                        if download_url and not download_url.startswith('http'):
                            download_url = urljoin(self.base_url, download_url)
                        
                        material_name = link.get_text(strip=True)
                        
                        if download_url and material_name:
                            download_links.append({
                                'url': download_url,
                                'title': material_name,
                                'product_name': product_info['name'],
                                'type': '产品资料',
                                'category': product_data.get('basic_info', {}).get('category', '未分类')
                            })
                            print(f"      找到资料: {material_name} - {download_url}")
        
        # 查找说明书历史版本
        history_div = tab_div.find('div', class_='intructHistory')
        if history_div:
            history_ul = history_div.find('ul', class_='listContAll')
            if history_ul:
                history_links = history_ul.find_all('a')
                for link in history_links:
                    download_url = link.get('href', '')
                    if download_url and not download_url.startswith('http'):
                        download_url = urljoin(self.base_url, download_url)
                    
                    material_name = link.get_text(strip=True)
                    
                    if download_url and material_name:
                        download_links.append({
                            'url': download_url,
                            'title': f"历史版本_{material_name}",
                            'product_name': product_info['name'],
                            'type': '历史版本',
                            'category': product_data.get('basic_info', {}).get('category', '未分类')
                        })
                        print(f"      找到历史版本: {material_name} - {download_url}")
        
        product_data['download_links'] = download_links
    
    def extract_tab_content_with_download(self, tab_div, tab_name, product_info, product_data):
        """提取标签页内容并立即下载相关文件"""
        content = {
            'text_content': '',
            'images': [],
            'videos': [],
            'download_links': []
        }
        
        # 提取文本内容
        shop_nt = tab_div.find('div', class_='shop-nt')
        if shop_nt:
            # 提取纯文本
            text_content = shop_nt.get_text(strip=True)
            content['text_content'] = text_content
            
            # 提取图片并立即下载 - 在整个tab_div中查找，不仅仅是shop_nt
            images = tab_div.find_all('img')
            for img in images:
                img_src = img.get('src', '')
                if img_src and not img_src.startswith('http'):
                    img_src = urljoin(self.base_url, img_src)
                
                img_alt = img.get('alt', '') or f"{tab_name}_图片_{len(content['images'])}"
                if img_src:
                    img_info = {
                        'url': img_src,
                        'alt': img_alt
                    }
                    content['images'].append(img_info)
                    
                    # 立即下载图片
                    self.download_image(img_src, img_alt, tab_name, product_info, product_data)
            
            # 提取视频并记录
            videos = shop_nt.find_all(['video', 'iframe'])
            for video in videos:
                video_src = video.get('src', '') or video.get('data-src', '')
                if video_src and not video_src.startswith('http'):
                    video_src = urljoin(self.base_url, video_src)
                
                if video_src:
                    content['videos'].append(video_src)
                    # 如果是视频文件，也尝试下载
                    if any(ext in video_src.lower() for ext in ['.mp4', '.avi', '.mov', '.wmv']):
                        video_name = f"{tab_name}_视频_{len(content['videos'])}"
                        self.download_video(video_src, video_name, tab_name, product_info, product_data)
            
            # 查找下载链接 - 在整个tab_div中查找，不仅仅是shop_nt
            all_links = tab_div.find_all('a', href=True)
            for link in all_links:
                href = link.get('href', '')
                link_text = link.get_text(strip=True)
                
                # 过滤有效的下载链接
                if href and link_text and href != 'javascript:;' and not href.startswith('#'):
                    # 转换为绝对URL
                    if not href.startswith('http'):
                        href = urljoin(self.base_url, href)
                    
                    # 判断是否是图片文件
                    is_image = (
                        href.lower().endswith('.jpg') or 
                        href.lower().endswith('.jpeg') or 
                        href.lower().endswith('.png') or 
                        href.lower().endswith('.gif') or
                        href.lower().endswith('.bmp') or
                        href.lower().endswith('.webp')
                    )
                    
                    # 判断是否是视频文件
                    is_video = (
                        href.lower().endswith('.mp4') or 
                        href.lower().endswith('.avi') or 
                        href.lower().endswith('.mov') or 
                        href.lower().endswith('.wmv') or
                        href.lower().endswith('.mkv') or
                        href.lower().endswith('.flv') or
                        '视频' in link_text
                    )
                    
                    # 判断是否是下载文件
                    is_download = (
                        href.lower().endswith('.pdf') or 
                        href.lower().endswith('.doc') or 
                        href.lower().endswith('.docx') or
                        href.lower().endswith('.zip') or
                        href.lower().endswith('.rar') or
                        href.lower().endswith('.xls') or
                        href.lower().endswith('.xlsx') or
                        '说明书' in link_text or
                        '手册' in link_text or
                        '资料' in link_text or
                        '规格' in link_text or
                        'manual' in link_text.lower() or
                        'specification' in link_text.lower()
                    )
                    
                    if is_image:
                        # 处理图片链接
                        img_info = {
                            'url': href,
                            'alt': link_text
                        }
                        content['images'].append(img_info)
                        print(f"        找到{tab_name}图片: {link_text}")
                        self.download_image(href, link_text, tab_name, product_info, product_data)
                    
                    elif is_video:
                        # 处理视频链接
                        content['videos'].append(href)
                        video_name = link_text or f"{tab_name}_视频_{len(content['videos'])}"
                        print(f"        找到{tab_name}视频: {link_text}")
                        self.download_video(href, video_name, tab_name, product_info, product_data)
                    
                    elif is_download:
                        download_info = {
                            'url': href,
                            'title': link_text,
                            'product_name': product_info['name'],
                            'type': tab_name,
                            'category': product_data.get('basic_info', {}).get('category', '未分类')
                        }
                        content['download_links'].append(download_info)
                        product_data['download_links'].append(download_info)
                        
                        # 立即下载文件
                        print(f"        找到{tab_name}资料: {link_text}")
                        if self.download_file(download_info):
                            product_data['downloaded_files'].append(download_info)
        
        return content
    
    def extract_tab_content(self, tab_div, tab_name):
        """提取标签页内容（旧版本，保留兼容性）"""
        content = {
            'text_content': '',
            'images': [],
            'videos': []
        }
        
        # 提取文本内容
        shop_nt = tab_div.find('div', class_='shop-nt')
        if shop_nt:
            # 提取纯文本
            text_content = shop_nt.get_text(strip=True)
            content['text_content'] = text_content
            
            # 提取图片
            images = shop_nt.find_all('img')
            for img in images:
                img_src = img.get('src', '')
                if img_src and not img_src.startswith('http'):
                    img_src = urljoin(self.base_url, img_src)
                
                img_alt = img.get('alt', '')
                if img_src:
                    content['images'].append({
                        'url': img_src,
                        'alt': img_alt
                    })
            
            # 提取视频（如果有）
            videos = shop_nt.find_all(['video', 'iframe'])
            for video in videos:
                video_src = video.get('src', '') or video.get('data-src', '')
                if video_src and not video_src.startswith('http'):
                    video_src = urljoin(self.base_url, video_src)
                
                if video_src:
                    content['videos'].append(video_src)
        
        return content
    
    def download_image(self, img_url, img_alt, tab_name, product_info, product_data):
        """下载图片"""
        # 如果启用了跳过下载模式，只返回成功信息但不实际下载
        if self.skip_download:
            print(f"        [跳过下载图片] {img_alt}")
            return True
            
        try:
            # 清理文件名
            img_alt = re.sub(r'[<>:"/\\|?*]', '_', img_alt)
            if not img_alt or not img_alt.strip():
                img_alt = f"{tab_name}_图片"
            
            # 获取文件扩展名
            parsed_url = urlparse(img_url)
            path = parsed_url.path
            if '.' in path:
                ext = path.split('.')[-1].lower()
                if ext not in ['jpg', 'jpeg', 'png', 'gif', 'bmp', 'webp']:
                    ext = 'jpg'  # 默认扩展名
            else:
                ext = 'jpg'
            
            filename = f"{img_alt}.{ext}"
            
            # 创建目录结构 - 按分类组织
            category = "未分类"
            if product_data and 'basic_info' in product_data and 'category' in product_data['basic_info']:
                category = product_data['basic_info']['category']
            
            # 清理分类名称和产品名称，避免文件系统不支持的字符
            category = re.sub(r'[<>:"/\\|?*]', '_', category)
            product_name = re.sub(r'[<>:"/\\|?*]', '_', product_info['name'])
            
            category_dir = os.path.join(self.download_dir, category)
            product_dir = os.path.join(category_dir, product_name)
            type_dir = os.path.join(product_dir, tab_name)
            os.makedirs(type_dir, exist_ok=True)
            
            filepath = os.path.join(type_dir, filename)
            
            # 如果文件已存在，跳过下载
            if os.path.exists(filepath):
                return True
            
            print(f"        正在下载图片: {filename}")
            
            # 下载图片
            response = self.session.get(img_url, stream=True, timeout=30)
            response.raise_for_status()
            
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            file_size = os.path.getsize(filepath)
            print(f"        图片下载完成: {filename} ({file_size} bytes)")
            
            # 记录下载的图片
            image_info = {
                'url': img_url,
                'title': img_alt,
                'filename': filename,
                'path': filepath,
                'size': file_size,
                'product_name': product_info['name'],
                'type': f"{tab_name}_图片"
            }
            product_data['downloaded_files'].append(image_info)
            
            return True
            
        except Exception as e:
            print(f"        图片下载失败 {img_url}: {e}")
            return False
    
    def download_video(self, video_url, video_name, tab_name, product_info, product_data):
        """下载视频"""
        try:
            # 清理文件名
            video_name = re.sub(r'[<>:"/\\|?*]', '_', video_name)
            
            # 获取文件扩展名
            parsed_url = urlparse(video_url)
            path = parsed_url.path
            if '.' in path:
                ext = path.split('.')[-1].lower()
                if ext not in ['mp4', 'avi', 'mov', 'wmv', 'flv', 'mkv']:
                    ext = 'mp4'  # 默认扩展名
            else:
                ext = 'mp4'
            
            filename = f"{video_name}.{ext}"
            
            # 创建目录结构 - 按分类组织
            category = "未分类"
            if product_data and 'basic_info' in product_data and 'category' in product_data['basic_info']:
                category = product_data['basic_info']['category']
            
            # 清理分类名称和产品名称，避免文件系统不支持的字符
            category = re.sub(r'[<>:"/\\|?*]', '_', category)
            product_name = re.sub(r'[<>:"/\\|?*]', '_', product_info['name'])
            
            category_dir = os.path.join(self.download_dir, category)
            product_dir = os.path.join(category_dir, product_name)
            type_dir = os.path.join(product_dir, tab_name)
            os.makedirs(type_dir, exist_ok=True)
            
            filepath = os.path.join(type_dir, filename)
            
            # 如果文件已存在，跳过下载
            if os.path.exists(filepath):
                return True
            
            print(f"        正在下载视频: {filename}")
            
            # 下载视频
            response = self.session.get(video_url, stream=True, timeout=60)
            response.raise_for_status()
            
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            file_size = os.path.getsize(filepath)
            print(f"        视频下载完成: {filename} ({file_size} bytes)")
            
            # 记录下载的视频
            video_info = {
                'url': video_url,
                'title': video_name,
                'filename': filename,
                'path': filepath,
                'size': file_size,
                'product_name': product_info['name'],
                'type': f"{tab_name}_视频"
            }
            product_data['downloaded_files'].append(video_info)
            
            return True
            
        except Exception as e:
            print(f"        视频下载失败 {video_url}: {e}")
            return False
    
    def download_file(self, download_info):
        """下载文件"""
        # 如果启用了跳过下载模式，只返回成功信息但不实际下载
        if self.skip_download:
            print(f"        [跳过下载] {download_info['title']}")
            return True
            
        try:
            url = download_info['url']
            filename = download_info['title']
            
            # 清理文件名，移除非法字符
            filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
            
            # 如果文件名为空或只有空格，设置默认名称
            if not filename or not filename.strip():
                filename = "产品资料"
            
            # 确保文件有扩展名
            if '.' not in filename.split('/')[-1]:
                # 尝试从URL获取扩展名
                parsed_url = urlparse(url)
                path = parsed_url.path
                if '.' in path:
                    ext = path.split('.')[-1].lower()
                    if ext in ['pdf', 'doc', 'docx', 'xls', 'xlsx', 'zip', 'rar', 'jpg', 'png']:
                        filename += f'.{ext}'
                else:
                    # 默认为PDF
                    filename += '.pdf'
            
            # 创建目录结构 - 按分类组织
            category = download_info.get('category', '未分类')
            product_name = re.sub(r'[<>:"/\\|?*]', '_', download_info['product_name'])
            material_type = download_info.get('type', '其他资料')
            
            # 清理分类名称，避免文件系统不支持的字符
            category = re.sub(r'[<>:"/\\|?*]', '_', category)
            
            # 创建分类目录
            category_dir = os.path.join(self.download_dir, category)
            if not os.path.exists(category_dir):
                os.makedirs(category_dir)
            
            # 创建产品目录
            product_dir = os.path.join(category_dir, product_name)
            if not os.path.exists(product_dir):
                os.makedirs(product_dir)
            
            # 创建资料类型目录
            type_dir = os.path.join(product_dir, material_type)
            if not os.path.exists(type_dir):
                os.makedirs(type_dir)
            
            # 完整的文件路径
            filepath = os.path.join(type_dir, filename)
            
            # 如果文件已存在，跳过下载
            if os.path.exists(filepath):
                print(f"        文件已存在，跳过: {filename}")
                return True
            
            print(f"        正在下载: {filename}")
            
            # 使用requests下载文件
            response = self.session.get(url, stream=True, timeout=30)
            response.raise_for_status()
            
            # 检查响应内容类型
            content_type = response.headers.get('content-type', '').lower()
            if 'text/html' in content_type and 'pdf' not in content_type:
                print(f"        警告: 下载的可能不是文件，而是HTML页面")
                return False
            
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            # 检查文件大小
            file_size = os.path.getsize(filepath)
            if file_size < 1024:  # 小于1KB可能是错误页面
                print(f"        警告: 文件大小异常 ({file_size} bytes)")
            
            print(f"        下载完成: {filename} ({file_size} bytes)")
            
            # 记录新文件
            file_key = f"{download_info['product_name']}_{filename}"
            if file_key not in self.processed_files:
                self.new_files.append({
                    'filename': filename,
                    'path': filepath,
                    'url': download_info['url'],
                    'size': file_size,
                    'product': download_info['product_name'],
                    'type': download_info.get('type', '其他资料')
                })
                self.processed_files.add(file_key)
            
            return True
            
        except Exception as e:
            print(f"        下载失败 {url}: {e}")
            return False
    
    def save_data(self, data, filename):
        """保存数据到文件"""
        filepath = os.path.join(self.output_dir, filename)
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            print(f"数据已保存到: {filepath}")
        except Exception as e:
            print(f"保存数据失败: {e}")
    
    def run(self):
        """运行爬虫"""
        self.log("🚀 开始爬取创优产品中心...")
        
        # 1. 获取所有产品
        products = self.parse_main_page()
        if not products:
            self.log("❌ 未找到任何产品")
            return
        
        self.log(f"📋 共找到 {len(products)} 个产品")
        
        # 如果指定了限制数量，则截取产品列表
        if self.limit:
            products = products[:self.limit]
            self.log(f"🔄 限制处理数量: {len(products)} 个产品")
        
        # 2. 保存产品列表
        self.save_data(products, 'products_list.json')
        
        # 3. 爬取每个产品的详细信息（每解析一个模块就立即下载）
        all_products_data = []
        total_downloaded_files = 0
        
        for i, product in enumerate(products, 1):
            self.log(f"🔄 进度: {i}/{len(products)} - {product['name']}")
            
            # 解析产品详情页（包含实时下载）
            product_data = self.parse_product_page(product)
            
            if product_data:
                # 合并基本信息
                full_product_data = {**product, **product_data}
                all_products_data.append(full_product_data)
                
                # 统计下载的文件数量
                downloaded_count = len(product_data.get('downloaded_files', []))
                if downloaded_count > 0:
                    total_downloaded_files += downloaded_count
                    self.log(f"    ✅ 产品 {product['name']} 完成，下载了 {downloaded_count} 个文件")
            
            # 添加延迟避免请求过快
            time.sleep(2)
        
        # 4. 保存所有产品详细信息
        if all_products_data:
            self.save_data(all_products_data, 'products_detail.json')
            self.log(f"✅ 产品信息爬取完成！共获取 {len(all_products_data)} 个产品详情")
            self.log(f"📥 资料下载完成！总共下载了 {total_downloaded_files} 个文件")
        
        # 5. 保存处理记录
        self.save_processed_files()
        
        # 6. 发送钉钉通知
        self.send_completion_notification()
        
        # 7. 生成统计报告
        self.generate_report(products, all_products_data, total_downloaded_files)
        
        # 8. 关闭WebDriver
        self.close_webdriver()
    
    def send_completion_notification(self):
        """发送完成通知"""
        if not self.new_files:
            if not self.is_first_run:
                self.log("📢 无新文件，不发送通知")
            return
        
        # 构建通知消息
        message_parts = []
        message_parts.append(f"📊 创优爬虫完成")
        message_parts.append(f"🕒 时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        message_parts.append(f"📁 新下载文件: {len(self.new_files)} 个")
        
        if self.is_first_run:
            message_parts.append("🆕 首次运行，已建立基线")
        
        # 按产品分组显示新文件
        product_files = {}
        for file_info in self.new_files:
            product = file_info['product']
            if product not in product_files:
                product_files[product] = []
            product_files[product].append(file_info)
        
        message_parts.append("\n📋 新文件详情:")
        for product, files in product_files.items():
            message_parts.append(f"  📂 {product}: {len(files)} 个文件")
            for file_info in files[:3]:  # 只显示前3个
                size_mb = file_info['size'] / 1024 / 1024
                message_parts.append(f"    📄 {file_info['filename']} ({size_mb:.1f}MB)")
            if len(files) > 3:
                message_parts.append(f"    ... 还有 {len(files) - 3} 个文件")
        
        message = "\n".join(message_parts)
        self.send_dingtalk_notification(message)
    
    def generate_report(self, products, products_data, total_downloaded_files=0):
        """生成爬取报告"""
        report = {
            '爬取时间': time.strftime('%Y-%m-%d %H:%M:%S'),
            '总产品数': len(products),
            '成功解析产品数': len(products_data),
            '总下载文件数': total_downloaded_files,
            '新文件数': len(self.new_files),
            '产品列表': [p['name'] for p in products]
        }
        
        # 保存报告
        self.save_data(report, '爬取报告.json')
        
        # 打印报告
        print("\n" + "="*50)
        print("爬取报告")
        print("="*50)
        print(f"爬取时间: {report['爬取时间']}")
        print(f"总产品数: {report['总产品数']}")
        print(f"成功解析产品数: {report['成功解析产品数']}")
        print(f"总下载文件数: {report['总下载文件数']}")
        print(f"新文件数: {report['新文件数']}")
        print("="*50)

if __name__ == "__main__":
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='创优产品中心爬虫')
    parser.add_argument('--limit', type=int, help='限制处理的产品数量（用于测试）')
    parser.add_argument('--categories', nargs='+', help='指定要爬取的分类编号，如：--categories 1 2 3')
    parser.add_argument('--no-webdriver', action='store_true', help='不使用WebDriver，仅使用requests')
    parser.add_argument('--skip-download', action='store_true', help='跳过文件下载，仅提取数据用于测试')
    parser.add_argument('--test-url', type=str, help='测试指定的产品URL')
    
    args = parser.parse_args()
    
    if args.test_url:
        # 测试指定URL
        spider = ChuangyouSpider(limit=1, skip_download=False)
        try:
            # 手动创建产品信息
            import re
            product_id_match = re.search(r'product=(\d+)', args.test_url)
            if product_id_match:
                product_id = product_id_match.group(1)
                product_info = {
                    'id': product_id,
                    'name': f'Test_Product_{product_id}',
                    'url': args.test_url,
                    'category': '测试分类',
                    'type': '产品'
                }
                print(f"开始测试产品: {args.test_url}")
                spider.parse_product_page(product_info)
            else:
                print("无法解析产品ID")
        except Exception as e:
            print(f"测试过程中出现错误: {e}")
        finally:
            spider.close_webdriver()
    else:
        # 创建爬虫实例
        spider = ChuangyouSpider(
            limit=args.limit,
            categories=args.categories,
            no_webdriver=args.no_webdriver,
            skip_download=args.skip_download
        )
        
        try:
            spider.run()
        except KeyboardInterrupt:
            print("\n用户中断爬取")
            spider.close_webdriver()
        except Exception as e:
            print(f"爬取过程中出现错误: {e}")
            spider.close_webdriver()
