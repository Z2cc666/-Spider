#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
德瑞斯产品中心爬虫
爬取变频器和永磁同步电机下面的所有模块
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
from datetime import datetime
from urllib.parse import urljoin, urlparse, quote_plus
import re
import urllib.request

class DiriseSpider:
    def __init__(self):
        self.base_url = "http://www.dirise.cn"
        self.main_url = "http://www.dirise.cn/product_index.html"
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
            self.base_dir = "/srv/downloads/approved/德瑞斯"
            self.output_dir = os.path.join(self.base_dir, "产品数据")
            self.download_dir = os.path.join(self.base_dir, "说明书下载")
        else:
            # 本地测试环境
            self.base_dir = os.path.join(os.getcwd(), "德瑞斯")
            self.output_dir = os.path.join(self.base_dir, "产品数据")
            self.download_dir = os.path.join(self.base_dir, "说明书下载")
        
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
                    "content": f"🤖 德瑞斯爬虫通知\n{message}"
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
        """解析主页面，获取所有模块链接"""
        print("正在解析主页面...")
        html = self.get_page(self.main_url)
        if not html:
            return []
        
        soup = BeautifulSoup(html, 'html.parser')
        modules = []
        
        # 查找变频器和永磁同步电机模块
        nav_div = soup.find('div', class_='p102-fdh-1-nav-one')
        if nav_div:
            # 获取主标题
            title_elem = nav_div.find('h3')
            if title_elem:
                main_title = title_elem.get_text(strip=True).replace('：', '')
                print(f"找到主模块: {main_title}")
            
            # 获取所有子模块
            dd_elements = nav_div.find_all('dd')
            for dd in dd_elements:
                link = dd.find('a')
                if link:
                    module_name = link.get('title', '').strip()
                    module_url = link.get('href', '').strip()
                    if module_name and module_url:
                        full_url = urljoin(self.base_url, module_url)
                        modules.append({
                            'name': module_name,
                            'url': full_url,
                            'type': '变频器模块'
                        })
                        print(f"找到模块: {module_name} - {full_url}")
        
        return modules
    
    def parse_module_page(self, module_info):
        """解析具体模块页面"""
        print(f"\n正在解析模块: {module_info['name']}")
        html = self.get_page(module_info['url'])
        if not html:
            return []
        
        soup = BeautifulSoup(html, 'html.parser')
        products = []
        
        # 查找产品列表 - 根据<dl>结构解析
        dl_elements = soup.find_all('dl')
        
        for dl in dl_elements:
            # 查找产品链接和标题
            dt = dl.find('dt')
            dd = dl.find('dd')
            
            if dt and dd:
                # 从dt中获取产品链接和图片信息
                link_elem = dt.find('a')
                if link_elem:
                    product_url = link_elem.get('href', '')
                    product_title = link_elem.get('title', '')
                    
                    # 如果没有title属性，尝试从img的alt或title获取
                    if not product_title:
                        img = link_elem.find('img')
                        if img:
                            product_title = img.get('alt', '') or img.get('title', '')
                    
                    # 从dd中获取产品描述
                    product_desc = ""
                    desc_div = dd.find('div', class_='p102-pros-1-desc')
                    if desc_div:
                        product_desc = desc_div.get_text(strip=True)
                    
                    # 获取产品名称（从h4标签）
                    product_name = ""
                    h4_elem = dd.find('h4')
                    if h4_elem:
                        h4_link = h4_elem.find('a')
                        if h4_link:
                            product_name = h4_link.get('title', '') or h4_link.get_text(strip=True)
                    
                    # 如果还没有产品名称，使用title
                    if not product_name:
                        product_name = product_title
                    
                    # 确保URL是完整的
                    if product_url and not product_url.startswith('http'):
                        product_url = urljoin(module_info['url'], product_url)
                    
                    if product_name and product_url:
                        product_info = {
                            'name': product_name,
                            'title': product_title,
                            'url': product_url,
                            'description': product_desc,
                            'module': module_info['name']
                        }
                        products.append(product_info)
                        print(f"  找到产品: {product_name}")
        
        # 如果没有找到<dl>结构的产品，尝试其他方式
        if not products:
            print("  未找到<dl>结构的产品，尝试其他方式...")
            # 查找所有可能的产品链接
            product_links = soup.find_all('a', href=True)
            
            for link in product_links:
                href = link.get('href', '')
                title = link.get('title', '') or link.get_text(strip=True)
                
                # 过滤产品链接
                if href and title and ('product' in href or 'detail' in href):
                    if not href.startswith('http'):
                        href = urljoin(module_info['url'], href)
                    
                    products.append({
                        'name': title,
                        'title': title,
                        'url': href,
                        'description': '',
                        'module': module_info['name']
                    })
        
        print(f"  共找到 {len(products)} 个产品")
        return products
    
    def parse_product_page(self, product_info):
        """解析产品详情页，查找说明书下载链接"""
        print(f"    正在解析产品: {product_info['name']}")
        html = self.get_page(product_info['url'])
        if not html:
            return []
        
        soup = BeautifulSoup(html, 'html.parser')
        download_links = []
        
        # 1. 查找特殊的说明书下载区域 (g_sms 或其他可能的class)
        sms_divs = soup.find_all('div', class_=['g_sms', 'sms', 'download-area'])
        
        for sms_div in sms_divs:
            if sms_div:
                print(f"      找到说明书下载区域: {sms_div.get('class')}")
                
                # 查找所有的li元素（每个li包含一个说明书）
                li_elements = sms_div.find_all('li')
                
                for li in li_elements:
                    # 获取说明书标题 (v2)
                    v2_div = li.find('div', class_='v2')
                    manual_title = ""
                    if v2_div:
                        manual_title = v2_div.get_text(strip=True)
                    
                    # 获取版本信息 (v3)
                    v3_div = li.find('div', class_='v3')
                    version_info = ""
                    if v3_div:
                        version_info = v3_div.get_text(strip=True)
                    
                    # 获取日期信息 (v4)
                    v4_div = li.find('div', class_='v4')
                    date_info = ""
                    if v4_div:
                        date_info = v4_div.get_text(strip=True)
                    
                    # 查找下载链接 (v6 中的 a 标签)
                    v6_div = li.find('div', class_='v6')
                    if v6_div:
                        # 查找a标签，不一定有attach class
                        attach_link = v6_div.find('a', class_='attach') or v6_div.find('a')
                        if attach_link:
                            path = attach_link.get('path', '')
                            href = attach_link.get('href', '')
                            
                            download_url = ""
                            if path:
                                download_url = urljoin(self.base_url, path)
                            elif href and not href.startswith('javascript:'):
                                download_url = urljoin(self.base_url, href) if not href.startswith('http') else href
                            
                            # 即使没有有效的下载URL，也要记录这个说明书信息，供后面的path链接处理使用
                            if manual_title:  # 只要有说明书标题就记录
                                # 生成文件名（不包含日期）
                                filename = manual_title if manual_title else "说明书"
                                if version_info:
                                    # 只保留版本号，不要日期
                                    version = version_info.replace('版本号：', '').replace(':', '_').strip()
                                    if version:
                                        filename += f"_{version}"
                                
                                # 确保文件名以.pdf结尾
                                if not filename.lower().endswith('.pdf'):
                                    filename += '.pdf'
                                
                                # 将说明书信息存储起来，供后面匹配使用
                                if 'manual_info' not in product_info:
                                    product_info['manual_info'] = []
                                product_info['manual_info'].append({
                                    'title': manual_title,
                                    'filename': filename,
                                    'version': version_info,
                                    'date': date_info
                                })
                                
                                if download_url:
                                    download_links.append({
                                        'url': download_url,
                                        'title': filename,
                                        'manual_title': manual_title,
                                        'version': version_info,
                                        'date': date_info,
                                        'product_name': product_info['name'],
                                        'module': product_info['module'],
                                        'is_manual': True  # 标记为说明书文件
                                    })
                                    print(f"      找到说明书: {manual_title} - {download_url}")
        
        # 1.5 如果没有找到g_sms，尝试查找其他说明书下载结构
        if not download_links:
            # 查找所有包含path属性和v2、v3、v4结构的区域
            all_lis = soup.find_all('li')
            for li in all_lis:
                # 检查是否包含v2, v6结构
                v2_div = li.find('div', class_='v2')
                v6_div = li.find('div', class_='v6')
                
                if v2_div and v6_div:
                    manual_title = v2_div.get_text(strip=True)
                    
                    # 获取版本信息 (v3)
                    v3_div = li.find('div', class_='v3')
                    version_info = ""
                    if v3_div:
                        version_info = v3_div.get_text(strip=True)
                    
                    # 获取日期信息 (v4)
                    v4_div = li.find('div', class_='v4')
                    date_info = ""
                    if v4_div:
                        date_info = v4_div.get_text(strip=True)
                    
                    # 查找下载链接
                    attach_link = v6_div.find('a', class_='attach')
                    if attach_link:
                        path = attach_link.get('path', '')
                        if path:
                            download_url = urljoin(self.base_url, path)
                            
                            # 生成文件名
                            filename = manual_title if manual_title else "说明书"
                            if version_info:
                                filename += f"_{version_info.replace('版本号：', '').replace(':', '_')}"
                            if date_info:
                                filename += f"_{date_info}"
                            
                            if not filename.lower().endswith('.pdf'):
                                filename += '.pdf'
                            
                            download_links.append({
                                'url': download_url,
                                'title': filename,
                                'manual_title': manual_title,
                                'version': version_info,
                                'date': date_info,
                                'product_name': product_info['name'],
                                'module': product_info['module'],
                                'is_manual': True
                            })
                            print(f"      找到说明书(无g_sms): {manual_title} - {download_url}")
        
        # 2. 查找所有可能的下载链接（包括path属性的链接）
        all_links = soup.find_all('a', href=True)
        seen_urls = set()  # 用于去重
        
        for link in all_links:
            # 检查是否有path属性（JavaScript下载链接）
            path = link.get('path', '')
            if path:
                # 构建完整的下载URL
                download_url = urljoin(self.base_url, path)
                
                # 尝试从周围的HTML结构获取更好的文件名
                title = link.get('title', '') or link.get_text(strip=True)
                is_manual_file = False
                
                # 首先尝试从存储的说明书信息中匹配
                if 'manual_info' in product_info and product_info['manual_info']:
                    # 如果有存储的说明书信息，使用第一个（或者可以根据URL匹配）
                    manual_info = product_info['manual_info'][0]  # 简单起见使用第一个
                    title = manual_info['filename']
                    is_manual_file = True
                    print(f"      使用存储的说明书信息: {title}")
                else:
                    # 查找父级li元素，看是否有v2标签
                    parent_li = link.find_parent('li')
                    if parent_li:
                        v2_div = parent_li.find('div', class_='v2')
                        if v2_div:
                            manual_title = v2_div.get_text(strip=True)
                            if manual_title:
                                # 获取版本信息
                                v3_div = parent_li.find('div', class_='v3')
                                version_info = ""
                                if v3_div:
                                    version_info = v3_div.get_text(strip=True)
                                
                                # 获取日期信息
                                v4_div = parent_li.find('div', class_='v4')
                                date_info = ""
                                if v4_div:
                                    date_info = v4_div.get_text(strip=True)
                                
                                # 生成更好的文件名（不包含日期）
                                filename = manual_title
                                if version_info:
                                    version = version_info.replace('版本号：', '').replace(':', '_').strip()
                                    if version:
                                        filename += f"_{version}"
                                
                                if not filename.lower().endswith('.pdf'):
                                    filename += '.pdf'
                                
                                title = filename
                                is_manual_file = True
                                print(f"      找到正确命名的说明书: {title}")
                
                if download_url not in seen_urls:
                    seen_urls.add(download_url)
                    download_links.append({
                        'url': download_url,
                        'title': title if title else "点击下载",
                        'product_name': product_info['name'],
                        'module': product_info['module'],
                        'is_manual': is_manual_file
                    })
                    print(f"      找到path下载链接: {title} - {download_url}")
        
        # 3. 如果还没有找到下载链接，使用备用方法
        if not download_links:
            print(f"      未找到path下载链接，尝试其他方式...")
            
            # 查找包含"下载"、"说明书"、"手册"等关键词的链接
            download_keywords = ['下载', '说明书', '手册', '文档', '资料', 'download', 'manual', 'guide']
            
            for link in all_links:
                link_text = link.get_text(strip=True).lower()
                link_href = link.get('href', '').lower()
                link_title = link.get('title', '').lower()
                
                # 检查链接文本或href是否包含下载关键词
                is_download_link = any(keyword in link_text or keyword in link_href or keyword in link_title 
                                      for keyword in download_keywords)
                
                if is_download_link:
                    href = link.get('href', '')
                    title = link.get('title', '') or link.get_text(strip=True)
                    
                    # 确保URL是完整的
                    if href and not href.startswith('http'):
                        href = urljoin(product_info['url'], href)
                    
                    # 过滤掉一些明显不是文件的链接
                    if href and not any(skip in href.lower() for skip in ['javascript:', 'mailto:', '#']):
                        if href not in seen_urls:
                            seen_urls.add(href)
                            download_links.append({
                                'url': href,
                                'title': title,
                                'product_name': product_info['name'],
                                'module': product_info['module'],
                                'is_manual': False
                            })
                            print(f"      找到下载链接: {title} - {href}")
            
            # 查找文件扩展名链接（.pdf, .doc, .docx等）
            file_extensions = ['.pdf', '.doc', '.docx', '.xls', '.xlsx', '.zip', '.rar']
            for link in all_links:
                href = link.get('href', '')
                if any(ext in href.lower() for ext in file_extensions):
                    title = link.get('title', '') or link.get_text(strip=True)
                    
                    if not href.startswith('http'):
                        href = urljoin(product_info['url'], href)
                    
                    # 避免重复添加
                    if href not in seen_urls:
                        seen_urls.add(href)
                        download_links.append({
                            'url': href,
                            'title': title,
                            'product_name': product_info['name'],
                            'module': product_info['module'],
                            'is_manual': False
                        })
                        print(f"      找到文件链接: {title} - {href}")
        
        return download_links
    
    def download_file(self, download_info):
        """下载文件"""
        try:
            url = download_info['url']
            filename = download_info['title']
            
            # 清理文件名，移除非法字符
            filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
            
            # 如果文件名为空或只有空格，设置默认名称
            if not filename or not filename.strip():
                filename = "下载文件"
            
            # 如果文件名没有扩展名，尝试从URL获取
            if '.' not in filename.split('/')[-1]:
                parsed_url = urlparse(url)
                path = parsed_url.path
                if '.' in path:
                    ext = path.split('.')[-1]
                    if ext.lower() in ['pdf', 'doc', 'docx', 'xls', 'xlsx', 'zip', 'rar']:
                        filename += f'.{ext}'
                else:
                    # 如果URL中也没有扩展名，默认为PDF
                    filename += '.pdf'
            
            # 清理模块名和产品名中的非法字符
            module_name = re.sub(r'[<>:"/\\|?*]', '_', download_info['module'])
            product_name = re.sub(r'[<>:"/\\|?*]', '_', download_info['product_name'])
            
            # 创建模块目录
            module_dir = os.path.join(self.download_dir, module_name)
            if not os.path.exists(module_dir):
                os.makedirs(module_dir)
            
            # 创建产品目录
            product_dir = os.path.join(module_dir, product_name)
            if not os.path.exists(product_dir):
                os.makedirs(product_dir)
            
            # 所有文件都放在产品目录下的说明书文件夹
            manual_dir = os.path.join(product_dir, "说明书")
            if not os.path.exists(manual_dir):
                os.makedirs(manual_dir)
            target_dir = manual_dir
            
            # 完整的文件路径
            filepath = os.path.join(target_dir, filename)
            
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
            if 'text/html' in content_type:
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
                # 可以选择删除小文件
                # os.remove(filepath)
                # return False
            
            print(f"        下载完成: {filename} ({file_size} bytes)")
            
            # 记录新文件
            file_key = f"{download_info['module']}_{download_info['product_name']}_{filename}"
            if file_key not in self.processed_files:
                self.new_files.append({
                    'filename': filename,
                    'path': filepath,
                    'url': download_info['url'],
                    'size': file_size,
                    'module': download_info['module'],
                    'product': download_info['product_name']
                })
                self.processed_files.add(file_key)
            
            return True
            
        except Exception as e:
            print(f"        下载失败 {url}: {e}")
            return False
    
    def download_manuals(self, products):
        """下载所有产品的说明书"""
        if not products:
            print("没有产品需要下载说明书")
            return
        
        print(f"\n开始下载说明书...")
        print(f"共有 {len(products)} 个产品需要处理")
        
        total_downloads = 0
        successful_downloads = 0
        
        for i, product in enumerate(products, 1):
            print(f"\n进度: {i}/{len(products)} - {product['module']} - {product['name']}")
            
            # 解析产品页面，查找下载链接
            download_links = self.parse_product_page(product)
            
            if download_links:
                print(f"    找到 {len(download_links)} 个下载链接")
                
                # 下载每个文件
                for download_info in download_links:
                    total_downloads += 1
                    if self.download_file(download_info):
                        successful_downloads += 1
                    
                    # 添加延迟避免请求过快
                    time.sleep(1)
            else:
                print(f"    未找到下载链接")
            
            # 产品间延迟
            time.sleep(2)
        
        print(f"\n说明书下载完成！")
        print(f"总尝试下载: {total_downloads} 个文件")
        print(f"成功下载: {successful_downloads} 个文件")
        print(f"失败: {total_downloads - successful_downloads} 个文件")
    
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
        self.log("🚀 开始爬取德瑞斯产品中心...")
        
        # 1. 获取所有模块
        modules = self.parse_main_page()
        if not modules:
            self.log("❌ 未找到任何模块")
            return
        
        self.log(f"📋 共找到 {len(modules)} 个模块")
        
        # 2. 保存模块信息
        self.save_data(modules, 'modules.json')
        
        # 3. 爬取每个模块的产品
        all_products = []
        for i, module in enumerate(modules, 1):
            self.log(f"🔄 进度: {i}/{len(modules)} - {module['name']}")
            products = self.parse_module_page(module)
            all_products.extend(products)
            
            # 添加延迟避免请求过快
            time.sleep(1)
        
        # 4. 保存所有产品信息
        if all_products:
            self.save_data(all_products, 'products.json')
            self.log(f"✅ 爬取完成！共获取 {len(all_products)} 个产品")
        else:
            self.log("❌ 未找到任何产品信息")
            return
        
        # 5. 下载说明书
        self.download_manuals(all_products)
        
        # 6. 保存处理记录
        self.save_processed_files()
        
        # 7. 发送钉钉通知
        self.send_completion_notification()
        
        # 8. 生成统计报告
        self.generate_report(modules, all_products)
    
    def send_completion_notification(self):
        """发送完成通知"""
        if not self.new_files:
            if not self.is_first_run:
                self.log("📢 无新文件，不发送通知")
            return
        
        # 构建通知消息
        message_parts = []
        message_parts.append(f"📊 德瑞斯爬虫完成")
        message_parts.append(f"🕒 时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        message_parts.append(f"📁 新下载文件: {len(self.new_files)} 个")
        
        if self.is_first_run:
            message_parts.append("🆕 首次运行，已建立基线")
        
        # 按模块分组显示新文件
        module_files = {}
        for file_info in self.new_files:
            module = file_info['module']
            if module not in module_files:
                module_files[module] = []
            module_files[module].append(file_info)
        
        message_parts.append("\n📋 新文件详情:")
        for module, files in module_files.items():
            message_parts.append(f"  📂 {module}: {len(files)} 个文件")
            for file_info in files[:3]:  # 只显示前3个
                size_mb = file_info['size'] / 1024 / 1024
                message_parts.append(f"    📄 {file_info['filename']} ({size_mb:.1f}MB)")
            if len(files) > 3:
                message_parts.append(f"    ... 还有 {len(files) - 3} 个文件")
        
        message = "\n".join(message_parts)
        self.send_dingtalk_notification(message)
    
    def generate_report(self, modules, products):
        """生成爬取报告"""
        report = {
            '爬取时间': time.strftime('%Y-%m-%d %H:%M:%S'),
            '总模块数': len(modules),
            '总产品数': len(products),
            '模块列表': [m['name'] for m in modules],
            '各模块产品数量': {}
        }
        
        # 统计各模块产品数量
        for module in modules:
            module_products = [p for p in products if p['module'] == module['name']]
            report['各模块产品数量'][module['name']] = len(module_products)
        
        # 保存报告
        self.save_data(report, '爬取报告.json')
        
        # 打印报告
        print("\n" + "="*50)
        print("爬取报告")
        print("="*50)
        print(f"爬取时间: {report['爬取时间']}")
        print(f"总模块数: {report['总模块数']}")
        print(f"总产品数: {report['总产品数']}")
        print("\n各模块产品数量:")
        for module_name, count in report['各模块产品数量'].items():
            print(f"  {module_name}: {count} 个")
        print("="*50)
    
    def download_manuals_only(self):
        """只下载说明书，不重新爬取产品信息"""
        print("开始下载说明书（使用现有产品数据）...")
        
        # 加载现有产品数据
        products_file = os.path.join(self.output_dir, 'products.json')
        if not os.path.exists(products_file):
            print("未找到产品数据文件，请先运行完整爬取")
            return
        
        try:
            with open(products_file, 'r', encoding='utf-8') as f:
                products = json.load(f)
            
            print(f"加载了 {len(products)} 个产品信息")
            self.download_manuals(products)
            
        except Exception as e:
            print(f"加载产品数据失败: {e}")

if __name__ == "__main__":
    spider = DiriseSpider()
    try:
        # 检查命令行参数
        import sys
        if len(sys.argv) > 1 and sys.argv[1] == '--download-only':
            spider.download_manuals_only()
        else:
            spider.run()
    except KeyboardInterrupt:
        print("\n用户中断爬取")
    except Exception as e:
        print(f"爬取过程中出现错误: {e}")
