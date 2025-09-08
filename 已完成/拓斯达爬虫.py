#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
拓斯达产品中心爬虫
爬取产品和技术下面的所有模块
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

class TopstarSpider:
    def __init__(self):
        self.base_url = "https://www.topstarltd.com"
        self.main_url = "https://www.topstarltd.com/lang-cn/product.html"
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
            self.base_dir = "/srv/downloads/approved/拓斯达"
            self.output_dir = os.path.join(self.base_dir, "产品数据")
            self.download_dir = os.path.join(self.base_dir, "资料下载")
        else:
            # 本地测试环境
            self.base_dir = os.path.join(os.getcwd(), "拓斯达")
            self.output_dir = os.path.join(self.base_dir, "产品数据")
            self.download_dir = os.path.join(self.base_dir, "资料下载")
        
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
                    "content": f"🤖 拓斯达爬虫通知\n{message}"
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
        """解析主页面，获取所有产品和技术模块链接"""
        print("正在解析主页面...")
        html = self.get_page(self.main_url)
        if not html:
            return []
        
        soup = BeautifulSoup(html, 'html.parser')
        modules = []
        
        # 查找产品和技术模块
        # 根据提供的HTML结构，查找 class="menuli active" 下的产品和技术模块
        menu_li = soup.find('li', class_='menuli active')
        if menu_li:
            # 在 toggle 下的 ul 中查找所有模块
            toggle_div = menu_li.find('div', class_='toggle')
            if toggle_div:
                # 获取所有主要模块（box1级别）
                main_modules = toggle_div.find_all('li')
                
                for main_module in main_modules:
                    box1 = main_module.find('div', class_='box1')
                    if box1:
                        # 获取主模块信息
                        main_link = box1.find('a')
                        if main_link:
                            main_name = main_link.get_text(strip=True)
                            main_url = main_link.get('href', '')
                            
                            if main_url and not main_url.startswith('http'):
                                main_url = urljoin(self.base_url, main_url)
                            
                            print(f"找到主模块: {main_name} - {main_url}")
                            
                            # 查找子模块
                            moretoggle = main_module.find('div', class_='moretoggle')
                            if moretoggle:
                                # 查找所有三级模块（box3级别）
                                box3_divs = moretoggle.find_all('div', class_='box3')
                                
                                for box3 in box3_divs:
                                    sub_link = box3.find('a')
                                    if sub_link:
                                        sub_name = sub_link.get_text(strip=True)
                                        sub_url = sub_link.get('href', '')
                                        
                                        # 过滤掉 javascript:void(0) 链接
                                        if sub_url and sub_url != 'javascript:void(0)':
                                            if not sub_url.startswith('http'):
                                                sub_url = urljoin(self.base_url, sub_url)
                                            
                                            modules.append({
                                                'name': sub_name,
                                                'url': sub_url,
                                                'type': '产品模块',
                                                'parent': main_name
                                            })
                                            print(f"  找到子模块: {sub_name} - {sub_url}")
                            
                            # 如果没有子模块，将主模块也添加（如科研力量）
                            if not moretoggle.find_all('div', class_='box3'):
                                # 检查是否有直接的box3子模块
                                direct_box3 = moretoggle.find_all('div', class_='box3')
                                if direct_box3:
                                    for box3 in direct_box3:
                                        sub_link = box3.find('a')
                                        if sub_link:
                                            sub_name = sub_link.get_text(strip=True)
                                            sub_url = sub_link.get('href', '')
                                            
                                            if sub_url and sub_url != 'javascript:void(0)':
                                                if not sub_url.startswith('http'):
                                                    sub_url = urljoin(self.base_url, sub_url)
                                                
                                                modules.append({
                                                    'name': sub_name,
                                                    'url': sub_url,
                                                    'type': '科研模块',
                                                    'parent': main_name
                                                })
                                                print(f"  找到科研模块: {sub_name} - {sub_url}")
                                else:
                                    # 如果确实没有子模块，添加主模块本身
                                    if main_url and main_url != 'javascript:void(0)':
                                        modules.append({
                                            'name': main_name,
                                            'url': main_url,
                                            'type': '主模块',
                                            'parent': '产品和技术'
                                        })
        
        return modules
    
    def parse_module_page(self, module_info):
        """解析具体模块页面，提取产品列表（支持分页）"""
        print(f"\n正在解析模块: {module_info['name']}")
        
        all_products = []
        page = 1
        
        while True:
            # 构建分页URL
            if page == 1:
                page_url = module_info['url']
            else:
                # 添加分页参数
                if '?' in module_info['url']:
                    page_url = f"{module_info['url']}&page={page}"
                else:
                    page_url = f"{module_info['url']}?page={page}"
            
            print(f"  正在解析第 {page} 页: {page_url}")
            
            html = self.get_page(page_url)
            if not html:
                break
            
            soup = BeautifulSoup(html, 'html.parser')
            page_products = []
            
            # 根据提供的HTML结构，查找产品列表
            # 查找 <div class="all list"> 下的产品
            all_list_div = soup.find('div', class_='all list')
            if all_list_div:
                # 在 ul 中查找所有 li 产品项
                product_ul = all_list_div.find('ul')
                if product_ul:
                    product_lis = product_ul.find_all('li', class_='time05')
                    
                    for li in product_lis:
                        # 获取产品名称和链接
                        name_div = li.find('div', class_='name line-one')
                        if name_div:
                            product_link = name_div.find('a')
                            if product_link:
                                product_name = product_link.get_text(strip=True)
                                product_url = product_link.get('href', '')
                                
                                if product_url and not product_url.startswith('http'):
                                    product_url = urljoin(self.base_url, product_url)
                                
                                # 获取产品描述信息
                                desc_info = {}
                                desc_div = li.find('div', class_='desc')
                                if desc_div:
                                    # 提取负载和臂长等信息
                                    text_divs = desc_div.find_all('div', class_=['text1', 'text2'])
                                    for text_div in text_divs:
                                        font_elem = text_div.find('font')
                                        if font_elem:
                                            text = font_elem.get_text(strip=True)
                                            if '负载：' in text:
                                                desc_info['负载'] = text
                                            elif '臂长：' in text:
                                                desc_info['臂长'] = text
                                
                                # 获取产品图片
                                img_url = ""
                                img_div = li.find('div', class_='img')
                                if img_div:
                                    img_elem = img_div.find('img')
                                    if img_elem:
                                        img_src = img_elem.get('src', '')
                                        if img_src and not img_src.startswith('http'):
                                            img_url = urljoin(self.base_url, img_src)
                                        else:
                                            img_url = img_src
                                
                                product_info = {
                                    'name': product_name,
                                    'url': product_url,
                                    'description': desc_info,
                                    'image_url': img_url,
                                    'module': module_info['name'],
                                    'parent_module': module_info.get('parent', ''),
                                    'page': page  # 记录页码
                                }
                                page_products.append(product_info)
                                print(f"    找到产品: {product_name}")
            
            # 如果当前页没有产品，说明已经到了最后一页
            if not page_products:
                print(f"  第 {page} 页没有产品，结束分页爬取")
                break
            

            
            all_products.extend(page_products)
            print(f"  第 {page} 页找到 {len(page_products)} 个产品")
            
            # 检查是否有下一页
            # 查找分页信息，如果有"下一页"或者数字链接，继续爬取
            has_next_page = False
            
            # 方法1：查找分页导航
            pagination_divs = soup.find_all('div', class_='fen-page')
            for pagination_div in pagination_divs:
                # 查找分页链接
                page_links = pagination_div.find_all('a')
                for link in page_links:
                    link_text = link.get_text(strip=True)
                    href = link.get('href', '')
                    # 如果找到下一页链接或者页码大于当前页
                    if ('下一页' in link_text or '>' in link_text or 
                        (link_text.isdigit() and int(link_text) > page)):
                        has_next_page = True
                        break
                if has_next_page:
                    break
            
            # 简化分页逻辑：直接根据模块名称决定页数
            if module_info['name'] == '机械手':
                max_pages = 2
            elif module_info['name'] == '辅机':
                max_pages = 3
            else:
                max_pages = 1
            
            # 如果当前页达到最大页数，停止爬取
            if page >= max_pages:
                has_next_page = False
                print(f"    模块 {module_info['name']} 已达到最大页数 {max_pages}，停止爬取")
                break
            
            # 简化逻辑：如果当前页小于最大页数，继续爬取下一页
            if page < max_pages:
                has_next_page = True
                print(f"    模块 {module_info['name']} 第{page}页完成，继续爬取第{page+1}页")
            else:
                has_next_page = False
                print(f"    模块 {module_info['name']} 已达到最大页数 {max_pages}，停止爬取")
            
            if not has_next_page:
                print(f"  没有更多页面，结束爬取")
                break
            
            page += 1
            
            # 添加页面间延迟
            time.sleep(1)
        
        print(f"  模块 {module_info['name']} 共找到 {len(all_products)} 个产品（跨 {page} 页）")
        return all_products
    
    def parse_product_page(self, product_info):
        """解析产品详情页，查找资料下载链接"""
        print(f"    正在解析产品: {product_info['name']}")
        html = self.get_page(product_info['url'])
        if not html:
            return []
        
        soup = BeautifulSoup(html, 'html.parser')
        download_links = []
        
        # 根据提供的HTML结构，查找资料下载区域
        # 查找 <div class="all provdown"> 下的资料下载
        provdown_div = soup.find('div', class_='all provdown')
        if provdown_div:
            # 查找资料下载标题确认
            title_div = provdown_div.find('div', class_='nytitle')
            if title_div and '资料下载' in title_div.get_text():
                print(f"      找到资料下载区域")
                
                # 在 desc 的 ul 中查找所有下载项
                desc_div = provdown_div.find('div', class_='desc')
                if desc_div:
                    download_ul = desc_div.find('ul')
                    if download_ul:
                        download_lis = download_ul.find_all('li', class_='time05')
                        
                        for li in download_lis:
                            download_link = li.find('a')
                            if download_link:
                                # 获取下载URL
                                download_url = download_link.get('href', '')
                                if download_url and not download_url.startswith('http'):
                                    download_url = urljoin(self.base_url, download_url)
                                
                                # 获取资料名称
                                data_download = download_link.get('data-download', '')
                                if not data_download:
                                    # 如果没有data-download属性，从p标签获取
                                    p_elem = download_link.find('p')
                                    if p_elem:
                                        data_download = p_elem.get_text(strip=True)
                                
                                if download_url and data_download:
                                    # 生成文件名
                                    filename = data_download
                                    if not filename.lower().endswith('.pdf'):
                                        filename += '.pdf'
                                    
                                    download_links.append({
                                        'url': download_url,
                                        'title': filename,
                                        'original_name': data_download,
                                        'product_name': product_info['name'],
                                        'module': product_info['module'],
                                        'parent_module': product_info.get('parent_module', ''),
                                        'is_material': True  # 标记为产品资料文件
                                    })
                                    print(f"      找到资料: {data_download} - {download_url}")
        
        # 如果没有找到专门的资料下载区域，查找其他可能的下载链接
        if not download_links:
            print(f"      未找到专门的资料下载区域，尝试查找其他下载链接...")
            
            # 查找所有可能的下载链接
            all_links = soup.find_all('a', href=True)
            download_keywords = ['下载', '资料', '手册', '文档', 'pdf', 'doc', 'download', 'manual']
            
            for link in all_links:
                href = link.get('href', '')
                link_text = link.get_text(strip=True).lower()
                
                # 检查是否是文档下载链接
                is_download_link = any(keyword in link_text or keyword in href.lower() 
                                     for keyword in download_keywords)
                
                if is_download_link and href:
                    if not href.startswith('http'):
                        href = urljoin(product_info['url'], href)
                    
                    # 过滤掉一些明显不是文件的链接
                    if not any(skip in href.lower() for skip in ['javascript:', 'mailto:', '#']):
                        title = link.get_text(strip=True) or "产品资料"
                        
                        download_links.append({
                            'url': href,
                            'title': title,
                            'original_name': title,
                            'product_name': product_info['name'],
                            'module': product_info['module'],
                            'parent_module': product_info.get('parent_module', ''),
                            'is_material': False
                        })
                        print(f"      找到备用下载链接: {title} - {href}")
        
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
                filename = "产品资料"
            
            # 确保文件有扩展名
            if '.' not in filename.split('/')[-1]:
                # 尝试从URL获取扩展名
                parsed_url = urlparse(url)
                path = parsed_url.path
                if '.' in path:
                    ext = path.split('.')[-1].lower()
                    if ext in ['pdf', 'doc', 'docx', 'xls', 'xlsx', 'zip', 'rar']:
                        filename += f'.{ext}'
                else:
                    # 默认为PDF
                    filename += '.pdf'
            
            # 清理目录名中的非法字符
            parent_module = re.sub(r'[<>:"/\\|?*]', '_', download_info.get('parent_module', '其他'))
            module_name = re.sub(r'[<>:"/\\|?*]', '_', download_info['module'])
            product_name = re.sub(r'[<>:"/\\|?*]', '_', download_info['product_name'])
            
            # 创建目录结构: 父模块/子模块/产品名/资料
            parent_dir = os.path.join(self.download_dir, parent_module)
            if not os.path.exists(parent_dir):
                os.makedirs(parent_dir)
            
            module_dir = os.path.join(parent_dir, module_name)
            if not os.path.exists(module_dir):
                os.makedirs(module_dir)
            
            product_dir = os.path.join(module_dir, product_name)
            if not os.path.exists(product_dir):
                os.makedirs(product_dir)
            
            # 所有文件都放在产品目录下的资料文件夹
            material_dir = os.path.join(product_dir, "产品资料")
            if not os.path.exists(material_dir):
                os.makedirs(material_dir)
            
            # 完整的文件路径
            filepath = os.path.join(material_dir, filename)
            
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
                    'product': download_info['product_name'],
                    'parent_module': download_info.get('parent_module', '')
                })
                self.processed_files.add(file_key)
            
            return True
            
        except Exception as e:
            print(f"        下载失败 {url}: {e}")
            return False
    
    def download_materials(self, products):
        """下载所有产品的资料"""
        if not products:
            print("没有产品需要下载资料")
            return
        
        print(f"\n开始下载产品资料...")
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
        
        print(f"\n产品资料下载完成！")
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
        self.log("🚀 开始爬取拓斯达产品中心...")
        
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
        
        # 5. 下载产品资料
        self.download_materials(all_products)
        
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
        message_parts.append(f"📊 拓斯达爬虫完成")
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
            '各模块产品数量': {},
            '分页统计': {}
        }
        
        # 统计各模块产品数量和分页信息
        for module in modules:
            module_products = [p for p in products if p['module'] == module['name']]
            report['各模块产品数量'][module['name']] = len(module_products)
            
            # 统计分页信息
            if module_products:
                max_page = max([p.get('page', 1) for p in module_products])
                page_counts = {}
                for page_num in range(1, max_page + 1):
                    page_products = [p for p in module_products if p.get('page', 1) == page_num]
                    if page_products:
                        page_counts[f'第{page_num}页'] = len(page_products)
                
                if max_page > 1:
                    report['分页统计'][module['name']] = {
                        '总页数': max_page,
                        '各页产品数': page_counts
                    }
        
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
            
            # 显示分页信息
            if module_name in report['分页统计']:
                page_info = report['分页统计'][module_name]
                print(f"    └─ 分页情况: 共{page_info['总页数']}页")
                for page, count in page_info['各页产品数'].items():
                    print(f"       {page}: {count}个产品")
        
        print("="*50)
    
    def download_materials_only(self):
        """只下载产品资料，不重新爬取产品信息"""
        print("开始下载产品资料（使用现有产品数据）...")
        
        # 加载现有产品数据
        products_file = os.path.join(self.output_dir, 'products.json')
        if not os.path.exists(products_file):
            print("未找到产品数据文件，请先运行完整爬取")
            return
        
        try:
            with open(products_file, 'r', encoding='utf-8') as f:
                products = json.load(f)
            
            print(f"加载了 {len(products)} 个产品信息")
            self.download_materials(products)
            
        except Exception as e:
            print(f"加载产品数据失败: {e}")

if __name__ == "__main__":
    spider = TopstarSpider()
    try:
        # 检查命令行参数
        import sys
        if len(sys.argv) > 1 and sys.argv[1] == '--download-only':
            spider.download_materials_only()
        else:
            spider.run()
    except KeyboardInterrupt:
        print("\n用户中断爬取")
    except Exception as e:
        print(f"爬取过程中出现错误: {e}")
