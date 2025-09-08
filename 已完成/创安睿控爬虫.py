#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
创安睿控下载中心爬虫
爬取所有分类下的技术资料和产品文档
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

class ChuangAnSpider:
    def __init__(self):
        self.base_url = "https://www.cschueun.com"
        self.main_url = "https://www.cschueun.com/download.html"
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
            self.base_dir = "/srv/downloads/approved/创安睿控"
            self.output_dir = os.path.join(self.base_dir, "产品数据")
            self.download_dir = os.path.join(self.base_dir, "下载文件")
        else:
            # 本地测试环境
            self.base_dir = os.path.join(os.getcwd(), "创安睿控下载")
            self.output_dir = os.path.join(self.base_dir, "产品数据")
            self.download_dir = os.path.join(self.base_dir, "下载文件")
        
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
                    "content": f"🤖 创安睿控爬虫通知\n{message}"
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
        """解析主页面，获取所有大模块"""
        print("正在解析主页面...")
        html = self.get_page(self.main_url)
        if not html:
            return []
        
        soup = BeautifulSoup(html, 'html.parser')
        modules = []
        
        # 查找大模块
        tree_div = soup.find('div', class_='p_c_tree')
        if tree_div:
            # 查找所有一级模块（deep-1）
            level1_items = tree_div.find_all('li', class_='p_c_item')
            
            for item in level1_items:
                # 查找一级模块链接
                level1_link = item.find('a', class_='p_c_title1')
                if level1_link:
                    module_name = level1_link.get_text(strip=True)
                    module_url = level1_link.get('href', '').strip()
                    
                    if module_name and module_url:
                        full_url = urljoin(self.base_url, module_url)
                        
                        # 查找子模块
                        sub_modules = []
                        level2_ul = item.find('ul', class_='deep-2')
                        if level2_ul:
                            level2_items = level2_ul.find_all('li', class_='p_c_item')
                            for sub_item in level2_items:
                                sub_link = sub_item.find('a', class_='p_c_title2')
                                if sub_link:
                                    sub_name = sub_link.get_text(strip=True)
                                    sub_url = sub_link.get('href', '').strip()
                                    if sub_name and sub_url:
                                        sub_full_url = urljoin(self.base_url, sub_url)
                                        sub_modules.append({
                                            'name': sub_name,
                                            'url': sub_full_url
                                        })
                        
                        module_info = {
                            'name': module_name,
                            'url': full_url,
                            'sub_modules': sub_modules,
                            'type': '一级模块'
                        }
                        modules.append(module_info)
                        print(f"找到模块: {module_name} - {full_url}")
                        if sub_modules:
                            for sub in sub_modules:
                                print(f"  子模块: {sub['name']} - {sub['url']}")
        
        return modules
    
    def parse_module_page(self, module_info):
        """解析模块页面，获取文件列表（支持翻页）"""
        print(f"\n正在解析模块: {module_info['name']}")
        all_files = []
        page = 1
        
        while True:
            # 构建分页URL
            if page == 1:
                url = module_info['url']
            else:
                # 如果上一次循环已经找到了下一页URL，直接使用
                if hasattr(self, '_next_page_url') and self._next_page_url:
                    url = self._next_page_url
                    self._next_page_url = None  # 清除，避免重复使用
                else:
                    # 使用保存的模块ID构建URL
                    if hasattr(self, '_module_ids') and module_info['name'] in self._module_ids:
                        module_id = self._module_ids[module_info['name']]
                        start = (page - 1) * 6
                        
                        base_url = module_info['url']
                        if '/download_1/' in base_url:
                            url = f"{self.base_url}/download_1/{module_id}-{start}-6.html"
                        else:
                            url = f"{self.base_url}/download/{module_id}-{start}-6.html"
                    else:
                        # 备用方法
                        base_url = module_info['url']
                        parts = base_url.split('/')
                        module_id = parts[-1].replace('.html', '')
                        start = (page - 1) * 6
                        
                        if '/download_1/' in base_url:
                            url = f"{self.base_url}/download_1/{module_id}-{start}-6.html"
                        else:
                            url = f"{self.base_url}/download/{module_id}-{start}-6.html"
            
            print(f"  解析第 {page} 页: {url}")
            html = self.get_page(url)
            if not html:
                break
            
            soup = BeautifulSoup(html, 'html.parser')
            files_found = False
            
            # 查找文件列表 - 使用正确的cbox-33结构
            p_list = soup.find('div', class_='p_list')
            if p_list:
                file_items = p_list.find_all('div', class_='cbox-33')
                
                for item in file_items:
                    files_found = True
                    
                    # 获取文件标题 (在cbox-35-0中的h1)
                    title_container = item.find('div', class_='cbox-35-0')
                    title = "未知文件"
                    if title_container:
                        title_elem = title_container.find('h1', class_='e_h1-41')
                        if title_elem:
                            title = title_elem.get_text(strip=True)
                    
                    # 获取文件大小 (在cbox-35-1中的p)
                    size_container = item.find('div', class_='cbox-35-1')
                    size = "未知大小"
                    if size_container:
                        size_elem = size_container.find('p', class_='e_text-40')
                        if size_elem:
                            size = size_elem.get_text(strip=True)
                    
                    # 获取发布日期 (在cbox-35-2中的p)
                    date_container = item.find('div', class_='cbox-35-2')
                    date = "未知日期"
                    if date_container:
                        date_elem = date_container.find('p', class_='e_timeFormat-36')
                        if date_elem:
                            date = date_elem.get_text(strip=True)
                    
                    # 获取下载链接 (在cbox-35-3中的a)
                    download_container = item.find('div', class_='cbox-35-3')
                    download_url = ""
                    if download_container:
                        download_link = download_container.find('a', href=True)
                        if download_link:
                            href = download_link.get('href', '')
                            if href.startswith('http'):
                                download_url = href
                            else:
                                download_url = urljoin(self.base_url, href)
                    
                    if download_url and title != "未知文件":
                        file_info = {
                            'title': title,
                            'size': size,
                            'date': date,
                            'url': download_url,
                            'module': module_info['name'],
                            'page': page
                        }
                        all_files.append(file_info)
                        print(f"    找到文件: {title} ({size}) - {date}")
            
            # 方法2: 如果没有找到cbox-2，查找Download链接及其相关信息
            if not files_found:
                download_links = soup.find_all('a', string=lambda x: x and 'download' in x.lower())
                
                for download_link in download_links:
                    files_found = True
                    
                    # 获取下载URL
                    href = download_link.get('href', '')
                    if href.startswith('http'):
                        download_url = href
                    else:
                        download_url = urljoin(self.base_url, href)
                    
                    # 向上查找包含文件信息的容器
                    container = download_link
                    for _ in range(10):  # 最多向上查找10级
                        container = container.parent
                        if not container:
                            break
                        
                        # 查找文件标题 - 在e_container-3或类似容器中
                        if 'e_container' in str(container.get('class', [])):
                            break
                    
                    title = "未知文件"
                    size = "未知大小"
                    date = "未知日期"
                    
                    if container:
                        # 查找标题
                        title_elem = container.find(['h1', 'h2', 'h3', 'p'], class_=lambda x: x and ('title' in str(x) or 'subtitle' in str(x)))
                        if title_elem:
                            title = title_elem.get_text(strip=True)
                        
                        # 查找文件大小
                        size_elem = container.find('p', string=lambda x: x and ('MB' in x or 'KB' in x or 'GB' in x))
                        if size_elem:
                            size = size_elem.get_text(strip=True)
                        
                        # 查找日期
                        date_elem = container.find('p', string=lambda x: x and ('/' in x or '-' in x) and len(x.strip()) <= 12)
                        if date_elem:
                            date = date_elem.get_text(strip=True)
                    
                    if download_url and title != "未知文件":
                        file_info = {
                            'title': title,
                            'size': size,
                            'date': date,
                            'url': download_url,
                            'module': module_info['name'],
                            'page': page
                        }
                        all_files.append(file_info)
                        print(f"    找到文件: {title} ({size}) - {date}")
            
            # 方法3: 如果还没有找到，尝试查找直接的文件链接
            if not files_found:
                # 查找所有文件链接
                file_links = soup.find_all('a', href=lambda x: x and any(ext in x.lower() for ext in ['.pdf', '.doc', '.docx', '.zip', '.rar']))
                
                for link in file_links:
                    files_found = True
                    
                    href = link.get('href', '')
                    if href.startswith('http'):
                        download_url = href
                    else:
                        download_url = urljoin(self.base_url, href)
                    
                    # 从URL或链接文本获取标题
                    title = link.get_text(strip=True) or href.split('/')[-1]
                    
                    file_info = {
                        'title': title,
                        'size': "未知大小",
                        'date': "未知日期",
                        'url': download_url,
                        'module': module_info['name'],
                        'page': page
                    }
                    all_files.append(file_info)
                    print(f"    找到文件链接: {title}")
            
            # 检查是否有下一页 - 使用模拟浏览器的方法
            has_next = False
            next_page_url = None
            
            # 方法1: 检查标准分页结构（优先方法）
            page_div = soup.find('div', class_='p_page')
            if page_div:
                # 查找下一页按钮
                next_link = page_div.find('a', class_='page_next')
                if next_link:
                    next_href = next_link.get('href', '')
                    disabled = 'disabled' in next_link.get('class', [])
                    
                    if not disabled and next_href and next_href != 'javascript:;':
                        # 找到有效的下一页链接
                        if next_href.startswith('/'):
                            next_page_url = urljoin(self.base_url, next_href)
                        else:
                            next_page_url = next_href
                        has_next = True
                        print(f"    找到下一页按钮: {next_page_url}")
                
                # 如果没有下一页按钮，检查页码链接
                if not has_next:
                    page_links = page_div.find_all('a', class_='page_num')
                    current_page_found = False
                    for i, link in enumerate(page_links):
                        link_classes = link.get('class', [])
                        link_text = link.get_text().strip()
                        
                        # 找到当前页
                        if 'current' in link_classes or link_text == str(page):
                            current_page_found = True
                            # 检查是否还有下一个页码
                            if i + 1 < len(page_links):
                                next_page_link = page_links[i + 1]
                                next_href = next_page_link.get('href', '')
                                if next_href and next_href != 'javascript:;':
                                    if next_href.startswith('/'):
                                        next_page_url = urljoin(self.base_url, next_href)
                                    else:
                                        next_page_url = next_href
                                    has_next = True
                                    next_page_num = next_page_link.get_text().strip()
                                    print(f"    找到第{next_page_num}页链接: {next_page_url}")
                            break
            
            # 方法2: 如果第一页没有分页结构，但文件数量较多，尝试探测性检查
            if not has_next and page == 1 and files_found:
                current_files = len([f for f in all_files if f['page'] == page])
                
                # 使用模块特定的规则来判断是否需要分页
                should_paginate = False
                module_name = module_info.get('name', '')
                
                # 根据已知信息进行特定判断
                if '宣传彩页' in module_name:
                    # 宣传彩页确实有3页
                    should_paginate = True
                    print(f"    宣传彩页模块：已知有3页，需要分页")
                elif '行业专机驱动器' in module_name:
                    # 行业专机驱动器只有1页
                    should_paginate = False
                    print(f"    行业专机驱动器模块：已知只有1页，无需分页")
                elif '说明书-变频器' in module_name:
                    # 说明书-变频器确实有3页
                    should_paginate = True
                    print(f"    说明书-变频器模块：已知有3页，需要分页")
                else:
                    # 其他模块使用 pageParamsJson 和文件数量综合判断
                    page_params_input = soup.find('input', attrs={'name': 'pageParamsJson'})
                    if page_params_input:
                        try:
                            import json
                            page_params = json.loads(page_params_input.get('value', '{}'))
                            total_count = page_params.get('totalCount', 0)
                            page_size = page_params.get('size', 6)
                            
                            # 如果 pageParamsJson 明确显示总数少于当前文件数，说明无需分页
                            if total_count > 0 and total_count < current_files:
                                should_paginate = False
                                print(f"    pageParamsJson显示总共{total_count}个文件，当前已有{current_files}个，无需分页")
                            elif current_files >= 6:
                                should_paginate = True
                                print(f"    当前{current_files}个文件较多，尝试分页")
                            else:
                                should_paginate = False
                                print(f"    当前{current_files}个文件，无需分页")
                        except:
                            should_paginate = current_files >= 6
                    else:
                        should_paginate = current_files >= 6
                
                # 只有当确实需要分页时才探测第2页
                if should_paginate:
                    print(f"    第一页有{current_files}个文件，尝试探测第2页")
                    
                    # 构造可能的第二页URL
                    test_urls = []
                    base_url = module_info['url']
                    
                    if '/download/' in base_url:
                        parts = base_url.split('/')
                        module_id = parts[-1].replace('.html', '')
                        test_urls.append(f"{self.base_url}/download/{module_id}-6-6.html")
                        # 尝试常见模式
                        test_urls.append(f"{self.base_url}/download/16957180-6-6.html")
                    elif '/download_1/' in base_url:
                        parts = base_url.split('/')
                        module_id = parts[-1].replace('.html', '')
                        # 对于宣传彩页，使用已知的正确模块ID
                        if module_id == '1' and '宣传彩页' in module_info['name']:
                            test_urls.append(f"{self.base_url}/download_1/16728513-6-6.html")
                        else:
                            test_urls.append(f"{self.base_url}/download_1/{module_id}-6-6.html")
                    
                    # 测试URL是否有效
                    for test_url in test_urls:
                        test_html = self.get_page(test_url)
                        if test_html:
                            test_soup = BeautifulSoup(test_html, 'html.parser')
                            test_downloads = test_soup.find_all('a', string=lambda x: x and 'download' in x.lower())
                            if test_downloads:
                                next_page_url = test_url
                                has_next = True
                                # 保存模块ID以便后续使用
                                if not hasattr(self, '_module_ids'):
                                    self._module_ids = {}
                                url_parts = test_url.split('/')[-1].split('-')
                                if len(url_parts) >= 3:
                                    self._module_ids[module_info['name']] = url_parts[0]
                                print(f"    探测到第2页存在: {test_url}")
                                break
            
            # 方法3: 对于已确认有分页的模块，继续构造后续页面
            elif not has_next and page > 1 and hasattr(self, '_module_ids'):
                if module_info['name'] in self._module_ids:
                    module_id = self._module_ids[module_info['name']]
                    start = page * 6
                    
                    base_url = module_info['url']
                    if '/download_1/' in base_url:
                        test_url = f"{self.base_url}/download_1/{module_id}-{start}-6.html"
                    else:
                        test_url = f"{self.base_url}/download/{module_id}-{start}-6.html"
                    
                    # 测试是否有更多页面
                    test_html = self.get_page(test_url)
                    if test_html:
                        test_soup = BeautifulSoup(test_html, 'html.parser')
                        test_downloads = test_soup.find_all('a', string=lambda x: x and 'download' in x.lower())
                        if test_downloads:
                            next_page_url = test_url
                            has_next = True
                            print(f"    继续找到第{page + 1}页: {test_url}")
            
            # 如果找到了下一页URL，保存它以便下次循环使用
            if has_next and next_page_url:
                self._next_page_url = next_page_url
                # 从下一页URL中提取模块ID，用于后续页面构造
                if not hasattr(self, '_module_ids'):
                    self._module_ids = {}
                if module_info['name'] not in self._module_ids:
                    url_parts = next_page_url.split('/')[-1].split('-')
                    if len(url_parts) >= 3:
                        self._module_ids[module_info['name']] = url_parts[0]
            
            # 如果没有找到文件或没有下一页，退出循环
            if not files_found or not has_next:
                break
                
            page += 1
            time.sleep(1)  # 避免请求过快
        
        print(f"  共找到 {len(all_files)} 个文件")
        return all_files
    
    def download_file(self, file_info):
        """下载文件"""
        try:
            url = file_info['url']
            title = file_info['title']
            
            # 清理文件名，移除非法字符
            filename = re.sub(r'[<>:"/\\|?*]', '_', title)
            
            # 如果文件名为空或只有空格，设置默认名称
            if not filename or not filename.strip():
                filename = "下载文件" 
            
            # 从URL获取文件扩展名
            parsed_url = urlparse(url)
            path = parsed_url.path
            if '.' in path:
                ext = path.split('.')[-1].lower()
                if ext in ['pdf', 'doc', 'docx', 'xls', 'xlsx', 'zip', 'rar']:
                    if not filename.lower().endswith(f'.{ext}'):
                        filename += f'.{ext}'
            else:
                # 如果URL中没有扩展名，根据内容类型判断
                filename += '.pdf'  # 默认PDF
            
            # 解析模块名，分离大模块和子模块
            module_full_name = file_info['module']
            if '-' in module_full_name:
                # 例如：说明书-变频器 -> 大模块：说明书，子模块：变频器
                main_module, sub_module = module_full_name.split('-', 1)
                main_module = re.sub(r'[<>:"/\\|?*]', '_', main_module.strip())
                sub_module = re.sub(r'[<>:"/\\|?*]', '_', sub_module.strip())
            else:
                # 只有一级模块
                main_module = re.sub(r'[<>:"/\\|?*]', '_', module_full_name)
                sub_module = None
            
            # 创建目录结构：大模块/子模块/
            if sub_module:
                module_dir = os.path.join(self.download_dir, main_module, sub_module)
            else:
                module_dir = os.path.join(self.download_dir, main_module)
            
            if not os.path.exists(module_dir):
                os.makedirs(module_dir)
            
            # 完整的文件路径
            filepath = os.path.join(module_dir, filename)
            
            # 如果文件已存在且大小合理，跳过下载
            if os.path.exists(filepath):
                file_size = os.path.getsize(filepath)
                if file_size > 1024:  # 大于1KB
                    print(f"        文件已存在，跳过: {filename}")
                    return True
            
            print(f"        正在下载: {filename}")
            
            # 为下载设置正确的请求头，包括Referer
            download_headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
                'Accept-Encoding': 'gzip, deflate, br',
                'Referer': 'https://www.cschueun.com/',  # 重要：设置Referer
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'cross-site',
                'Cache-Control': 'max-age=0'
            }
            
            # 使用requests下载文件
            response = self.session.get(url, headers=download_headers, stream=True, timeout=30)
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
                return False
            
            print(f"        下载完成: {filename} ({file_size} bytes)")
            
            # 记录新文件
            file_key = f"{file_info['module']}_{filename}"
            if file_key not in self.processed_files:
                # 构建显示用的模块路径
                if sub_module:
                    display_module = f"{main_module}/{sub_module}"
                else:
                    display_module = main_module
                    
                self.new_files.append({
                    'filename': filename,
                    'path': filepath,
                    'url': file_info['url'],
                    'size': file_size,
                    'module': display_module,
                    'title': file_info['title']
                })
                self.processed_files.add(file_key)
            
            return True
            
        except Exception as e:
            print(f"        下载失败 {url}: {e}")
            return False
    
    def download_all_files(self, all_files):
        """下载所有文件"""
        if not all_files:
            print("没有文件需要下载")
            return
        
        print(f"\n开始下载文件...")
        print(f"共有 {len(all_files)} 个文件需要处理")
        
        total_downloads = 0
        successful_downloads = 0
        
        for i, file_info in enumerate(all_files, 1):
            print(f"\n进度: {i}/{len(all_files)} - {file_info['module']} - {file_info['title']}")
            
            total_downloads += 1
            if self.download_file(file_info):
                successful_downloads += 1
            
            # 添加延迟避免请求过快
            time.sleep(1)
        
        print(f"\n文件下载完成！")
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
        self.log("🚀 开始爬取创安睿控下载中心...")
        
        # 1. 获取所有大模块
        modules = self.parse_main_page()
        if not modules:
            self.log("❌ 未找到任何模块")
            return
        
        self.log(f"📋 共找到 {len(modules)} 个大模块")
        
        # 2. 保存模块信息
        self.save_data(modules, 'modules.json')
        
        # 3. 爬取每个模块的文件
        all_files = []
        
        for i, module in enumerate(modules, 1):
            self.log(f"🔄 进度: {i}/{len(modules)} - {module['name']}")
            
            # 如果模块有子模块，爬取子模块
            if module['sub_modules']:
                for sub_module in module['sub_modules']:
                    sub_module_info = {
                        'name': f"{module['name']}-{sub_module['name']}",
                        'url': sub_module['url']
                    }
                    files = self.parse_module_page(sub_module_info)
                    all_files.extend(files)
                    time.sleep(1)
            else:
                # 直接爬取模块
                files = self.parse_module_page(module)
                all_files.extend(files)
            
            # 添加延迟避免请求过快
            time.sleep(2)
        
        # 4. 保存所有文件信息
        if all_files:
            self.save_data(all_files, 'files.json')
            self.log(f"✅ 爬取完成！共获取 {len(all_files)} 个文件")
        else:
            self.log("❌ 未找到任何文件信息")
            return
        
        # 5. 下载文件
        self.download_all_files(all_files)
        
        # 6. 保存处理记录
        self.save_processed_files()
        
        # 7. 发送钉钉通知
        self.send_completion_notification()
        
        # 8. 生成统计报告
        self.generate_report(modules, all_files)
    
    def send_completion_notification(self):
        """发送完成通知"""
        if not self.new_files:
            if not self.is_first_run:
                self.log("📢 无新文件，不发送通知")
            return
        
        # 构建通知消息
        message_parts = []
        message_parts.append(f"📊 创安睿控爬虫完成")
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
    
    def generate_report(self, modules, all_files):
        """生成爬取报告"""
        report = {
            '爬取时间': time.strftime('%Y-%m-%d %H:%M:%S'),
            '总模块数': len(modules),
            '总文件数': len(all_files),
            '模块列表': [m['name'] for m in modules],
            '各模块文件数量': {}
        }
        
        # 统计各模块文件数量
        for module in modules:
            module_files = [f for f in all_files if f['module'].startswith(module['name'])]
            report['各模块文件数量'][module['name']] = len(module_files)
        
        # 保存报告
        self.save_data(report, '爬取报告.json')
        
        # 打印报告
        print("\n" + "="*50)
        print("爬取报告")
        print("="*50)
        print(f"爬取时间: {report['爬取时间']}")
        print(f"总模块数: {report['总模块数']}")
        print(f"总文件数: {report['总文件数']}")
        print("\n各模块文件数量:")
        for module_name, count in report['各模块文件数量'].items():
            print(f"  {module_name}: {count} 个")
        print("="*50)

if __name__ == "__main__":
    spider = ChuangAnSpider()
    try:
        spider.run()
    except KeyboardInterrupt:
        print("\n用户中断爬取")
    except Exception as e:
        print(f"爬取过程中出现错误: {e}")
