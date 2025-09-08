#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
库卡下载中心爬虫
爬取库卡下载中心的所有资料文件
支持钉钉通知和自动检测新文件
新增Selenium滚动加载功能，确保获取完整数据
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
import argparse

# 新增Selenium相关导入
try:
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.chrome.options import Options
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False
    print("警告: Selenium未安装，将使用传统API方式")

class KukaSpider:
    def __init__(self):
        self.base_url = "https://www.kuka.cn"
        
        # 中文网站配置
        self.cn_config = {
            'main_url': "https://www.kuka.cn/zh-cn/services/downloads",
            'api_refinements_url': "https://www.kuka.cn/zh-cn/api/downloadcentersearch/Refinements",
            'api_results_url': "https://www.kuka.cn/zh-cn/api/downloadcentersearch/Results",
            'selenium_url': "https://www.kuka.cn/zh-cn/services/downloads",
            'language': 'zh-cn',
            'folder_name': '中文网站'
        }
        
        # 英文网站配置
        self.en_config = {
            'main_url': "https://www.kuka.com/en-de/services/downloads",
            'api_refinements_url': "https://www.kuka.com/en-de/api/downloadcentersearch/Refinements",
            'api_results_url': "https://www.kuka.com/en-de/api/downloadcentersearch/Results",
            'selenium_url': "https://www.kuka.com/en-de/downloads",
            'language': 'en-de',
            'folder_name': '英文网站'
        }
        
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/json, text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        
        # 服务器固定路径（按规范要求），本地测试使用当前目录
        if os.path.exists("/srv/downloads/approved/"):
            self.base_dir = "/srv/downloads/approved/库卡"
            self.output_dir = os.path.join(self.base_dir, "产品数据")
            self.download_dir = os.path.join(self.base_dir, "资料下载")
        else:
            # 本地测试环境
            self.base_dir = os.path.join(os.getcwd(), "库卡")
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
        
        # 跳过的分类（根据用户要求，但保留Brochures和DataSheets分类）
        self.skip_categories = ['软件', 'CAD', 'Software']
        
        # Selenium配置
        self.use_selenium = SELENIUM_AVAILABLE
        if not self.use_selenium:
            print("⚠️ Selenium不可用，将使用传统API方式")
    
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
                    "content": f"🤖 库卡爬虫通知\n{message}"
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
    
    def get_api_data(self, url, params=None, max_retries=3):
        """获取API数据，带重试机制"""
        for attempt in range(max_retries):
            try:
                response = self.session.get(url, params=params, timeout=15)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 502 and attempt < max_retries - 1:
                    print(f"获取API数据失败 {url}: {e} (尝试 {attempt + 1}/{max_retries})")
                    time.sleep(5 * (attempt + 1))  # 递增延迟
                    continue
                else:
                    print(f"获取API数据失败 {url}: {e}")
                    return None
            except Exception as e:
                if attempt < max_retries - 1:
                    print(f"获取API数据失败 {url}: {e} (尝试 {attempt + 1}/{max_retries})")
                    time.sleep(3 * (attempt + 1))
                    continue
                else:
                    print(f"获取API数据失败 {url}: {e}")
                    return None
        return None
    
    def get_categories(self, config):
        """获取指定网站的所有分类信息"""
        print(f"正在获取{config['folder_name']}分类信息...")
        
        # 获取分类数据
        data = self.get_api_data(config['api_refinements_url'])
        if not data:
            return []
        
        categories = []
        
        # 解析分类数据
        if 'refinements' in data:
            for refinement in data['refinements']:
                if refinement.get('termId') == 'Category':
                    for facet in refinement.get('facets', []):
                        term = facet.get('term', {})
                        category_name = term.get('label', '')
                        category_count = facet.get('count', 0)
                        
                        # 跳过软件和CAD分类
                        if category_name in self.skip_categories:
                            print(f"跳过分类: {category_name} ({category_count} 个文件)")
                            continue
                        
                        # 跳过外部链接（CAD）
                        if category_name == 'CAD':
                            print(f"跳过外部链接分类: {category_name}")
                            continue
                        
                        if category_count > 0:
                            categories.append({
                                'name': category_name,
                                'value_id': term.get('valueId', ''),
                                'count': category_count,
                                'term_id': 'Category',
                                'website': config['folder_name']
                            })
                            print(f"找到分类: {category_name} ({category_count} 个文件)")
        
        return categories
    
    def get_downloads_by_category_selenium(self, category, config):
        """使用Selenium滚动加载获取某个分类的所有下载文件"""
        if not self.use_selenium:
            self.log("Selenium不可用，回退到API方式")
            return self.get_all_downloads_by_category(category, config)
        
        self.log(f"使用Selenium获取{category['website']}分类 '{category['name']}' 的文件...")
        
        # 配置Chrome选项
        chrome_options = Options()
        chrome_options.add_argument('--headless')  # 无头模式
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument('--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        
        driver = None
        try:
            # 创建WebDriver
            driver = webdriver.Chrome(options=chrome_options)
            self.log("Chrome WebDriver创建成功")
            
            # 访问下载中心页面
            url = config['selenium_url']
            self.log(f"访问页面: {url}")
            driver.get(url)
            
            # 等待页面加载
            self.log("等待页面加载...")
            time.sleep(10)
            
            # 等待下载中心组件加载
            try:
                WebDriverWait(driver, 30).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "mod-downloadcenter"))
                )
                self.log("下载中心组件加载完成")
            except Exception as e:
                self.log(f"等待下载中心组件超时: {e}")
                return []
            
            # 查找并点击指定分类
            self.log(f"查找并点击分类: {category['name']}")
            try:
                # 查找分类链接
                category_links = driver.find_elements(By.CSS_SELECTOR, ".item__link__text")
                self.log(f"找到分类链接数量: {len(category_links)}")
                
                target_category = None
                for link in category_links:
                    if category['name'] in link.text:
                        target_category = link
                        self.log(f"找到目标分类链接: {link.text}")
                        break
                
                if target_category:
                    # 点击分类
                    self.log(f"点击分类: {category['name']}")
                    driver.execute_script("arguments[0].click();", target_category)
                    time.sleep(5)
                    
                    # 等待结果加载
                    try:
                        WebDriverWait(driver, 20).until(
                            EC.presence_of_element_located((By.CLASS_NAME, "mod-downloadcenter__results__item"))
                        )
                        self.log("分类结果加载完成")
                    except Exception as e:
                        self.log(f"等待分类结果超时: {e}")
                        return []
                    
                    # 滚动加载更多内容
                    self.log("开始滚动加载更多内容...")
                    all_files = []
                    previous_count = 0
                    scroll_attempts = 0
                    max_scroll_attempts = 50  # 最大滚动次数
                    
                    while scroll_attempts < max_scroll_attempts:
                        # 获取当前结果项数量
                        result_items = driver.find_elements(By.CLASS_NAME, "mod-downloadcenter__results__item")
                        current_count = len(result_items)
                        
                        self.log(f"滚动尝试 {scroll_attempts + 1}: 当前结果数量: {current_count}")
                        
                        if current_count > previous_count:
                            self.log(f"发现新内容！数量从 {previous_count} 增加到 {current_count}")
                            previous_count = current_count
                            scroll_attempts = 0  # 重置计数器
                        else:
                            scroll_attempts += 1
                        
                        # 滚动到页面底部
                        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                        time.sleep(2)
                        
                        # 再次滚动一点，确保触发加载
                        driver.execute_script("window.scrollTo(0, document.body.scrollHeight + 100);")
                        time.sleep(3)
                        
                        # 检查是否有加载指示器
                        try:
                            loading_indicators = driver.find_elements(By.CSS_SELECTOR, ".loading, .spinner, [class*='loading'], [class*='spinner']")
                            if loading_indicators:
                                self.log("检测到加载指示器，等待加载完成...")
                                time.sleep(5)
                        except:
                            pass
                        
                        # 如果连续多次没有新内容，可能已经加载完成
                        if scroll_attempts >= 10:
                            self.log("连续10次没有新内容，可能已加载完成")
                            break
                    
                    self.log(f"滚动加载完成，最终结果数量: {len(result_items)}")
                    
                    # 解析所有结果项
                    self.log("解析所有文件...")
                    for i, item in enumerate(result_items):
                        try:
                            # 获取标题
                            title_elem = item.find_element(By.CSS_SELECTOR, ".m-results__list-item-headline span")
                            title = title_elem.text
                            
                            # 获取分类
                            category_elem = item.find_element(By.CSS_SELECTOR, ".item__caption")
                            category_name = category_elem.text
                            
                            # 获取下载链接
                            link_elem = item.find_element(By.CSS_SELECTOR, "a")
                            download_url = link_elem.get_attribute("href")
                            
                            # 获取描述
                            try:
                                desc_elem = item.find_element(By.CSS_SELECTOR, ".copy span")
                                description = desc_elem.text
                            except:
                                description = ""
                            
                            # 获取修改时间
                            try:
                                modified_elem = item.find_element(By.CSS_SELECTOR, ".item__caption--modified")
                                modified = modified_elem.text
                            except:
                                modified = ""
                            
                            # 清理标题
                            clean_title = title
                            if title and ' - ' in title:
                                parts = title.split(' - ')
                                if len(parts) > 1:
                                    last_part = parts[-1]
                                    if any(ext in last_part.upper() for ext in ['.PDF', '.DOC', '.XLS', '.ZIP', 'KB', 'MB']):
                                        clean_title = ' - '.join(parts[:-1])
                            
                            file_info = {
                                'url': download_url,
                                'title': clean_title,
                                'original_title': title,
                                'description': description,
                                'filetype': '',
                                'filesize': '',
                                'category': category_name,
                                'modified': modified,
                                'page': 1,
                                'website': category['website']
                            }
                            
                            all_files.append(file_info)
                            
                        except Exception as e:
                            self.log(f"解析第{i+1}项时出错: {e}")
                            continue
                    
                    self.log(f"通过Selenium获取到 {len(all_files)} 个文件")
                    return all_files
                    
                else:
                    self.log(f"未找到分类: {category['name']}")
                    return []
                    
            except Exception as e:
                self.log(f"处理分类时出错: {e}")
                import traceback
                traceback.print_exc()
                return []
                
        except Exception as e:
            self.log(f"Selenium异常: {e}")
            import traceback
            traceback.print_exc()
            return []
            
        finally:
            if driver:
                try:
                    driver.quit()
                    self.log("WebDriver已关闭")
                except:
                    pass
    
    def get_downloads_by_category(self, category, config, page_number=1):
        """根据分类获取下载文件列表"""
        print(f"正在获取{category['website']}分类 '{category['name']}' 第 {page_number} 页的文件...")
        
        # 构建API参数，根据网站语言设置Language参数
        language_param = "zh:1" if config['language'] == 'zh-cn' else "en:1"
        params = {
            'searchTerm': '',
            'activeTerms': f"Language:{language_param},Category:{category['value_id']}",
            'pageNumber': page_number
        }
        
        data = self.get_api_data(config['api_results_url'], params)
        if not data:
            return [], False
        
        downloads = []
        
        # 解析下载数据
        if 'resultPage' in data:
            for result in data['resultPage']:
                # 只处理公开的文件（public=true）
                if not result.get('public', False):
                    continue
                
                download_url = result.get('downloadUrl', '')
                title = result.get('title', '')
                description = result.get('description', '')
                filetype = result.get('filetype', '')
                filesize = result.get('filesize', '')
                category_name = result.get('category', category['name'])
                modified = result.get('modified', '')
                
                # 清理文件名：去掉文件类型和大小信息
                clean_title = title
                if title and ' - ' in title:
                    # 移除 " - .PDF, 73 kB" 这样的后缀
                    parts = title.split(' - ')
                    if len(parts) > 1:
                        # 检查最后一部分是否包含文件类型和大小
                        last_part = parts[-1]
                        if any(ext in last_part.upper() for ext in ['.PDF', '.DOC', '.XLS', '.ZIP', 'KB', 'MB']):
                            clean_title = ' - '.join(parts[:-1])
                
                if download_url and title:
                    downloads.append({
                        'url': download_url,
                        'title': clean_title,
                        'original_title': title,
                        'description': description,
                        'filetype': filetype,
                        'filesize': filesize,
                        'category': category_name,
                        'modified': modified,
                        'page': page_number,
                        'website': category['website']
                    })
                    print(f"    找到文件: {clean_title}")
        
        # 检查是否有更多页面
        has_more = data.get('hasMore', False)
        total_count = data.get('totalCount', 0)
        current_count = len(data.get('resultPage', []))
        
        print(f"  第 {page_number} 页找到 {current_count} 个文件（总计: {total_count}）")
        
        return downloads, has_more
    
    def get_all_downloads_by_category(self, category, config):
        """获取某个分类的所有下载文件（支持分页）"""
        # 对于Brochures分类，优先使用Selenium滚动加载
        if category['name'] == 'Brochures' and self.use_selenium:
            self.log(f"对Brochures分类使用Selenium滚动加载...")
            return self.get_downloads_by_category_selenium(category, config)
        
        # 其他分类使用传统API方式
        all_downloads = []
        page = 1
        
        while True:
            downloads, has_more = self.get_downloads_by_category(category, config, page)
            all_downloads.extend(downloads)
            
            if not has_more or not downloads:
                break
            
            page += 1
            time.sleep(1)  # 添加延迟避免请求过快
        
        print(f"分类 '{category['name']}' 共找到 {len(all_downloads)} 个文件")
        return all_downloads
    
    def download_file(self, download_info):
        """下载文件"""
        try:
            url = download_info['url']
            filename = download_info['title']
            category = download_info['category']
            website = download_info.get('website', '中文网站')
            
            # 清理文件名，移除非法字符
            filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
            
            # 如果文件名为空或只有空格，设置默认名称
            if not filename or not filename.strip():
                filename = "库卡资料"
            
            # 从原始标题或URL中获取文件扩展名
            ext = ""
            original_title = download_info.get('original_title', '')
            filetype = download_info.get('filetype', '')
            
            # 优先从filetype获取扩展名
            if filetype:
                if filetype.startswith('.'):
                    ext = filetype.lower()
                else:
                    ext = f'.{filetype.lower()}'
            else:
                # 从URL获取扩展名
                parsed_url = urlparse(url)
                path = parsed_url.path
                if '.' in path:
                    url_ext = '.' + path.split('.')[-1].lower()
                    if url_ext in ['.pdf', '.doc', '.docx', '.xls', '.xlsx', '.zip', '.rar', '.exe', '.dmg', '.msi', '.deb', '.rpm', '.tar', '.gz', '.7z', '.txt', '.rtf', '.ppt', '.pptx', '.csv', '.xml', '.json', '.html', '.htm']:
                        ext = url_ext
            
            # 如果还是没有扩展名，默认为PDF
            if not ext:
                ext = '.pdf'
            
            # 确保文件名有扩展名
            if not filename.lower().endswith(ext):
                filename += ext
            
            # 清理分类名中的非法字符
            category_clean = re.sub(r'[<>:"/\\|?*]', '_', category)
            
            # 创建目录结构: 网站/分类/文件
            website_dir = os.path.join(self.download_dir, website)
            category_dir = os.path.join(website_dir, category_clean)
            os.makedirs(category_dir, exist_ok=True)
            
            # 完整的文件路径
            filepath = os.path.join(category_dir, filename)
            
            # 如果文件已存在，跳过下载
            if os.path.exists(filepath):
                print(f"        文件已存在，跳过: {filename}")
                return True
            
            print(f"        正在下载: {filename}")
            
            # 使用requests下载文件
            response = self.session.get(url, stream=True, timeout=120)  # 增加到120秒适应大文件
            response.raise_for_status()
            
            # 检查文件大小
            content_length = response.headers.get('content-length')
            if content_length:
                size_mb = int(content_length) / (1024 * 1024)
                if size_mb > 50:  # 大于50MB的文件给出提示
                    print(f"        文件较大 ({size_mb:.1f} MB)，下载可能需要较长时间...")
            
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
            file_key = f"{website}_{category}_{filename}"
            if file_key not in self.processed_files:
                self.new_files.append({
                    'filename': filename,
                    'path': filepath,
                    'url': url,
                    'size': file_size,
                    'category': category,
                    'description': download_info.get('description', ''),
                    'modified': download_info.get('modified', '')
                })
                self.processed_files.add(file_key)
            
            return True
            
        except Exception as e:
            print(f"        下载失败 {url}: {e}")
            return False
    
    def download_materials(self, downloads, skip_download=False):
        """下载所有文件"""
        if not downloads:
            print("没有文件需要下载")
            return
        
        if skip_download:
            print("跳过下载模式，只显示文件信息")
            for download in downloads:
                print(f"文件: {download['title']} - {download['category']}")
            return
        
        print(f"\n开始下载文件...")
        print(f"共有 {len(downloads)} 个文件需要处理")
        
        total_downloads = 0
        successful_downloads = 0
        
        for i, download in enumerate(downloads, 1):
            print(f"\n进度: {i}/{len(downloads)} - {download['category']} - {download['title']}")
            
            total_downloads += 1
            if self.download_file(download):
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
    
    def run(self, limit=None, skip_download=False, target_categories=None, target_websites=None):
        """运行爬虫"""
        self.log("🚀 开始爬取库卡下载中心...")
        
        # 确定要爬取的网站
        websites_to_crawl = []
        if target_websites:
            if '中文' in target_websites or 'cn' in target_websites:
                websites_to_crawl.append(self.cn_config)
            if '英文' in target_websites or 'en' in target_websites:
                websites_to_crawl.append(self.en_config)
        else:
            # 默认爬取两个网站
            websites_to_crawl = [self.cn_config, self.en_config]
        
        all_categories = []
        
        # 1. 获取所有网站的分类
        for config in websites_to_crawl:
            categories = self.get_categories(config)
            if categories:
                all_categories.extend(categories)
            else:
                self.log(f"❌ {config['folder_name']}未找到任何分类")
        
        if not all_categories:
            self.log("❌ 未找到任何分类")
            return
        
        # 如果指定了特定分类，只处理这些分类
        if target_categories:
            filtered_categories = []
            for cat in all_categories:
                if cat['name'] in target_categories:
                    filtered_categories.append(cat)
            all_categories = filtered_categories
            if not all_categories:
                self.log(f"❌ 未找到指定的分类: {target_categories}")
                return
            self.log(f"📋 筛选后共 {len(all_categories)} 个分类: {[cat['website'] + '-' + cat['name'] for cat in all_categories]}")
        else:
            self.log(f"📋 共找到 {len(all_categories)} 个分类")
        
        # 2. 保存分类信息
        self.save_data(all_categories, 'categories.json')
        
        # 3. 爬取每个分类的文件
        all_downloads = []
        for i, category in enumerate(all_categories, 1):
            self.log(f"🔄 进度: {i}/{len(all_categories)} - {category['website']}-{category['name']}")
            
            # 找到对应的网站配置
            config = self.cn_config if category['website'] == '中文网站' else self.en_config
            downloads = self.get_all_downloads_by_category(category, config)
            all_downloads.extend(downloads)
            
            # 如果设置了限制，检查是否达到限制
            if limit and len(all_downloads) >= limit:
                all_downloads = all_downloads[:limit]
                self.log(f"⚠️ 达到限制数量 {limit}，停止爬取")
                break
            
            # 添加延迟避免请求过快
            time.sleep(2)
        
        # 4. 保存所有文件信息
        if all_downloads:
            self.save_data(all_downloads, 'downloads.json')
            self.log(f"✅ 爬取完成！共获取 {len(all_downloads)} 个文件")
        else:
            self.log("❌ 未找到任何文件")
            return
        
        # 5. 下载文件
        self.download_materials(all_downloads, skip_download)
        
        # 6. 保存处理记录
        if not skip_download:
            self.save_processed_files()
        
        # 7. 发送钉钉通知
        if not skip_download:
            self.send_completion_notification()
        
        # 8. 生成统计报告
        self.generate_report(all_categories, all_downloads)
    
    def send_completion_notification(self):
        """发送完成通知"""
        if not self.new_files:
            if not self.is_first_run:
                self.log("📢 无新文件，不发送通知")
            return
        
        # 构建通知消息
        message_parts = []
        message_parts.append(f"📊 库卡爬虫完成")
        message_parts.append(f"🕒 时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        message_parts.append(f"📁 新下载文件: {len(self.new_files)} 个")
        
        if self.is_first_run:
            message_parts.append("🆕 首次运行，已建立基线")
        
        # 按分类分组显示新文件
        category_files = {}
        for file_info in self.new_files:
            category = file_info['category']
            if category not in category_files:
                category_files[category] = []
            category_files[category].append(file_info)
        
        message_parts.append("\n📋 新文件详情:")
        for category, files in category_files.items():
            message_parts.append(f"  📂 {category}: {len(files)} 个文件")
            for file_info in files[:3]:  # 只显示前3个
                size_mb = file_info['size'] / 1024 / 1024
                message_parts.append(f"    📄 {file_info['filename']} ({size_mb:.1f}MB)")
            if len(files) > 3:
                message_parts.append(f"    ... 还有 {len(files) - 3} 个文件")
        
        message = "\n".join(message_parts)
        self.send_dingtalk_notification(message)
    
    def generate_report(self, categories, downloads):
        """生成爬取报告"""
        report = {
            '爬取时间': time.strftime('%Y-%m-%d %H:%M:%S'),
            '总分类数': len(categories),
            '总文件数': len(downloads),
            '分类列表': [c['name'] for c in categories],
            '各分类文件数量': {},
            '跳过的分类': self.skip_categories,
            '使用Selenium': self.use_selenium
        }
        
        # 统计各分类文件数量
        for category in categories:
            category_downloads = [d for d in downloads if d['category'] == category['name']]
            report['各分类文件数量'][category['name']] = len(category_downloads)
        
        # 保存报告
        self.save_data(report, '爬取报告.json')
        
        # 打印报告
        print("\n" + "="*50)
        print("库卡下载中心爬取报告")
        print("="*50)
        print(f"爬取时间: {report['爬取时间']}")
        print(f"总分类数: {report['总分类数']}")
        print(f"总文件数: {report['总文件数']}")
        print(f"跳过分类: {', '.join(report['跳过的分类'])}")
        print(f"使用Selenium: {'是' if report['使用Selenium'] else '否'}")
        print("\n各分类文件数量:")
        for category_name, count in report['各分类文件数量'].items():
            print(f"  {category_name}: {count} 个")
        
        print("="*50)
    
    def download_materials_only(self):
        """只下载文件，不重新爬取文件信息"""
        print("开始下载文件（使用现有文件数据）...")
        
        # 加载现有文件数据
        downloads_file = os.path.join(self.output_dir, 'downloads.json')
        if not os.path.exists(downloads_file):
            print("未找到文件数据，请先运行完整爬取")
            return
        
        try:
            with open(downloads_file, 'r', encoding='utf-8') as f:
                downloads = json.load(f)
            
            print(f"加载了 {len(downloads)} 个文件信息")
            self.download_materials(downloads)
            
        except Exception as e:
            print(f"加载文件数据失败: {e}")
    
    def get_manuals(self, config):
        """获取手册文件（包括Brochures、手册等分类）"""
        self.log(f"开始获取手册文件...")
        
        # 获取所有分类
        categories = self.get_categories(config)
        
        # 定义手册相关的分类名称
        manual_keywords = ['手册', 'Brochures', 'brochure', 'manual', 'Manual', 'Manuals']
        
        manual_downloads = []
        
        for category in categories:
            category_name = category['name']
            
            # 检查分类名称是否包含手册相关关键词
            is_manual_category = any(keyword in category_name for keyword in manual_keywords)
            
            if is_manual_category:
                self.log(f"找到手册分类: {category_name} ({category['count']} 个文件)")
                
                # 获取该分类的所有文件
                downloads = self.get_all_downloads_by_category(category, config)
                
                # 为每个文件添加分类信息
                for download in downloads:
                    download['category'] = category_name
                
                manual_downloads.extend(downloads)
                self.log(f"从分类 '{category_name}' 获取到 {len(downloads)} 个文件")
        
        self.log(f"总共获取到 {len(manual_downloads)} 个手册文件")
        return manual_downloads

if __name__ == "__main__":
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='库卡下载中心爬虫')
    parser.add_argument('--download-only', action='store_true', help='只下载文件，不重新爬取')
    parser.add_argument('--limit', type=int, help='限制爬取的文件数量（用于测试）')
    parser.add_argument('--skip-download', action='store_true', help='跳过下载，只爬取文件信息')
    parser.add_argument('--categories', nargs='+', help='指定要爬取的分类名称（可多个）')
    parser.add_argument('--websites', nargs='+', choices=['中文', '英文', 'cn', 'en'], help='指定要爬取的网站（中文/英文）')
    parser.add_argument('--no-selenium', action='store_true', help='禁用Selenium，使用传统API方式')
    
    args = parser.parse_args()
    
    spider = KukaSpider()
    
    # 如果指定了禁用Selenium
    if args.no_selenium:
        spider.use_selenium = False
        print("已禁用Selenium，将使用传统API方式")
    
    try:
        if args.download_only:
            spider.download_materials_only()
        else:
            spider.run(limit=args.limit, skip_download=args.skip_download, target_categories=args.categories, target_websites=args.websites)
    except KeyboardInterrupt:
        print("\n用户中断爬取")
    except Exception as e:
        print(f"爬取过程中出现错误: {e}")
        import traceback
        traceback.print_exc()
