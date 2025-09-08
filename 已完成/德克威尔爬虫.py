#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import json
import pickle
import platform
import re
import requests
from datetime import datetime, date
from pathlib import Path
from urllib.parse import urljoin, urlparse, unquote, parse_qs
from bs4 import BeautifulSoup
import hashlib

class DexwellSpider:
    def __init__(self):
        # 基础配置
        self.base_url = "https://www.welllinkio.com"
        self.download_url = "https://www.welllinkio.com/download"
        
        # 服务器固定路径（按规范要求），本地测试使用当前目录
        if os.path.exists("/srv/downloads/approved/"):
            self.base_dir = "/srv/downloads/approved/德克威尔"
        else:
            # 本地测试环境
            self.base_dir = os.path.join(os.getcwd(), "downloads", "德克威尔")
        
        # 确保目录存在
        os.makedirs(self.base_dir, exist_ok=True)
        
        # 钉钉通知配置
        self.dingtalk_webhook = os.getenv('DINGTALK_WEBHOOK', '')
        
        # 数据存储
        self.processed_files = self.load_processed_files()
        self.module_structure = self.load_module_structure()
        self.new_files = []
        self.new_modules = []
        
        # 时间过滤
        self.filter_date = datetime(2024, 11, 1).date()
        
        # 请求头
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
        
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        
        # 初始化页面处理状态
        self._processed_pages = set()
        
        # 钉钉通知配置
        self.ACCESS_TOKEN = "1a431b6482e59ec652a6eab37705d50fd282dc426f0b829b3eb9a9c1b277ed24"
        self.SECRET = "SECc5e2168011dd97d4c511e06832d172f31635ef635771668e4e6003fce23c07bb"
        
        # 服务器保存地址
        self.server_save_path = "/srv/downloads/approved/德克威尔"
        
    def clean_folder_name(self, name):
        """清理文件夹名称，替换特殊字符"""
        if not name:
            return "未知"
        
        # 替换斜杠为下划线
        cleaned = name.replace('/', '_').replace('\\', '_')
        
        # 替换其他可能的特殊字符
        cleaned = re.sub(r'[<>:"|?*]', '_', cleaned)
        
        # 清理多余的下划线
        cleaned = re.sub(r'_+', '_', cleaned)
        
        # 去除首尾下划线
        cleaned = cleaned.strip('_')
        
        return cleaned if cleaned else "未知"

    def log(self, message):
        """日志记录"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{timestamp}] {message}")
    
    def load_processed_files(self):
        """加载已处理的文件列表"""
        processed_file = os.path.join(self.base_dir, 'processed_files.pkl')
        if os.path.exists(processed_file):
            try:
                with open(processed_file, 'rb') as f:
                    return pickle.load(f)
            except:
                return set()
        return set()
    
    def save_processed_files(self):
        """保存已处理的文件列表"""
        processed_file = os.path.join(self.base_dir, 'processed_files.pkl')
        with open(processed_file, 'wb') as f:
            pickle.dump(self.processed_files, f)
    
    def load_module_structure(self):
        """加载模块结构"""
        structure_file = os.path.join(self.base_dir, 'module_structure.json')
        if os.path.exists(structure_file):
            try:
                with open(structure_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except:
                return {}
        return {}
    
    def save_module_structure(self):
        """保存模块结构"""
        structure_file = os.path.join(self.base_dir, 'module_structure.json')
        with open(structure_file, 'w', encoding='utf-8') as f:
            json.dump(self.module_structure, f, ensure_ascii=False, indent=2)
    
    def get_page_content(self, url, retry_count=3):
        """获取页面内容"""
        for attempt in range(retry_count):
            try:
                self.log(f"🔄 访问页面 (尝试{attempt+1}/{retry_count}): {url}")
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                response.encoding = 'utf-8'
                return response.text
            except Exception as e:
                self.log(f"❌ 页面访问失败 (尝试{attempt+1}): {str(e)}")
                if attempt < retry_count - 1:
                    time.sleep(5)
        
        self.log(f"❌ 页面访问完全失败: {url}")
        return None
    
    def scan_module_structure(self):
        """扫描模块结构"""
        self.log("🔍 开始扫描模块结构")
        
        try:
            # 获取主页面
            page_content = self.get_page_content(self.download_url)
            if not page_content:
                return {}
            
            soup = BeautifulSoup(page_content, 'html.parser')
            
            # 查找左侧模块导航
            left_nav = soup.find('div', class_='newleft')
            if not left_nav:
                self.log("❌ 未找到左侧导航")
                return {}
            
            # 查找所有模块
            modules = left_nav.find_all('li', class_='cur')
            current_structure = {}
            
            for module in modules:
                # 获取模块名称
                module_header = module.find('h4')
                if not module_header:
                    continue
                
                module_name = module_header.get_text().strip()
                self.log(f"📋 发现模块: {module_name}")
                
                # 查找子模块
                sub_modules = module.find('div', class_='newtoolsnav')
                if sub_modules:
                    sub_module_links = sub_modules.find_all('a')
                    sub_modules_list = []
                    
                    for link in sub_module_links:
                        onclick = link.get('onclick', '')
                        checkbox = link.find('input')
                        if checkbox and onclick:
                            # 提取子模块名称
                            sub_name = link.get_text().strip()
                            # 提取URL参数
                            url_params = self.extract_url_params(onclick)
                            
                            sub_modules_list.append({
                                'name': sub_name,
                                'onclick': onclick,
                                'url_params': url_params
                            })
                            self.log(f"   📎 子模块: {sub_name}")
                    
                    current_structure[module_name] = sub_modules_list
            
            # 检查是否有新模块
            self.check_new_modules(current_structure)
            
            # 保存当前结构
            self.module_structure = current_structure
            self.save_module_structure()
            
            return current_structure
            
        except Exception as e:
            self.log(f"❌ 扫描模块结构失败: {str(e)}")
            return {}
    
    def extract_url_params(self, onclick):
        """从onclick中提取URL参数"""
        try:
            # 提取routerWay函数中的参数
            match = re.search(r"routerWay\('([^']+)'\)", onclick)
            if match:
                url = match.group(1)
                if url.startswith('/download'):
                    # 解析查询参数
                    parsed = urlparse(url)
                    params = parse_qs(parsed.query)
                    return params
            return {}
        except:
            return {}
    
    def check_new_modules(self, current_structure):
        """检查新模块"""
        if not self.module_structure:
            # 首次运行，记录所有模块
            self.log("🎉 首次运行，记录所有模块")
            return
        
        for module_name, sub_modules in current_structure.items():
            if module_name not in self.module_structure:
                self.log(f"🆕 发现新模块: {module_name}")
                self.new_modules.append(module_name)
                self.notify_dingtalk(f"发现新模块: {module_name}")
            
            # 检查子模块
            if module_name in self.module_structure:
                existing_sub_names = {sm['name'] for sm in self.module_structure[module_name]}
                current_sub_names = {sm['name'] for sm in sub_modules}
                
                new_sub_modules = current_sub_names - existing_sub_names
                if new_sub_modules:
                    for new_sub in new_sub_modules:
                        self.log(f"🆕 发现新子模块: {module_name} -> {new_sub}")
                        self.new_modules.append(f"{module_name} -> {new_sub}")
                        self.notify_dingtalk(f"发现新子模块: {module_name} -> {new_sub}")
    
    def notify_dingtalk(self, message):
        """钉钉通知"""
        if not self.dingtalk_webhook:
            self.log(f"📢 钉钉通知: {message}")
            return
        
        try:
            data = {
                "msgtype": "text",
                "text": {
                    "content": f"德克威尔爬虫通知: {message}"
                }
            }
            
            response = requests.post(self.dingtalk_webhook, json=data, timeout=10)
            if response.status_code == 200:
                self.log(f"✅ 钉钉通知发送成功: {message}")
            else:
                self.log(f"❌ 钉钉通知发送失败: {response.status_code}")
        except Exception as e:
            self.log(f"❌ 钉钉通知异常: {str(e)}")
    
    def should_download_file(self, title, publish_date, module_name):
        """判断是否应该下载文件"""
        # 产品手册需要时间过滤（只下载2024.11.1之后的文件）
        if module_name in ["选型手册", "使用手册", "宣传手册"]:
            try:
                # 解析日期
                if publish_date:
                    file_date = datetime.strptime(publish_date.strip(), "%Y-%m-%d").date()
                    if file_date < self.filter_date:
                        self.log(f"⏭️ 跳过过期文件: {title} ({publish_date})")
                        return False
            except:
                # 日期解析失败，默认下载
                pass
        
        # 其他模块（配置文件、图纸、软件与调试工具）全部下载
        return True
    
    def download_file(self, url, filename, folder_path):
        """下载文件"""
        try:
            # 创建目录
            os.makedirs(folder_path, exist_ok=True)
            file_path = os.path.join(folder_path, filename)
            
            # 检查文件是否已存在
            if os.path.exists(file_path):
                file_size = os.path.getsize(file_path)
                if file_size > 1024:  # 大于1KB认为有效
                    self.log(f"📁 文件已存在，跳过: {filename}")
                    return True
                else:
                    self.log(f"🔄 文件存在但大小异常，重新下载: {filename}")
                    os.remove(file_path)
            
            # 下载文件
            response = self.session.get(url, stream=True, timeout=30)
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
    
    def process_download_page(self, url, module_name, sub_module_name):
        """处理下载页面"""
        # 检查页面是否已处理
        if not hasattr(self, '_processed_pages'):
            self._processed_pages = set()
        
        if url in self._processed_pages:
            self.log(f"⏭️ 页面已处理，跳过: {url}")
            return
        
        self.log(f"📄 处理下载页面: {sub_module_name}")
        self._processed_pages.add(url)
        
        try:
            page_content = self.get_page_content(url)
            if not page_content:
                return
            
            soup = BeautifulSoup(page_content, 'html.parser')
            
            # 查找下载列表
            download_list = soup.find('div', class_='dllist')
            if not download_list:
                self.log(f"❌ 未找到下载列表: {url}")
                return
            
            # 查找所有下载项
            download_items = download_list.find_all('div', class_='list')
            self.log(f"🔍 找到 {len(download_items)} 个下载项")
            
            # 创建模块目录
            clean_module_name = self.clean_folder_name(module_name)
            clean_sub_module_name = self.clean_folder_name(sub_module_name)
            module_dir = os.path.join(self.base_dir, clean_module_name, clean_sub_module_name)
            
            for item in download_items:
                try:
                    # 获取文件信息
                    title_element = item.find('h3')
                    if not title_element:
                        continue
                    
                    title = title_element.get_text().strip()
                    
                    # 获取发布日期
                    date_element = item.find('p')
                    publish_date = date_element.get_text().strip() if date_element else ""
                    
                    # 获取下载链接
                    download_link = item.find('a', target='_blank')
                    if not download_link:
                        continue
                    
                    file_url = download_link.get('href', '')
                    if not file_url:
                        continue
                    
                    # 判断是否应该下载
                    if not self.should_download_file(title, publish_date, sub_module_name):
                        continue
                    
                    # 生成文件名
                    filename = self.generate_filename(title, file_url)
                    
                    # 检查是否已处理
                    file_hash = hashlib.md5(f"{file_url}_{title}".encode()).hexdigest()
                    if file_hash in self.processed_files:
                        self.log(f"⏭️ 文件已处理，跳过: {title}")
                        continue
                    
                    # 下载文件
                    if self.download_file(file_url, filename, module_dir):
                        self.processed_files.add(file_hash)
                    
                    time.sleep(1)  # 下载间隔
                    
                except Exception as e:
                    self.log(f"❌ 处理下载项时出错: {str(e)}")
                    continue
            
            # 检查是否有分页
            self.check_pagination(soup, url, module_name, sub_module_name)
            
        except Exception as e:
            self.log(f"❌ 处理下载页面失败: {str(e)}")
    
    def check_pagination(self, soup, current_url, module_name, sub_module_name):
        """检查分页"""
        try:
            pagination = soup.find('div', class_='pagee')
            if not pagination:
                return
            
            # 查找所有页码链接
            page_links = pagination.find_all('a')
            if not page_links:
                return
            
            # 获取当前页码
            current_page = 1
            if 'page=' in current_url:
                page_match = re.search(r'page=(\d+)', current_url)
                if page_match:
                    current_page = int(page_match.group(1))
            
            # 记录已处理的页面，避免重复
            if not hasattr(self, '_processed_pages'):
                self._processed_pages = set()
            
            # 收集所有分页URL，避免递归调用
            page_urls = []
            for link in page_links:
                href = link.get('href', '')
                if href and 'page=' in href:
                    # 构建完整URL
                    if href.startswith('/'):
                        page_url = urljoin(self.base_url, href)
                    else:
                        page_url = urljoin(current_url, href)
                    
                    # 避免重复处理当前页和已处理的页面
                    if page_url != current_url and page_url not in self._processed_pages:
                        page_urls.append(page_url)
            
            # 批量处理分页，避免递归
            for page_url in page_urls:
                if page_url not in self._processed_pages:
                    self.log(f"📄 发现分页: {page_url}")
                    self._processed_pages.add(page_url)
                    # 使用延迟处理，避免递归调用
                    self.process_download_page_delayed(page_url, module_name, sub_module_name)
                    time.sleep(2)  # 页面间延迟
            
        except Exception as e:
            self.log(f"❌ 检查分页时出错: {str(e)}")
    
    def process_download_page_delayed(self, url, module_name, sub_module_name):
        """延迟处理下载页面，避免递归"""
        try:
            # 直接处理页面，不检查分页
            page_content = self.get_page_content(url)
            if not page_content:
                return
            
            soup = BeautifulSoup(page_content, 'html.parser')
            
            # 查找下载列表
            download_list = soup.find('div', class_='dllist')
            if not download_list:
                self.log(f"❌ 未找到下载列表: {url}")
                return
            
            # 查找所有下载项
            download_items = download_list.find_all('div', class_='list')
            self.log(f"🔍 找到 {len(download_items)} 个下载项")
            
            # 创建模块目录
            clean_module_name = self.clean_folder_name(module_name)
            clean_sub_module_name = self.clean_folder_name(sub_module_name)
            module_dir = os.path.join(self.base_dir, clean_module_name, clean_sub_module_name)
            
            for item in download_items:
                try:
                    # 获取文件信息
                    title_element = item.find('h3')
                    if not title_element:
                        continue
                    
                    title = title_element.get_text().strip()
                    
                    # 获取发布日期
                    date_element = item.find('p')
                    publish_date = date_element.get_text().strip() if date_element else ""
                    
                    # 获取下载链接
                    download_link = item.find('a', target='_blank')
                    if not download_link:
                        continue
                    
                    file_url = download_link.get('href', '')
                    if not file_url:
                        continue
                    
                    # 判断是否应该下载
                    if not self.should_download_file(title, publish_date, sub_module_name):
                        continue
                    
                    # 生成文件名
                    filename = self.generate_filename(title, file_url)
                    
                    # 检查是否已处理
                    file_hash = hashlib.md5(f"{file_url}_{title}".encode()).hexdigest()
                    if file_hash in self.processed_files:
                        self.log(f"⏭️ 文件已处理，跳过: {title}")
                        continue
                    
                    # 下载文件
                    if self.download_file(file_url, filename, module_dir):
                        self.processed_files.add(file_hash)
                    
                    time.sleep(1)  # 下载间隔
                    
                except Exception as e:
                    self.log(f"❌ 处理下载项时出错: {str(e)}")
                    continue
            
        except Exception as e:
            self.log(f"❌ 延迟处理下载页面失败: {str(e)}")
    
    def generate_filename(self, title, url):
        """生成文件名"""
        try:
            # 清理标题，将斜杠替换为下划线
            clean_title = re.sub(r'[^\w\s\-\u4e00-\u9fff]', '', title)
            clean_title = clean_title.replace('/', '_').replace('\\', '_')  # 替换斜杠
            clean_title = re.sub(r'\s+', '_', clean_title.strip())
            
            # 从URL获取文件扩展名
            parsed_url = urlparse(url)
            path = parsed_url.path
            filename = os.path.basename(path)
            
            if filename and '.' in filename:
                ext = os.path.splitext(filename)[1]
                return f"{clean_title}{ext}"
            else:
                # 根据URL判断扩展名
                if '.pdf' in url.lower():
                    return f"{clean_title}.pdf"
                elif '.zip' in url.lower():
                    return f"{clean_title}.zip"
                elif '.exe' in url.lower():
                    return f"{clean_title}.exe"
                else:
                    return f"{clean_title}.pdf"  # 默认PDF
            
        except Exception as e:
            self.log(f"⚠️ 文件名生成失败: {str(e)}")
            return f"document_{int(time.time())}.pdf"
    
    def crawl_all_modules(self):
        """爬取所有模块"""
        self.log("🚀 开始爬取所有模块")
        
        try:
            # 扫描模块结构
            module_structure = self.scan_module_structure()
            
            if not module_structure:
                self.log("❌ 未找到任何模块")
                return
            
            # 爬取每个模块
            for module_name, sub_modules in module_structure.items():
                self.log(f"📋 处理模块: {module_name}")
                
                for sub_module in sub_modules:
                    sub_name = sub_module['name']
                    onclick = sub_module['onclick']
                    
                    self.log(f"🔄 处理子模块: {sub_name}")
                    
                    # 构建URL
                    if onclick:
                        # 从onclick中提取URL
                        url_match = re.search(r"routerWay\('([^']+)'\)", onclick)
                        if url_match:
                            url_path = url_match.group(1)
                            if url_path.startswith('/download'):
                                full_url = urljoin(self.base_url, url_path)
                                self.process_download_page(full_url, module_name, sub_name)
                                time.sleep(3)  # 模块间延迟
                            elif url_path.startswith('http'):
                                # 外部链接（如3D数据库）
                                self.log(f"🔗 外部链接，跳过: {url_path}")
                            else:
                                self.log(f"⚠️ 未知链接格式: {url_path}")
                    
                    time.sleep(2)  # 子模块间延迟
            
            # 保存进度
            self.save_processed_files()
            
            # 统计结果
            total_files = len(self.new_files)
            self.log(f"🎉 爬取完成！共下载 {total_files} 个新文件")
            
            if self.new_files:
                self.log("📁 新下载的文件:")
                for file_info in self.new_files[:10]:  # 显示前10个
                    self.log(f"   📄 {file_info['filename']} ({file_info['size']} bytes)")
                
                if len(self.new_files) > 10:
                    self.log(f"   ... 还有 {len(self.new_files) - 10} 个文件")
            
            # 发送完成通知
            if self.new_files or self.new_modules:
                summary = f"爬取完成！新文件: {len(self.new_files)}个, 新模块: {len(self.new_modules)}个"
                self.notify_dingtalk(summary)
            
        except Exception as e:
            self.log(f"❌ 爬取过程出错: {str(e)}")
            self.notify_dingtalk(f"爬取出错: {str(e)}")
    
    def run(self):
        """运行爬虫"""
        try:
            self.log("🚀 开始运行德克威尔文档爬虫")
            self.crawl_all_modules()
        except Exception as e:
            self.log(f"❌ 爬虫运行出错: {str(e)}")
        finally:
            self.session.close()

def test_single_module():
    """测试单个模块"""
    spider = DexwellSpider()
    
    try:
        # 测试选型手册
        test_url = "https://www.welllinkio.com/download?cpsc=%E9%80%89%E5%9E%8B%E6%89%8B%E5%86%8C"
        spider.process_download_page(test_url, "产品手册", "选型手册")
        
    except Exception as e:
        spider.log(f"❌ 测试失败: {str(e)}")
    finally:
        spider.session.close()

if __name__ == "__main__":
    import sys
    
    # 检查是否是测试模式
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        # 测试模式
        test_single_module()
    else:
        # 正常运行
        spider = DexwellSpider()
        spider.run()
