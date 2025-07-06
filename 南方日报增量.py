#!/usr/bin/env python
# -*- coding: UTF-8 -*-
import os
import json
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import pandas as pd
from bs4 import BeautifulSoup
import random
import schedule  # 用于定时任务
import requests
import re

class NanfangDailySpider:
    def __init__(self):
        # 基础配置
        self.headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Connection': 'keep-alive'
        }
        
        # 基础URL和目录
        self.base_url = "https://epaper.nfnews.com/nfdaily/html"
        self.base_dir = "南方日报"
        
        # 爬虫配置
        self.max_workers = 5
        self.request_delay = 1
        
        # 记录文件
        self.record_file = "crawled_articles.json"
        self.crawled_records = self.load_crawled_records()
        
        # 创建基础目录
        os.makedirs(self.base_dir, exist_ok=True)

    def clean_filename(self, filename):
        """清理文件名中的非法字符"""
        return re.sub(r'[\\/:*?"<>|]', '_', filename)

    def create_article_dir(self, date_str, version_code, version_name, title):
        """创建文章目录结构"""
        # 日期目录
        date_dir = os.path.join(self.base_dir, date_str)
        
        # 版面目录
        version_dir = os.path.join(date_dir, f"{version_code}_{version_name}")
        
        # 文章目录
        article_dir = os.path.join(version_dir, self.clean_filename(title))
        
        # 创建目录
        os.makedirs(article_dir, exist_ok=True)
        
        return article_dir

    def process_version(self, version, date_str):
        """处理单个版面"""
        try:
            print(f"\n└── 版面: {version['code']}_{version['name']}")
            
            formatted_date = f"{date_str[:6]}/{date_str[6:]}"
            
            resp = self.safe_request(version['url'])
            if not resp:
                return
            
            resp.encoding = 'utf-8'
            
            soup = BeautifulSoup(resp.text, 'html.parser')
            
            # 获取版面上的所有文章链接
            articles = []
            for link in soup.find_all('a', href=True):
                href = link.get('href', '')
                if 'content_' in href and href.endswith('.html'):
                    if href.startswith('http'):
                        article_url = href
                    else:
                        article_url = f"https://epaper.nfnews.com/nfdaily/html/{formatted_date}/{href}"
                    articles.append(article_url)
            
            # 处理每篇文章
            for article_url in articles:
                try:
                    self.process_article(article_url, date_str, version['code'], version['name'])
                    time.sleep(self.request_delay)
                except Exception as e:
                    print(f"    ├── ❌ 处理文章失败: {article_url}")
                    print(f"    └── 错误: {str(e)}")
            
        except Exception as e:
            print(f"└── ❌ 处理版面失败: {version['url']}")
            print(f"    └── 错误: {str(e)}")

    def crawl_today(self):
        """爬取今天的新闻"""
        today = datetime.now().strftime('%Y-%m-%d')
        print(f"\n开始爬取 {today} 的新闻...")
        self.run(today, today)

    def load_crawled_records(self):
        """加载已爬取的文章记录"""
        try:
            if os.path.exists(self.record_file):
                with open(self.record_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            return {}
        except Exception as e:
            print(f"加载爬取记录失败: {str(e)}")
            return {}

    def save_crawled_records(self):
        """保存已爬取的文章记录"""
        try:
            with open(self.record_file, 'w', encoding='utf-8') as f:
                json.dump(self.crawled_records, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"保存爬取记录失败: {str(e)}")

    def is_article_crawled(self, date_str, article_url, title):
        """检查文章是否已经爬取过"""
        article_key = f"{date_str}_{article_url}_{title}"
        return article_key in self.crawled_records

    def mark_article_crawled(self, date_str, article_url, title, file_path):
        """标记文章为已爬取"""
        article_key = f"{date_str}_{article_url}_{title}"
        self.crawled_records[article_key] = {
            'date': date_str,
            'url': article_url,
            'title': title,
            'file_path': file_path,
            'crawled_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }

    def process_article(self, article_url, date_str, version_code, version_name):
        """处理单篇文章（增量版本）"""
        try:
            formatted_date = f"{date_str[:6]}/{date_str[6:]}"
            
            resp = self.safe_request(article_url)
            if not resp:
                return None

            soup = BeautifulSoup(resp.text, 'html.parser')
            
            # 获取文章标题
            title_elem = soup.find('div', class_='title') or soup.find('h1')
            if not title_elem:
                print(f"    └── ❌ 未找到文章标题: {article_url}")
                return None
            
            title = title_elem.text.strip()
            print(f"    ├── 文章: {title}")
            
            # 检查文章是否已爬取
            if self.is_article_crawled(date_str, article_url, title):
                print(f"    │   └── ⏭️  文章已存在，跳过")
                return self.crawled_records[f"{date_str}_{article_url}_{title}"]['file_path']

            # 创建文章目录
            article_dir = self.create_article_dir(date_str, version_code, version_name, title)
            
            # 处理文章内容
            content = soup.find('div', class_='article-content') or soup.find('div', id='content')
            if not content:
                print(f"    │   └── ❌ 未找到文章内容")
                return None
            
            # 清理内容
            for tag in content.find_all(['script', 'style']):
                tag.decompose()
            
            text = content.get_text(separator='\n', strip=True)
            
            # 保存文章文本
            clean_title = self.clean_filename(title)
            text_file = os.path.join(article_dir, f"{clean_title}.txt")
            with open(text_file, 'w', encoding='utf-8') as f:
                f.write(f"标题：{title}\n")
                f.write(f"日期：{date_str}\n")
                f.write(f"版面：{version_code}_{version_name}\n")
                f.write(f"链接：{article_url}\n\n")
                f.write("正文：\n")
                f.write(text)
            
            print(f"    │   ├── 📄 已保存文本")
            
            # 下载图片
            images = content.find_all('img')
            for i, img in enumerate(images, 1):
                img_url = img.get('src')
                if img_url:
                    if img_url.startswith('..'):
                        img_url = f"https://epaper.nfnews.com/nfdaily/res/{formatted_date}/{img_url.split('/')[-1]}"
                    elif not img_url.startswith('http'):
                        img_url = f"https://epaper.nfnews.com{img_url}"
                    
                    img_path = os.path.join(article_dir, f"{clean_title}_{i:02d}.jpg")
                    if self.download_image(img_url, img_path):
                        print(f"    │   ├── 🖼️  已保存图片_{i:02d}")
            
            # 标记文章为已爬取
            self.mark_article_crawled(date_str, article_url, title, text_file)
            print(f"    │   └── ✅ 完成")
            return text_file
        
        except Exception as e:
            print(f"    │   └── ❌ 处理失败: {str(e)}")
            return None

    def run(self, start_date, end_date):
        """运行爬虫（增量版本）"""
        try:
            date_range = pd.date_range(start=start_date, end=end_date, freq='D')
            
            for date in date_range:
                date_str = date.strftime('%Y%m%d')
                print(f"\n📅 {date_str}")
                
                versions = self.get_version_list(date_str)
                if not versions:
                    print(f"└── ❌ 未获取到版面列表")
                    continue

                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    futures = []
                    for version in versions:
                        futures.append(
                            executor.submit(self.process_version, version, date_str)
                        )
                        time.sleep(self.request_delay)

                    for future in as_completed(futures):
                        try:
                            future.result()
                        except Exception as e:
                            print(f"└── ❌ 版面处理任务失败: {str(e)}")

                # 每处理完一天的数据就保存一次记录
                self.save_crawled_records()
                print(f"\n└── ✅ 完成 {date_str} 的数据处理")
                time.sleep(5)

        except Exception as e:
            print(f"\n❌ 爬虫运行出错: {str(e)}")
        finally:
            # 最后再保存一次记录
            self.save_crawled_records()
            print("\n已完成爬取任务！")

    def safe_request(self, url, retry_times=3):
        """安全的请求方法"""
        for i in range(retry_times):
            try:
                # 更新请求头
                headers = {
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
                    'Cache-Control': 'max-age=0',
                    'Connection': 'keep-alive',
                    'Host': 'epaper.nfnews.com',
                    'Referer': 'https://epaper.nfnews.com/',
                    'Sec-Ch-Ua': '"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"',
                    'Sec-Ch-Ua-Mobile': '?0',
                    'Sec-Ch-Ua-Platform': '"macOS"',
                    'Sec-Fetch-Dest': 'document',
                    'Sec-Fetch-Mode': 'navigate',
                    'Sec-Fetch-Site': 'same-origin',
                    'Sec-Fetch-User': '?1',
                    'Upgrade-Insecure-Requests': '1',
                    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'
                }
                
                # 随机延迟
                time.sleep(random.uniform(2, 5))
                
                response = requests.get(url, headers=headers, timeout=15)
                response.raise_for_status()
                response.encoding = 'utf-8'
                return response
                
            except Exception as e:
                print(f"    ├── ⚠️ 请求失败 {url}")
                print(f"    ├── ⚠️ 重试 {i+1}/{retry_times}: {str(e)}")
                if i == retry_times - 1:
                    return None
                time.sleep(random.uniform(5, 10))  # 失败后等待更长时间

    def download_image(self, img_url, save_path):
        """下载图片"""
        try:
            response = self.safe_request(img_url)
            if response and response.content:
                with open(save_path, 'wb') as f:
                    f.write(response.content)
                return True
            return False
        except Exception as e:
            print(f"下载图片失败 {img_url}: {str(e)}")
            return False

    def get_version_list(self, date_str):
        """获取版面列表"""
        try:
            formatted_date = f"{date_str[:6]}/{date_str[6:]}"
            versions = []
            visited_urls = set()  # 记录已访问的URL，避免循环
            print(f"├── 🔍 开始获取版面列表...")

            # 从A01版开始
            current_url = f"{self.base_url}/{formatted_date}/node_A01.html"
            
            while current_url and current_url not in visited_urls:
                visited_urls.add(current_url)
                try:
                    resp = self.safe_request(current_url)
                    if not resp:
                        break
                    
                    soup = BeautifulSoup(resp.text, 'html.parser')
                    
                    # 获取当前版面信息
                    version_code = current_url.split('node_')[1].split('.')[0]
                    
                    # 尝试多种方式获取版面名称
                    version_name = None
                    # 1. 从标题获取
                    title_elem = soup.find('title')
                    if title_elem:
                        title_text = title_elem.text.strip()
                        if '南方日报' in title_text:
                            version_name = title_text.split('南方日报')[1].strip()
                    
                    # 2. 从版面导航获取
                    if not version_name:
                        nav_elem = soup.find('div', class_='position') or soup.find('div', class_='nav')
                        if nav_elem:
                            version_name = nav_elem.text.strip()
                    
                    # 3. 如果都没找到，使用默认名称
                    if not version_name:
                        version_name = f"第{version_code}版"
                    
                    # 添加到版面列表
                    if not any(v['code'] == version_code for v in versions):
                        versions.append({
                            'code': version_code,
                            'name': version_name,
                            'url': current_url
                        })
                        print(f"│   ├── ✓ 找到版面: {version_code}_{version_name}")
                    
                    # 查找下一版链接
                    next_link = None
                    
                    # 1. 尝试找"下一版"按钮
                    for link in soup.find_all('a', href=True):
                        if '下一版' in link.text or '下一页' in link.text:
                            next_link = link.get('href')
                            break
                    
                    # 2. 尝试从版面导航获取
                    if not next_link:
                        nav_area = soup.find('div', class_='nav-area') or soup.find('div', class_='list-box')
                        if nav_area:
                            current_found = False
                            for link in nav_area.find_all('a', href=True):
                                href = link.get('href', '')
                                if current_url.endswith(href):
                                    current_found = True
                                elif current_found and 'node_' in href:
                                    next_link = href
                                    break
                    
                    # 更新下一个URL
                    if next_link:
                        if next_link.startswith('http'):
                            current_url = next_link
                        else:
                            current_url = f"{self.base_url}/{formatted_date}/{next_link}"
                    else:
                        break
                    
                except Exception as e:
                    print(f"│   ├── ⚠️ 处理版面出错: {str(e)}")
                    break
            
            # 按版面编号排序
            versions.sort(key=lambda x: (x['code'][0], int(x['code'][1:] if x['code'][1:].isdigit() else x['code'][2:])))
            
            if versions:
                print(f"\n├── 📋 共找到 {len(versions)} 个版面:")
                for v in versions:
                    print(f"│   ├── {v['code']}_{v['name']}")
            else:
                print(f"├── ⚠️ 未找到任何版面，请检查网址是否正确")
            
            return versions
            
        except Exception as e:
            print(f"├── ❌ 获取版面列表失败: {str(e)}")
            return []

class NanfangDailyIncrementalSpider(NanfangDailySpider):
    def __init__(self):
        super().__init__()  # 正确调用父类的初始化方法
        self.record_file = "crawled_articles.json"
        self.crawled_records = self.load_crawled_records()

    def process_article(self, article_url, date_str, version_code, version_name):
        """处理单篇文章（增量版本）"""
        try:
            formatted_date = f"{date_str[:6]}/{date_str[6:]}"
            
            resp = self.safe_request(article_url)
            if not resp:
                return None

            soup = BeautifulSoup(resp.text, 'html.parser')
            
            # 获取文章标题
            title_elem = soup.find('div', class_='title') or soup.find('h1')
            if not title_elem:
                print(f"未找到文章标题: {article_url}")
                return None
            
            title = title_elem.text.strip()
            print(f"处理文章: {title}")
            
            # 检查文章是否已爬取
            if self.is_article_crawled(date_str, article_url, title):
                print(f"文章已存在，跳过: {title}")
                return self.crawled_records[f"{date_str}_{article_url}_{title}"]['file_path']

            # 创建文章目录
            article_dir = self.create_article_dir(date_str, version_code, version_name, title)
            
            # 处理文章内容
            content = soup.find('div', class_='article-content') or soup.find('div', id='content')
            if not content:
                print(f"未找到文章内容: {article_url}")
                return None
            
            # 清理内容
            for tag in content.find_all(['script', 'style']):
                tag.decompose()
            
            text = content.get_text(separator='\n', strip=True)
            
            # 保存文章文本
            clean_title = self.clean_filename(title)
            text_file = os.path.join(article_dir, f"{clean_title}.txt")
            with open(text_file, 'w', encoding='utf-8') as f:
                f.write(f"标题：{title}\n")
                f.write(f"日期：{date_str}\n")
                f.write(f"版面：{version_code}_{version_name}\n")
                f.write(f"链接：{article_url}\n\n")
                f.write("正文：\n")
                f.write(text)
            
            # 下载图片
            images = content.find_all('img')
            for i, img in enumerate(images, 1):
                img_url = img.get('src')
                if img_url:
                    if img_url.startswith('..'):
                        img_url = f"https://epaper.nfnews.com/nfdaily/res/{formatted_date}/{img_url.split('/')[-1]}"
                    elif not img_url.startswith('http'):
                        img_url = f"https://epaper.nfnews.com{img_url}"
                    
                    img_path = os.path.join(article_dir, f"{clean_title}_{i:02d}.jpg")
                    if self.download_image(img_url, img_path):
                        print(f"已保存图片: {img_path}")
            
            # 标记文章为已爬取
            self.mark_article_crawled(date_str, article_url, title, text_file)
            print(f"已保存文章: {title}")
            return text_file
        
        except Exception as e:
            print(f"处理文章失败: {article_url}, 错误: {str(e)}")
            return None

def run_spider():
    """运行爬虫的定时任务"""
    spider = NanfangDailyIncrementalSpider()
    spider.crawl_today()
    print(f"完成定时爬取任务: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == '__main__':
    # 测试模式
    TEST_MODE = True
    spider = NanfangDailyIncrementalSpider()
    
    if TEST_MODE:
        print("=== 测试模式 ===")
        # 使用固定的测试日期
        test_date = '2025-07-3'  # 使用一个确定存在的日期
        print(f"测试爬取日期: {test_date}")
        spider.run(test_date, test_date)
    else:
        # 正常模式：设置定时任务
        schedule.every().day.at("06:00").do(run_spider)  # 早上6点
        schedule.every().day.at("14:00").do(run_spider)  # 下午2点
        schedule.every().day.at("22:00").do(run_spider)  # 晚上10点
        
        print(f"增量爬虫已启动，将在每天 06:00、14:00、22:00 运行...")
        print(f"当前时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 先执行一次，爬取当天的内容
        print("执行首次爬取...")
        run_spider()
        
        # 运行定时任务循环
        while True:
            try:
                schedule.run_pending()
                time.sleep(60)  # 每分钟检查一次
            except KeyboardInterrupt:
                print("\n爬虫已停止运行")
                break
            except Exception as e:
                print(f"发生错误: {str(e)}")
                time.sleep(300)  # 发生错误时等待5分钟后继续
