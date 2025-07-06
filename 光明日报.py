#!/usr/bin/env python
# -*- coding: UTF-8 -*-
import os
import time
import json
import requests
import pandas as pd
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
import random
import re
from urllib.parse import urljoin, urlparse

class GuangmingDailySpider:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Referer': 'https://epaper.gmw.cn/gmrb/'
        }
        self.base_url = "https://epaper.gmw.cn/gmrb/html"
        self.base_dir = "光明日报"
        self.max_workers = 3
        self.request_delay = 2

    def safe_request(self, url):
        """安全的网络请求函数"""
        try:
            time.sleep(random.uniform(1, 2))
            resp = requests.get(url, headers=self.headers, timeout=20)  # 增加超时时间
            resp.raise_for_status()
            resp.encoding = 'utf-8'
            return resp if resp.text.strip() else None
        except Exception as e:
            print(f"请求失败: {url}, 错误: {str(e)}")
            # 如果是超时错误，再试一次
            if 'timeout' in str(e).lower():
                try:
                    time.sleep(2)  # 等待2秒后重试
                    resp = requests.get(url, headers=self.headers, timeout=30)  # 第二次尝试用更长的超时时间
                    resp.raise_for_status()
                    resp.encoding = 'utf-8'
                    return resp if resp.text.strip() else None
                except:
                    pass
            return None

    def get_date_url_formats(self, date_str):
        """获取可能的日期URL格式"""
        date_obj = datetime.strptime(date_str, '%Y%m%d')
        year = date_obj.year
        month = date_obj.month
        day = date_obj.day
        
        # 基于实际观察的URL格式
        formats = [
            f"{self.base_url}/{year}-{month:02d}/{day:02d}/nbs.D110000gmrb_01.htm",
            f"{self.base_url}/{year}-{month:02d}/{day:02d}/nw.D110000gmrb_{date_str}_1-01.htm",
            f"{self.base_url}/{year}-{month:02d}/{day:02d}/nbs.D110000gmrb_06.htm",
        ]
        
        return formats

    def find_valid_url(self, date_str):
        """寻找有效的URL"""
        formats = self.get_date_url_formats(date_str)
        
        for url in formats:
            print(f"尝试访问: {url}")
            resp = self.safe_request(url)
            if resp:
                return url, resp
        
        return None, None

    def clean_version_name(self, name):
        """清理版面名称，去除变体"""
        # 移除常见的后缀
        name = re.sub(r'[_\-]光明网$', '', name)
        name = re.sub(r'[_\-]光明日报$', '', name)
        # 移除其他可能的变体标记
        name = re.sub(r'[_\-][\d]+$', '', name)
        return name.strip()

    def get_version_list(self, date_str):
        """获取版面列表"""
        try:
            year, month, day = date_str[:4], date_str[4:6], date_str[6:]
            url = f"{self.base_url}/{year}-{month}/{day}/nbs.D110000gmrb_01.htm"
            resp = self.safe_request(url)
            if not resp:
                return []

            soup = BeautifulSoup(resp.text, 'html.parser')
            versions = []
            processed = set()

            # 查找所有版面链接
            for item in soup.find_all(['a', 'div'], string=re.compile(r'第\d+版')):
                match = re.search(r'第(\d+)版[：:]?(.*?)(?=第|$)', item.text)
                if not match:
                    continue

                code = match.group(1).zfill(2)
                name = re.sub(r'[_\-](光明网|光明日报)$', '', match.group(2).strip())
                key = f"{code}_{name}"

                if key in processed:
                    continue
                processed.add(key)

                version_url = f"{self.base_url}/{year}-{month}/{day}/nbs.D110000gmrb_{code}.htm"
                versions.append({
                    'code': code,
                    'name': name,
                    'url': version_url
                })

            # 按版面号排序并去重
            versions.sort(key=lambda x: int(x['code']))
            # 使用字典去重，保留第一个出现的版面
            unique_versions = {v['code']: v for v in versions}.values()
            
            print(f"找到 {len(unique_versions)} 个版面")
            for version in unique_versions:
                print(f"  版面: {version['code']} - {version['name']}")
            
            return list(unique_versions)
        except Exception as e:
            print(f"获取版面列表失败: {str(e)}")
            return []

    def get_articles_from_version(self, version_url):
        """获取版面文章"""
        try:
            resp = self.safe_request(version_url)
            if not resp:
                return []

            soup = BeautifulSoup(resp.text, 'html.parser')
            articles = []
            processed = set()

            # 查找文章链接
            for link in soup.find_all('a', href=True):
                title = link.get_text(strip=True)
                href = link.get('href', '')

                if (not title or len(title) < 4 or title in processed or
                    any(skip in title for skip in ['上一版', '下一版', '返回目录', '光明日报']) or
                    not any(pattern in href for pattern in ['nw.D110000gmrb', 'content_'])):
                    continue

                processed.add(title)
                articles.append({
                    'title': title,
                    'url': urljoin(version_url, href)
                })

            return articles
        except Exception as e:
            print(f"获取文章列表失败: {str(e)}")
            return []

    def create_article_dir(self, date_str, version_code, version_name, title):
        """创建文章目录结构"""
        date_dir = os.path.join(self.base_dir, date_str)
        version_dir = os.path.join(date_dir, f"{version_code}_{version_name}")
        article_dir = os.path.join(version_dir, self.clean_filename(title))
        os.makedirs(article_dir, exist_ok=True)
        return article_dir

    def clean_filename(self, filename):
        """清理文件名"""
        return re.sub(r'[<>:"/\\|?*]', '_', filename)[:100].strip()

    def process_article(self, article_url, date_str, version_code, version_name, title):
        """处理文章"""
        try:
            resp = self.safe_request(article_url)
            if not resp:
                return None

            soup = BeautifulSoup(resp.text, 'html.parser')
            article_dir = os.path.join(self.base_dir, date_str, f"{version_code}_{version_name}", 
                                     self.clean_filename(title))
            os.makedirs(article_dir, exist_ok=True)

            # 获取文章内容
            content = None
            for selector in ['div#ozoom', 'div.article-content', 'div.content']:
                content = soup.select_one(selector)
                if content:
                    break

            if not content:
                title_elem = soup.find(['h1', 'h2'], string=lambda x: x and title in x)
                content = title_elem.find_next(['div', 'article']) if title_elem else None

            if content:
                # 清理内容
                for tag in content.find_all(['script', 'style', 'nav', 'header', 'footer', 'a']):
                    tag.decompose()

                text = content.get_text(separator='\n', strip=True)
                if len(text) > 50:
                    # 保存文章
                    with open(os.path.join(article_dir, f"{self.clean_filename(title)}.txt"), 'w', encoding='utf-8') as f:
                        f.write(f"标题：{title}\n日期：{date_str}\n版面：{version_code} - {version_name}\n链接：{article_url}\n")
                        f.write("-" * 50 + "\n\n" + text)

                    # 保存图片
                    saved_images = set()  # 用于去重
                    
                    # 1. 首先尝试从文章内容中查找图片
                    for img in content.find_all('img'):
                        self.process_image(img, article_url, article_dir, title, date_str, version_code, saved_images)
                    
                    # 2. 如果没找到图片，尝试在整个页面查找
                    if not saved_images:
                        # 查找可能包含图片的区域
                        img_containers = soup.find_all(['div', 'p'], class_=lambda x: x and any(name in str(x).lower() 
                            for name in ['image', 'pic', 'photo', 'content', 'article']))
                        for container in img_containers:
                            for img in container.find_all('img'):
                                self.process_image(img, article_url, article_dir, title, date_str, version_code, saved_images)
                    
                    # 3. 特殊处理：尝试构建光明日报特有的图片URL格式
                    if not saved_images:
                        # 构建可能的图片URL
                        possible_urls = [
                            f"{self.base_url}/{date_str[:4]}-{date_str[4:6]}/{date_str[6:]}/images/{version_code}/{date_str}_{version_code}_pic.jpg",
                            f"https://epaper.gmw.cn/gmrb/images/{date_str[:4]}-{date_str[4:6]}/{date_str[6:]}/{version_code}/{date_str}{version_code}_b.jpg",
                            f"https://epaper.gmw.cn/gmrb/images/{date_str[:4]}-{date_str[4:6]}/{date_str[6:]}/{version_code}/p{version_code}.jpg"
                        ]
                        
                        for img_url in possible_urls:
                            if img_url not in saved_images:
                                img_resp = self.safe_request(img_url)
                                if img_resp and img_resp.content and len(img_resp.content) > 10 * 1024:
                                    img_name = f"{self.clean_filename(title)}_{len(saved_images)+1}.jpg"
                                    img_path = os.path.join(article_dir, img_name)
                                    with open(img_path, 'wb') as f:
                                        f.write(img_resp.content)
                                    print(f"      已保存图片: {img_name}")
                                    saved_images.add(img_url)

                    if saved_images:
                        print(f"      共保存 {len(saved_images)} 张图片")
                    print(f"    已保存文章: {title}")
                    return True

        except Exception as e:
            print(f"处理文章失败: {title}, 错误: {str(e)}")
        return False

    def process_image(self, img, article_url, article_dir, title, date_str, version_code, saved_images):
        """处理单个图片"""
        try:
            img_url = img.get('src', '')
            if not img_url or img_url in saved_images:
                return
                
            # 过滤装饰性图片
            if any(skip in img_url for skip in ['ico10.gif', 'd.gif', 'd1.gif', 'logo', 'banner']):
                return
                
            # 构建完整URL
            if not img_url.startswith('http'):
                if img_url.startswith('//'):
                    img_url = 'https:' + img_url
                else:
                    img_url = urljoin(article_url, img_url)
            
            # 检查是否已保存过该图片
            if img_url in saved_images:
                return
                
            # 下载图片
            img_resp = self.safe_request(img_url)
            if not img_resp or not img_resp.content:
                return
                
            # 检查图片大小
            if len(img_resp.content) < 10 * 1024:  # 跳过小于10KB的图片
                return
                
            # 获取图片扩展名
            ext = 'jpg'  # 默认扩展名
            if '.' in img_url.split('/')[-1]:
                ext = img_url.split('.')[-1].lower()
                if ext not in ['jpg', 'jpeg', 'png', 'bmp']:
                    return
                    
            # 保存图片
            img_name = f"{self.clean_filename(title)}_{len(saved_images)+1}.{ext}"
            img_path = os.path.join(article_dir, img_name)
            with open(img_path, 'wb') as f:
                f.write(img_resp.content)
            print(f"      已保存图片: {img_name}")
            saved_images.add(img_url)
            
        except Exception as e:
            print(f"      处理图片失败: {img_url}, 错误: {str(e)}")

    def process_version(self, version, date_str):
        """处理单个版面"""
        try:
            print(f"\n开始处理版面: {version['code']} - {version['name']}")
            
            # 获取版面的文章列表
            articles = self.get_articles_from_version(version['url'])
            
            if not articles:
                print(f"  版面 {version['code']} 没有找到文章")
                # 如果第一次获取失败，尝试使用另一种URL格式
                alt_url = f"{self.base_url}/{date_str[:4]}-{date_str[4:6]}/{date_str[6:]}/nw.D110000gmrb_{date_str}_1-{version['code']}.htm"
                articles = self.get_articles_from_version(alt_url)
                if not articles:
                    return
            
            print(f"  找到 {len(articles)} 篇文章")
            
            # 处理每篇文章
            for article in articles:
                self.process_article(
                    article['url'], 
                    date_str, 
                    version['code'], 
                    version['name'], 
                    article['title']
                )
                time.sleep(1)  # 减少延迟时间

        except Exception as e:
            print(f"处理版面失败: {version['code']}, 错误: {str(e)}")

    def run(self, start_date, end_date):
        """运行爬虫"""
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        date = start
        
        while date <= end:
            date_str = date.strftime('%Y%m%d')
            print(f"\n{'='*60}\n开始处理 {date_str} 的数据...\n{'='*60}")

            versions = self.get_version_list(date_str)
            if versions:
                for version in versions:
                    print(f"\n处理版面: {version['code']} - {version['name']}")
                    self.process_version(version, date_str)

            date = datetime.strptime((date + timedelta(days=1)).strftime('%Y-%m-%d'), '%Y-%m-%d')

if __name__ == '__main__':
    spider = GuangmingDailySpider()
    start_date = '2025-06-26'
    end_date = '2025-06-26'
    print(f"开始爬取从 {start_date} 到 {end_date} 的新闻...")
    spider.run(start_date, end_date)