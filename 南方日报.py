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

class NanfangDailySpider:
    def __init__(self):
        self.headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Referer': 'https://epaper.southcn.com/',
            'Connection': 'keep-alive',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache',
            'Upgrade-Insecure-Requests': '1',
            'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-User': '?1'
        }
        
        self.base_url = "https://epaper.southcn.com/nfdaily/html"
        self.base_dir = "南方日报"
        self.max_workers = 5
        self.request_delay = 1

    def safe_request(self, url, max_retry=3):
        """安全的网络请求函数"""
        for attempt in range(max_retry):
            try:
                # 增加随机延迟
                time.sleep(random.uniform(2, 5))
                
                # 增加超时时间
                resp = requests.get(url, headers=self.headers, timeout=30)
                resp.raise_for_status()
                resp.encoding = 'utf-8'
                
                if not resp.text.strip():
                    raise Exception("Empty response")
                    
                return resp
                
            except Exception as e:
                print(f"请求失败(尝试 {attempt + 1}/{max_retry}): {url}, 错误: {str(e)}")
                if attempt == max_retry - 1:
                    return None
                # 增加重试等待时间
                time.sleep(5 * (attempt + 1))
        return None

    def get_version_list(self, date_str, current_url=None):
        """获取版面列表"""
        try:
            date_obj = datetime.strptime(date_str, '%Y%m%d')
            formatted_date = f"{date_str[:6]}/{date_str[6:]}"
            
            # 使用传入的URL或构建初始URL
            url = current_url or f"{self.base_url}/{formatted_date}/node_A01.html"
            print(f"访问URL: {url}")
            
            resp = self.safe_request(url)
            if not resp:
                return []

            soup = BeautifulSoup(resp.text, 'html.parser')
            versions = []
            
            # 获取当前版面的文章
            article_links = []
            for link in soup.find_all('a', href=True):
                href = link.get('href', '')
                if 'content_' in href and href.endswith('.html'):
                    article_links.append({
                        'title': link.text.strip(),
                        'url': href if href.startswith('http') else f"{self.base_url}/{formatted_date}/{href}"
                    })
                    print(f"找到文章: {link.text.strip()}")

            # 获取当前版面信息
            title_elem = soup.find('title')
            if title_elem:
                title_text = title_elem.text
                version_code = title_text.split('第')[-1].split('版')[0] if '版' in title_text else 'A01'
                version_name = title_text.split('：')[-1].split('_')[0] if '：' in title_text else '未命名'
                
                if article_links:
                    versions.append({
                        'code': version_code,
                        'name': version_name,
                        'articles': article_links
                    })
                    print(f"处理版面: {version_code} - {version_name}")

            # 获取下一版链接并递归处理
            next_page = soup.find('a', string='下一版')
            if next_page and next_page.get('href'):
                next_url = next_page['href']
                if not next_url.startswith('http'):
                    next_url = f"{self.base_url}/{formatted_date}/{next_url}"
                print(f"处理下一版: {next_url}")
                # 递归获取下一版内容
                next_versions = self.get_version_list(date_str, next_url)
                versions.extend(next_versions)
            
            return versions
            
        except Exception as e:
            print(f"获取版面列表失败: {str(e)}")
            return []

    def create_article_dir(self, date_str, version_code, version_name, title):
        """创建文章目录结构"""
        # 日期目录: YYYYMMDD
        date_dir = os.path.join(self.base_dir, date_str)
        
        # 版面目录: A01_要闻
        version_dir = os.path.join(date_dir, f"{version_code}_{version_name}")
        
        # 文章目录: 文章标题
        article_dir = os.path.join(version_dir, self.clean_filename(title))
        
        # 创建文章目录
        os.makedirs(article_dir, exist_ok=True)
        
        return article_dir

    def clean_filename(self, filename):
        """清理文件名中的非法字符"""
        # 替换文件名中的非法字符
        invalid_chars = '<>:"/\\|?*'
        for char in invalid_chars:
            filename = filename.replace(char, '_')
        return filename.strip()

    def process_article(self, article_url, date_str, version_code, version_name):
        """处理单篇文章"""
        try:
            formatted_date = f"{date_str[:6]}/{date_str[6:]}"
            
            resp = self.safe_request(article_url)
            if not resp:
                return None

            soup = BeautifulSoup(resp.text, 'html.parser')
            
            # 获取文章标题
            title_elem = soup.find('h1') or soup.find('div', class_='article-title')
            title = title_elem.text.strip() if title_elem else '无标题'
            
            # 创建文章目录
            article_dir = self.create_article_dir(date_str, version_code, version_name, title)
            
            # 处理文章内容
            content = None
            for selector in ['div#ozoom', 'div.article-content', 'div#content']:
                content = soup.find('div', id=selector.split('#')[1]) if '#' in selector else soup.find('div', class_=selector.split('.')[-1])
                if content:
                    break
                
            if content:
                # 清理内容
                for tag in content.find_all(['script', 'style']):
                    tag.decompose()
                
                text = content.get_text(separator='\n', strip=True)
                
                # 保存文章文本，使用文章标题作为文件名
                clean_title = self.clean_filename(title)
                text_file = os.path.join(article_dir, f"{clean_title}.txt")
                with open(text_file, 'w', encoding='utf-8') as f:
                    f.write(f"标题：{title}\n\n")
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
                        
                        img_url = img_url.split('.jpg')[0] + '.jpg'
                        # 图片直接保存在文章目录下
                        img_path = os.path.join(article_dir, f"{clean_title}_{i:02d}.jpg")
                        self.download_image(img_url, img_path)
                
                print(f"已保存文章: {title}")
                return text_file
            
            return None
            
        except Exception as e:
            print(f"处理文章失败: {article_url}, 错误: {str(e)}")
            return None

    def download_image(self, url, filepath):
        """下载图片"""
        try:
            resp = self.safe_request(url)
            if resp and resp.content:
                os.makedirs(os.path.dirname(filepath), exist_ok=True)
                with open(filepath, 'wb') as f:
                    f.write(resp.content)
                print(f"图片已保存: {filepath}")
                return True
        except Exception as e:
            print(f"下载图片失败: {url}, 错误: {str(e)}")
        return False

    def process_version(self, version, date_str):
        """处理单个版面"""
        try:
            # 处理版面中的所有文章
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = []
                for article in version['articles']:
                    futures.append(
                        executor.submit(
                            self.process_article,
                            article['url'],
                            date_str,
                            version['code'],
                            version['name']
                        )
                    )
                    time.sleep(self.request_delay)

                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        print(f"处理文章任务失败: {str(e)}")

        except Exception as e:
            print(f"处理版面失败: {version['code']}, 错误: {str(e)}")

    def run(self, start_date, end_date):
        """运行爬虫"""
        try:
            date_range = pd.date_range(start=start_date, end=end_date, freq='D')
            
            for date in date_range:
                date_str = date.strftime('%Y%m%d')
                print(f"\n开始处理 {date_str} 的数据...")
                
                # 获取版面列表
                versions = self.get_version_list(date_str)
                if not versions:
                    print(f"未获取到 {date_str} 的版面列表")
                    continue

                # 处理每个版面
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
                            print(f"版面处理任务失败: {str(e)}")

                print(f"完成 {date_str} 的数据处理")
                time.sleep(5)  # 每天之间的间隔

        except Exception as e:
            print(f"爬虫运行出错: {str(e)}")
        finally:
            print("\n爬取任务完成！")

if __name__ == '__main__':
    spider = NanfangDailySpider()
    
    # 直接指定日期范围
    start_date = '2024-01-01'  # 从2022年1月1日开始
    end_date = '2024-01-02'    # 到2024年3月19日结束
    
    print(f"开始爬取从 {start_date} 到 {end_date} 的新闻...")
    spider.run(start_date, end_date)
