#!/usr/bin/env python
# -*- coding: UTF-8 -*-
import os
import time
import requests
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import re
from urllib.parse import urljoin
import random
import schedule
import logging
from pathlib import Path

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('gmrb_crawler.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

class GuangmingDailyIncrementalSpider:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Referer': 'https://epaper.gmw.cn/gmrb/'
        }
        self.base_url = "https://epaper.gmw.cn/gmrb/html"
        self.base_dir = "光明日报"
        
        # 确保基础目录存在
        Path(self.base_dir).mkdir(exist_ok=True)
        
        # 记录已处理的文章，避免重复爬取
        self.processed_file = "processed_articles.txt"
        self.processed_articles = self.load_processed_articles()

    def load_processed_articles(self):
        """加载已处理的文章记录"""
        try:
            if os.path.exists(self.processed_file):
                with open(self.processed_file, 'r', encoding='utf-8') as f:
                    return set(line.strip() for line in f)
            return set()
        except Exception as e:
            logging.error(f"加载已处理文章记录失败: {e}")
            return set()

    def save_processed_article(self, article_url):
        """保存已处理的文章记录"""
        try:
            with open(self.processed_file, 'a', encoding='utf-8') as f:
                f.write(article_url + '\n')
            self.processed_articles.add(article_url)
        except Exception as e:
            logging.error(f"保存文章记录失败: {e}")

    def safe_request(self, url):
        """安全的网络请求函数"""
        try:
            time.sleep(random.uniform(1, 2))
            resp = requests.get(url, headers=self.headers, timeout=20)
            resp.raise_for_status()
            resp.encoding = 'utf-8'
            return resp if resp.text.strip() else None
        except Exception as e:
            if 'timeout' in str(e).lower():
                try:
                    time.sleep(2)
                    resp = requests.get(url, headers=self.headers, timeout=30)
                    resp.raise_for_status()
                    resp.encoding = 'utf-8'
                    return resp if resp.text.strip() else None
                except:
                    pass
            logging.error(f"请求失败: {url}, 错误: {e}")
            return None

    def clean_filename(self, filename):
        """清理文件名"""
        return re.sub(r'[<>:"/\\|?*]', '_', filename)[:100].strip()

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

            versions.sort(key=lambda x: int(x['code']))
            unique_versions = {v['code']: v for v in versions}.values()
            
            logging.info(f"找到 {len(unique_versions)} 个版面")
            for version in unique_versions:
                logging.info(f"  版面: {version['code']} - {version['name']}")
            
            return list(unique_versions)
        except Exception as e:
            logging.error(f"获取版面列表失败: {e}")
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
            logging.error(f"获取文章列表失败: {e}")
            return []

    def process_image(self, img, article_url, article_dir, title, date_str, version_code, saved_images):
        """处理单个图片"""
        try:
            img_url = img.get('src', '')
            if not img_url or img_url in saved_images:
                return
                
            if any(skip in img_url for skip in ['ico10.gif', 'd.gif', 'd1.gif', 'logo', 'banner']):
                return
                
            if not img_url.startswith('http'):
                if img_url.startswith('//'):
                    img_url = 'https:' + img_url
                else:
                    img_url = urljoin(article_url, img_url)
            
            if img_url in saved_images:
                return
                
            img_resp = self.safe_request(img_url)
            if not img_resp or not img_resp.content:
                return
                
            if len(img_resp.content) < 10 * 1024:
                return
                
            ext = 'jpg'
            if '.' in img_url.split('/')[-1]:
                ext = img_url.split('.')[-1].lower()
                if ext not in ['jpg', 'jpeg', 'png', 'bmp']:
                    return
                    
            img_name = f"{self.clean_filename(title)}_{len(saved_images)+1}.{ext}"
            img_path = os.path.join(article_dir, img_name)
            with open(img_path, 'wb') as f:
                f.write(img_resp.content)
            logging.info(f"      已保存图片: {img_name}")
            saved_images.add(img_url)
            
        except Exception as e:
            logging.error(f"      处理图片失败: {img_url}, 错误: {e}")

    def process_article(self, article_url, date_str, version_code, version_name, title):
        """处理文章"""
        # 检查是否已处理过
        if article_url in self.processed_articles:
            logging.info(f"    跳过已处理的文章: {title}")
            return False

        try:
            resp = self.safe_request(article_url)
            if not resp:
                return None

            soup = BeautifulSoup(resp.text, 'html.parser')
            article_dir = os.path.join(self.base_dir, date_str, f"{version_code}_{version_name}", 
                                     self.clean_filename(title))
            os.makedirs(article_dir, exist_ok=True)

            content = None
            for selector in ['div#ozoom', 'div.article-content', 'div.content']:
                content = soup.select_one(selector)
                if content:
                    break

            if not content:
                title_elem = soup.find(['h1', 'h2'], string=lambda x: x and title in x)
                content = title_elem.find_next(['div', 'article']) if title_elem else None

            if content:
                for tag in content.find_all(['script', 'style', 'nav', 'header', 'footer', 'a']):
                    tag.decompose()

                text = content.get_text(separator='\n', strip=True)
                if len(text) > 50:
                    with open(os.path.join(article_dir, f"{self.clean_filename(title)}.txt"), 'w', encoding='utf-8') as f:
                        f.write(f"标题：{title}\n日期：{date_str}\n版面：{version_code} - {version_name}\n链接：{article_url}\n")
                        f.write("-" * 50 + "\n\n" + text)

                    saved_images = set()
                    
                    # 1. 从文章内容中查找图片
                    for img in content.find_all('img'):
                        self.process_image(img, article_url, article_dir, title, date_str, version_code, saved_images)
                    
                    # 2. 在整个页面查找图片
                    if not saved_images:
                        img_containers = soup.find_all(['div', 'p'], class_=lambda x: x and any(name in str(x).lower() 
                            for name in ['image', 'pic', 'photo', 'content', 'article']))
                        for container in img_containers:
                            for img in container.find_all('img'):
                                self.process_image(img, article_url, article_dir, title, date_str, version_code, saved_images)
                    
                    # 3. 尝试特殊的图片URL格式
                    if not saved_images:
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
                                    logging.info(f"      已保存图片: {img_name}")
                                    saved_images.add(img_url)

                    if saved_images:
                        logging.info(f"      共保存 {len(saved_images)} 张图片")
                    logging.info(f"    已保存文章: {title}")
                    
                    # 记录已处理的文章
                    self.save_processed_article(article_url)
                    return True

        except Exception as e:
            logging.error(f"处理文章失败: {title}, 错误: {e}")
        return False

    def process_version(self, version, date_str):
        """处理单个版面"""
        try:
            logging.info(f"\n开始处理版面: {version['code']} - {version['name']}")
            
            articles = self.get_articles_from_version(version['url'])
            
            if not articles:
                logging.info(f"  版面 {version['code']} 没有找到文章")
                alt_url = f"{self.base_url}/{date_str[:4]}-{date_str[4:6]}/{date_str[6:]}/nw.D110000gmrb_{date_str}_1-{version['code']}.htm"
                articles = self.get_articles_from_version(alt_url)
                if not articles:
                    return
            
            logging.info(f"  找到 {len(articles)} 篇文章")
            
            for article in articles:
                self.process_article(
                    article['url'], 
                    date_str, 
                    version['code'], 
                    version['name'], 
                    article['title']
                )
                time.sleep(1)

        except Exception as e:
            logging.error(f"处理版面失败: {version['code']}, 错误: {e}")

    def crawl_today(self):
        """爬取今天的新闻"""
        date_str = datetime.now().strftime('%Y%m%d')
        logging.info(f"\n{'='*60}\n开始处理 {date_str} 的数据...\n{'='*60}")

        versions = self.get_version_list(date_str)
        if versions:
            for version in versions:
                self.process_version(version, date_str)
        else:
            logging.warning(f"未找到 {date_str} 的版面")

def run_crawler():
    """运行爬虫"""
    try:
        spider = GuangmingDailyIncrementalSpider()
        spider.crawl_today()
    except Exception as e:
        logging.error(f"爬虫运行出错: {e}")

def schedule_crawler():
    """设置定时任务"""
    logging.info("设置定时任务时间点：08:35, 10:00, 22:31")
    schedule.every().day.at("02:44").do(run_crawler)
    schedule.every().day.at("10:00").do(run_crawler)
    schedule.every().day.at("22:31").do(run_crawler)
    
    # 检查是否有需要立即运行的任务
    now = datetime.now()
    current_time = now.time()
    run_times = ["08:35", "10:00", "22:31"]
    
    # 获取今天已经过去的时间点
    missed_times = [
        datetime.strptime(t, "%H:%M").time()
        for t in run_times
        if current_time > datetime.strptime(t, "%H:%M").time()
    ]
    
    if missed_times:
        latest_missed = max(missed_times)
        logging.info(f"错过了今天 {latest_missed.strftime('%H:%M')} 的运行时间，立即开始运行")
        run_crawler()
    
    while True:
        try:
            schedule.run_pending()
            time.sleep(60)
        except Exception as e:
            logging.error(f"调度器运行出错: {e}")
            time.sleep(300)  # 出错后等待5分钟再继续

if __name__ == '__main__':
    schedule_crawler()
