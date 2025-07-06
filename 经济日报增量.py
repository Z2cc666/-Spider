#!/usr/bin/env python
# -*- coding: UTF-8 -*-
import os
import time
import requests
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import random
import re
from urllib.parse import urljoin
import schedule
import logging
from pathlib import Path

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('jjrb_crawler.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

class EconomicDailyIncrementalSpider:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Referer': 'http://paper.ce.cn/'
        }
        self.base_url = "http://paper.ce.cn/pc"
        self.base_dir = "经济日报"
        
        # 确保基础目录存在
        Path(self.base_dir).mkdir(exist_ok=True)
        
        # 记录已处理的文章，避免重复爬取
        self.processed_file = "processed_articles_jjrb.txt"
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
            year = date_str[:4]
            month = date_str[4:6]
            day = date_str[6:]
            url = f"{self.base_url}/layout/{year}{month}/{day}/node_01.html"
            
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
                name = match.group(2).strip()
                key = f"{code}_{name}"

                if key in processed:
                    continue
                processed.add(key)

                version_url = f"{self.base_url}/layout/{year}{month}/{day}/node_{code}.html"
                versions.append({
                    'code': code,
                    'name': name,
                    'url': version_url
                })

            versions.sort(key=lambda x: int(x['code']))
            logging.info(f"找到 {len(versions)} 个版面")
            for version in versions:
                logging.info(f"  版面: {version['code']} - {version['name']}")
            
            return versions
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

            # 查找标题导航区域下的所有链接
            nav_links = soup.find_all('a', href=lambda x: x and 'content_' in x)
            for link in nav_links:
                title = link.get_text(strip=True)
                href = link.get('href', '')
                
                if (not title or len(title) < 2 or title in processed or
                    any(skip in title for skip in ['上一版', '下一版', '返回目录', '经济日报'])):
                    continue
                
                processed.add(title)
                full_url = urljoin(version_url, href)
                articles.append({
                    'title': title,
                    'url': full_url
                })

            return articles
        except Exception as e:
            logging.error(f"获取文章列表失败: {e}")
            return []

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

            # 获取文章内容
            content_text = []
            
            # 1. 获取标题
            main_title = soup.find('h2')
            if main_title:
                content_text.append(main_title.get_text(strip=True))
                
                # 获取副标题（如果有）
                subtitle = main_title.find_next_sibling()
                if subtitle and not subtitle.find('a'):  # 确保不是导航链接
                    subtitle_text = subtitle.get_text(strip=True)
                    if subtitle_text and len(subtitle_text) > 5:
                        content_text.append(subtitle_text)
            
            # 2. 获取作者信息
            author_text = None
            for text in soup.stripped_strings:
                if '本报' in text:
                    author_text = text.strip()
                    break
            if author_text:
                content_text.append(author_text)
            
            # 3. 获取正文内容
            # 首先尝试查找文章主体区域
            article_body = None
            for div in soup.find_all('div'):
                if len(div.find_all('p')) > 3:  # 通常正文区域会有多个段落
                    article_body = div
                    break
            
            if article_body:
                # 处理正文段落
                for p in article_body.find_all(['p', 'div']):
                    # 跳过导航和页脚
                    if any(skip in str(p) for skip in ['标题导航', '上一篇', '下一篇', '关于经济日报']):
                        continue
                    
                    # 获取段落文本
                    text = p.get_text(strip=True)
                    if text and len(text) > 10:  # 有效段落通常大于10个字符
                        content_text.append(text)
            else:
                # 如果找不到明确的文章主体，尝试获取所有可能的段落
                for p in soup.find_all(['p', 'div']):
                    if p.parent and any(skip in str(p.parent) for skip in ['标题导航', 'header', 'footer', 'nav']):
                        continue
                    
                    text = p.get_text(strip=True)
                    if text and len(text) > 10 and not any(skip in text for skip in ['上一篇', '下一篇', '关于']):
                        content_text.append(text)

            # 如果找到了内容
            if len(content_text) > 1:  # 至少要有标题和一段正文
                # 保存文章
                article_file = os.path.join(article_dir, f"{self.clean_filename(title)}.txt")
                with open(article_file, 'w', encoding='utf-8') as f:
                    f.write(f"标题：{title}\n日期：{date_str}\n版面：{version_code} - {version_name}\n链接：{article_url}\n")
                    f.write("-" * 50 + "\n\n")
                    f.write('\n\n'.join(content_text))

                # 保存图片
                saved_images = set()
                
                # 1. 从文章内容中查找图片
                for img in soup.find_all('img'):
                    if img.get('src'):
                        img_url = img['src']
                        if img_url.startswith('/'):
                            img_url = f"http://paper.ce.cn{img_url}"
                        elif not img_url.startswith('http'):
                            img_url = urljoin(article_url, img_url)
                        
                        if not any(skip in img_url.lower() for skip in ['logo', 'icon', 'banner']):
                            try:
                                img_resp = self.safe_request(img_url)
                                if img_resp and img_resp.content and len(img_resp.content) > 10 * 1024:
                                    ext = '.jpg'
                                    if '.' in img_url.split('/')[-1]:
                                        ext = '.' + img_url.split('.')[-1].lower()
                                        if ext not in ['.jpg', '.jpeg', '.png', '.bmp']:
                                            ext = '.jpg'
                                    
                                    img_name = f"{self.clean_filename(title)}_{len(saved_images)+1}{ext}"
                                    img_path = os.path.join(article_dir, img_name)
                                    with open(img_path, 'wb') as f:
                                        f.write(img_resp.content)
                                    logging.info(f"      已保存图片: {img_name}")
                                    saved_images.add(img_url)
                            except Exception as e:
                                logging.error(f"      处理图片失败: {img_url}, 错误: {str(e)}")
                
                # 2. 尝试特定的图片URL格式
                if not saved_images:
                    # 构建可能的图片URL
                    possible_urls = [
                        f"http://paper.ce.cn/pc/res/zip/{year}{month}/{day}/version/images/{version_code}.jpg",
                        f"http://paper.ce.cn/pc/res/zip/{year}{month}/{day}/version/images/{version_code}_b.jpg",
                        f"http://paper.ce.cn/pc/res/zip/{year}{month}/{day}/version/images/{version_code}_s.jpg",
                        f"http://paper.ce.cn/pc/res/zip/{year}{month}/{day}/version/images/p{version_code}.jpg"
                    ]
                    
                    for img_url in possible_urls:
                        if img_url not in saved_images:
                            try:
                                img_resp = self.safe_request(img_url)
                                if img_resp and img_resp.content and len(img_resp.content) > 10 * 1024:
                                    ext = '.jpg'
                                    img_name = f"{self.clean_filename(title)}_{len(saved_images)+1}{ext}"
                                    img_path = os.path.join(article_dir, img_name)
                                    with open(img_path, 'wb') as f:
                                        f.write(img_resp.content)
                                    logging.info(f"      已保存图片: {img_name}")
                                    saved_images.add(img_url)
                            except Exception as e:
                                logging.error(f"      尝试备选图片URL失败: {img_url}, 错误: {str(e)}")

                if saved_images:
                    logging.info(f"      共保存 {len(saved_images)} 张图片")
                logging.info(f"    已保存文章: {title}")
                
                # 记录已处理的文章
                self.save_processed_article(article_url)
                return True

        except Exception as e:
            logging.error(f"处理文章失败: {title}, 错误: {str(e)}")
        return False

    def process_version(self, version, date_str):
        """处理单个版面"""
        try:
            logging.info(f"\n开始处理版面: {version['code']} - {version['name']}")
            
            articles = self.get_articles_from_version(version['url'])
            
            if not articles:
                logging.info(f"  版面 {version['code']} 没有找到文章")
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
            logging.error(f"处理版面失败: {version['code']}, 错误: {str(e)}")

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
        spider = EconomicDailyIncrementalSpider()
        spider.crawl_today()
    except Exception as e:
        logging.error(f"爬虫运行出错: {e}")

def schedule_crawler():
    """设置定时任务"""
    logging.info("设置定时任务时间点：06:00, 10:00, 15:32, 18:00, 22:00")
    schedule.every().day.at("06:00").do(run_crawler)
    schedule.every().day.at("10:00").do(run_crawler)
    schedule.every().day.at("15:39").do(run_crawler)
    schedule.every().day.at("18:00").do(run_crawler)
    schedule.every().day.at("22:00").do(run_crawler)
    
    # 检查是否有需要立即运行的任务
    now = datetime.now()
    current_time = now.time()
    run_times = ["06:00", "10:00", "15:32", "18:00", "22:00"]
    
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
