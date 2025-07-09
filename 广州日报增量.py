#!/usr/bin/env python
# -*- coding: UTF-8 -*-
import os
import time
import requests
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import random
import re
from urllib.parse import urljoin, urlparse
import schedule
import logging
from pathlib import Path

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('gzrb_crawler.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

class GuangzhouDailyIncrementalSpider:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Referer': 'https://gzdaily.dayoo.com/',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
        self.base_url = "https://gzdaily.dayoo.com/pc/html"
        self.base_dir = "广州日报"
        
        # 确保基础目录存在
        Path(self.base_dir).mkdir(exist_ok=True)
        
        # 记录已处理的文章，避免重复爬取
        self.processed_file = "processed_articles_gzrb.txt"
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

    def safe_request(self, url, retries=3):
        """安全的网络请求函数"""
        for attempt in range(retries):
            try:
                time.sleep(random.uniform(1, 3))
                resp = requests.get(url, headers=self.headers, timeout=30)
                
                if resp.status_code == 200:
                    resp.encoding = 'utf-8'
                    return resp if resp.text.strip() else None
                elif resp.status_code == 404:
                    logging.warning(f"页面不存在: {url}")
                    return None
                else:
                    logging.warning(f"HTTP {resp.status_code}: {url}")
                    
            except requests.exceptions.Timeout:
                logging.warning(f"请求超时 (尝试 {attempt + 1}/{retries}): {url}")
                if attempt < retries - 1:
                    time.sleep(5)
                    continue
            except Exception as e:
                logging.error(f"请求失败 (尝试 {attempt + 1}/{retries}): {url}, 错误: {str(e)}")
                if attempt < retries - 1:
                    time.sleep(5)
                    continue
                    
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
            
            # 构建主页面URL
            main_url = f"{self.base_url}/{year}-{month}/{day}/node_1.htm"
            
            logging.info(f"尝试访问主页面: {main_url}")
            resp = self.safe_request(main_url)
            
            if not resp:
                logging.error(f"无法访问 {date_str} 的主页面")
                return []

            soup = BeautifulSoup(resp.text, 'html.parser')
            versions = []
            processed = set()

            # 查找版面链接
            for link in soup.find_all('a', href=True):
                text = link.get_text(strip=True)
                href = link.get('href', '')
                
                # 匹配版面格式，如 "第A1版：头版"
                match = re.search(r'第([A-Z]\d+)版[：:]?(.*?)(?=第|$)', text)
                if match:
                    code = match.group(1)
                    name = match.group(2).strip() if match.group(2) else "未知版面"
                    
                    key = f"{code}_{name}"
                    if key not in processed:
                        processed.add(key)
                        
                        # 构建版面URL
                        version_url = urljoin(main_url, href)
                        versions.append({
                            'code': code,
                            'name': name,
                            'url': version_url
                        })

            # 按版面代码排序
            versions.sort(key=lambda x: x['code'])
            
            logging.info(f"找到 {len(versions)} 个版面")
            for version in versions:
                logging.info(f"  版面: {version['code']} - {version['name']}")
            
            return versions
            
        except Exception as e:
            logging.error(f"获取版面列表失败: {str(e)}")
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
                href = link.get('href', '')
                title = link.get_text(strip=True)
                
                # 检查是否是文章链接
                if ('content_' in href and 
                    title and len(title) > 2 and 
                    title not in processed and
                    not any(skip in title for skip in ['上一版', '下一版', '返回目录', '广州日报'])):
                    
                    processed.add(title)
                    articles.append({
                        'title': title,
                        'url': urljoin(version_url, href)
                    })

            logging.info(f"在版面中找到 {len(articles)} 篇文章")
            return articles
            
        except Exception as e:
            logging.error(f"获取文章列表失败: {str(e)}")
            return []

    def extract_content(self, soup):
        """提取文章内容"""
        content_parts = []
        
        # 1. 获取标题
        title = soup.find('h1')
        if title:
            content_parts.append(('标题', title.get_text(strip=True)))
        
        # 2. 获取作者信息
        author_patterns = [
            r'文/.*?(?=\s|$)',
            r'本报.*?记者.*?(?=\s|$)',
            r'记者.*?(?=\s|$)',
            r'通讯员.*?(?=\s|$)',
            r'来源.*?(?=\s|$)',
        ]
        
        text_content = soup.get_text()
        for pattern in author_patterns:
            match = re.search(pattern, text_content)
            if match:
                content_parts.append(('作者', match.group(0).strip()))
                break
        
        # 3. 获取正文内容
        # 首先尝试获取文章字数信息
        word_count_elem = soup.find('div', string=re.compile(r'本文字数：\d+'))
        if word_count_elem:
            content_parts.append(('字数', word_count_elem.get_text(strip=True)))
        
        # 尝试多个可能的内容容器
        content_selectors = [
            'div#ozoom',  # 主要内容容器
            'div.article',  # 备选容器
            'div.content',  # 备选容器
            'div#content'   # 备选容器
        ]
        
        content = None
        for selector in content_selectors:
            content = soup.select_one(selector)
            if content and len(content.get_text(strip=True)) > 50:
                break
        
        if content:
            # 清理内容
            for tag in content.find_all(['script', 'style', 'a']):
                tag.decompose()
            
            # 提取段落
            paragraphs = []
            # 首先尝试查找所有p标签
            p_tags = content.find_all('p')
            if p_tags:
                for p in p_tags:
                    text = p.get_text(strip=True)
                    if text and len(text) > 10:
                        paragraphs.append(text)
            else:
                # 如果没有p标签，按照换行符分割文本
                text = content.get_text('\n', strip=True)
                for line in text.split('\n'):
                    line = line.strip()
                    if line and len(line) > 10:
                        paragraphs.append(line)
            
            if paragraphs:
                # 过滤掉作者信息（避免重复）和其他无关信息
                filtered_paragraphs = []
                for para in paragraphs:
                    if not any(pattern in para for pattern in ['记者', '通讯员', '来源', '字数', '版权所有', '责编']):
                        filtered_paragraphs.append(para)
                
                if filtered_paragraphs:
                    content_parts.append(('正文', '\n\n'.join(filtered_paragraphs)))
        
        return content_parts

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
            
            # 提取内容
            content_parts = self.extract_content(soup)
            
            if not content_parts:
                logging.warning(f"未能提取到内容: {title}")
                return False
            
            # 创建文章目录
            article_dir = os.path.join(self.base_dir, date_str, f"{version_code}_{version_name}", 
                                     self.clean_filename(title))
            os.makedirs(article_dir, exist_ok=True)

            # 保存文章
            article_file = os.path.join(article_dir, f"{self.clean_filename(title)}.txt")
            with open(article_file, 'w', encoding='utf-8') as f:
                f.write(f"标题：{title}\n")
                f.write(f"日期：{date_str}\n")
                f.write(f"版面：{version_code} - {version_name}\n")
                f.write(f"链接：{article_url}\n")
                f.write("-" * 50 + "\n\n")
                
                # 先写入作者和字数信息
                for content_type, content in content_parts:
                    if content_type in ['作者', '字数']:
                        f.write(f"{content_type}：{content}\n")
                
                # 再写入正文
                for content_type, content in content_parts:
                    if content_type == '正文':
                        f.write("\n" + content + "\n")

            # 尝试保存图片
            saved_count = 0
            for img in soup.find_all('img'):
                if saved_count >= 5:  # 限制图片数量
                    break
                    
                src = img.get('src')
                if not src:
                    continue
                    
                # 构建完整的图片URL
                img_url = urljoin(article_url, src)
                
                # 过滤掉装饰性图片
                if any(skip in img_url.lower() for skip in ['logo', 'icon', 'banner', 'ad', 'button']):
                    continue
                    
                try:
                    img_resp = self.safe_request(img_url, retries=1)
                    if img_resp and img_resp.content and len(img_resp.content) > 5000:  # 至少5KB
                        # 确定文件扩展名
                        ext = '.jpg'
                        if '.' in img_url.split('/')[-1]:
                            potential_ext = '.' + img_url.split('.')[-1].lower()
                            if potential_ext in ['.jpg', '.jpeg', '.png', '.bmp']:
                                ext = potential_ext
                        
                        img_name = f"{self.clean_filename(title)}_{saved_count + 1}{ext}"
                        img_path = os.path.join(article_dir, img_name)
                        
                        with open(img_path, 'wb') as f:
                            f.write(img_resp.content)
                        
                        saved_count += 1
                        logging.info(f"      已保存图片: {img_name}")
                        
                except Exception as e:
                    logging.warning(f"      保存图片失败: {img_url}, 错误: {str(e)}")
            
            if saved_count > 0:
                logging.info(f"      共保存 {saved_count} 张图片")
            
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
                logging.warning(f"  版面 {version['code']} 没有找到文章")
                return
            
            logging.info(f"  找到 {len(articles)} 篇文章")
            
            success_count = 0
            for article in articles:
                if self.process_article(
                    article['url'], 
                    date_str, 
                    version['code'], 
                    version['name'], 
                    article['title']
                ):
                    success_count += 1
                time.sleep(random.uniform(1, 2))
            
            logging.info(f"  版面 {version['code']} 成功处理 {success_count}/{len(articles)} 篇文章")

        except Exception as e:
            logging.error(f"处理版面失败: {version['code']}, 错误: {str(e)}")

    def crawl_today(self):
        """爬取今天的新闻"""
        date_str = datetime.now().strftime('%Y%m%d')
        logging.info(f"\n{'='*60}")
        logging.info(f"开始处理 {date_str} 的数据...")
        logging.info(f"{'='*60}")

        versions = self.get_version_list(date_str)
        if versions:
            for version in versions:
                self.process_version(version, date_str)
        else:
            logging.warning(f"未找到 {date_str} 的任何版面")

def run_crawler():
    """运行爬虫"""
    try:
        spider = GuangzhouDailyIncrementalSpider()
        spider.crawl_today()
    except Exception as e:
        logging.error(f"爬虫运行出错: {e}")

def schedule_crawler():
    """设置定时任务"""
    logging.info("设置定时任务时间点：06:00, 10:00, 15:32, 18:00, 22:00")
    schedule.every().day.at("06:00").do(run_crawler)
    schedule.every().day.at("10:00").do(run_crawler)
    schedule.every().day.at("15:32").do(run_crawler)
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