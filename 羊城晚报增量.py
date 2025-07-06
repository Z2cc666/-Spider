#!/usr/bin/env python
# -*- coding: UTF-8 -*-
import os
import json
from datetime import datetime, timedelta
import time
import requests
from bs4 import BeautifulSoup
import random
import schedule  # 用于定时任务
import re
from urllib.parse import urljoin, urlparse
import logging
from pathlib import Path

class YangchengEveningNewsIncrementalSpider:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Referer': 'https://ep.ycwb.com/',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
        self.base_url = "https://ep.ycwb.com/epaper/ycwb/html"
        self.base_dir = "羊城晚报"
        self.request_delay = 2
        
        # 记录文件
        self.record_file = "羊城晚报_crawled_articles.json"
        
        # 确保基础目录存在
        Path(self.base_dir).mkdir(exist_ok=True)
        
        # 设置日志
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('羊城晚报_crawler.log', encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
        # 加载已爬取记录
        self.crawled_records = self.load_crawled_records()

    def load_crawled_records(self):
        """加载已爬取的文章记录"""
        try:
            if os.path.exists(self.record_file):
                with open(self.record_file, 'r', encoding='utf-8') as f:
                    records = json.load(f)
                    self.logger.info(f"已加载 {len(records)} 条爬取记录")
                    return records
            else:
                self.logger.info(f"记录文件 {self.record_file} 不存在，将创建新文件")
            return {}
        except Exception as e:
            self.logger.error(f"加载爬取记录失败: {str(e)}")
            return {}

    def save_crawled_records(self):
        """保存已爬取的文章记录"""
        try:
            with open(self.record_file, 'w', encoding='utf-8') as f:
                json.dump(self.crawled_records, f, ensure_ascii=False, indent=2)
        except Exception as e:
            self.logger.error(f"保存爬取记录失败: {str(e)}")

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
                    self.logger.warning(f"页面不存在: {url}")
                    return None
                else:
                    self.logger.warning(f"HTTP {resp.status_code}: {url}")
                    
            except requests.exceptions.Timeout:
                self.logger.warning(f"请求超时 (尝试 {attempt + 1}/{retries}): {url}")
                if attempt < retries - 1:
                    time.sleep(5)
                    continue
            except Exception as e:
                self.logger.error(f"请求失败 (尝试 {attempt + 1}/{retries}): {url}, 错误: {str(e)}")
                if attempt < retries - 1:
                    time.sleep(5)
                    continue
                    
        return None

    def clean_filename(self, filename):
        """清理文件名"""
        return re.sub(r'[<>:"/\\|?*]', '_', filename)[:100].strip()

    def get_main_page(self, date_str):
        """获取主页面，寻找正确的版面链接"""
        try:
            year = date_str[:4]
            month = date_str[4:6]
            day = date_str[6:]
            
            # 构建主页面URL
            main_url = f"{self.base_url}/{year}-{month}/{day}/node_1.htm"
            
            self.logger.info(f"尝试访问主页面: {main_url}")
            resp = self.safe_request(main_url)
            if resp:
                self.logger.info(f"成功访问主页面: {main_url}")
                return resp, main_url
                    
            self.logger.error(f"无法访问 {date_str} 的主页面")
            return None, None
            
        except Exception as e:
            self.logger.error(f"获取主页面失败: {str(e)}")
            return None, None

    def get_version_list(self, date_str):
        """获取版面列表"""
        try:
            resp, main_url = self.get_main_page(date_str)
            if not resp:
                return []

            soup = BeautifulSoup(resp.text, 'html.parser')
            versions = []
            processed = set()

            # 在羊城晚报中，版面信息通常在左侧导航栏中
            # 查找所有可能的版面链接
            for link in soup.find_all('a', href=True):
                href = link.get('href', '')
                text = link.get_text(strip=True)
                
                # 匹配版面格式，如 "(A01) 要闻"
                match = re.match(r'\(?([A-Z]\d+)\)?\s*(.*?)$', text)
                if match and 'node_' in href:
                    code = match.group(1)  # A01, A02 等
                    name = match.group(2).strip()
                    if not name:
                        name = "未知版面"
                    
                    # 构建完整URL
                    if href.startswith('/'):
                        version_url = f"https://ep.ycwb.com{href}"
                    elif href.startswith('http'):
                        version_url = href
                    else:
                        version_url = urljoin(main_url, href)
                    
                    key = f"{code}_{name}"
                    if key not in processed:
                        processed.add(key)
                        versions.append({
                            'code': code,
                            'name': name,
                            'url': version_url
                        })

            versions.sort(key=lambda x: x['code'])
            self.logger.info(f"找到 {len(versions)} 个版面")
            for version in versions:
                self.logger.info(f"  版面: {version['code']} - {version['name']}")
            
            return versions
            
        except Exception as e:
            self.logger.error(f"获取版面列表失败: {str(e)}")
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

            # 查找所有可能的文章链接
            for link in soup.find_all('a', href=True):
                href = link.get('href', '')
                title = link.get_text(strip=True)
                
                # 检查是否是文章链接
                if ('content_' in href and 
                    title and len(title) > 2 and title not in processed):
                    
                    # 过滤掉导航链接
                    if any(skip in title for skip in ['上一版', '下一版', '返回目录', '羊城晚报', '首页']):
                        continue
                    
                    # 构建完整URL
                    if href.startswith('/'):
                        full_url = f"https://ep.ycwb.com{href}"
                    elif href.startswith('http'):
                        full_url = href
                    else:
                        full_url = urljoin(version_url, href)
                    
                    processed.add(title)
                    articles.append({
                        'title': title,
                        'url': full_url
                    })

            self.logger.info(f"在版面中找到 {len(articles)} 篇文章")
            return articles
            
        except Exception as e:
            self.logger.error(f"获取文章列表失败: {str(e)}")
            return []

    def extract_content(self, soup):
        """提取文章内容"""
        content_parts = []
        
        # 1. 获取标题
        title_selectors = [
            'h1', 'h2', 'h3',
            '.title', '.headline', '.article-title',
            '[class*="title"]', '[id*="title"]'
        ]
        
        for selector in title_selectors:
            title_elem = soup.select_one(selector)
            if title_elem:
                title_text = title_elem.get_text(strip=True)
                if title_text and len(title_text) > 3:
                    content_parts.append(('标题', title_text))
                    break
        
        # 2. 获取作者信息
        author_patterns = [
            r'来源[：:]\s*羊城晚报\s*记者\s*(.*?)(?=\n|\s|$)',
            r'记者[：:]\s*(.*?)(?=\n|\s|$)',
            r'通讯员[：:]\s*(.*?)(?=\n|\s|$)',
            r'来源[：:]\s*(.*?)(?=\n|\s|$)',
        ]
        
        for text in soup.stripped_strings:
            for pattern in author_patterns:
                match = re.search(pattern, text)
                if match:
                    content_parts.append(('作者', text.strip()))
                    break
            else:
                continue
            break
        
        # 3. 获取正文内容
        # 尝试多种内容选择器
        content_selectors = [
            '.content', '.article-content', '.text-content',
            '[class*="content"]', '[id*="content"]',
            '.article-body', '.news-content',
            'main', 'article'
        ]
        
        content_container = None
        for selector in content_selectors:
            container = soup.select_one(selector)
            if container and len(container.get_text(strip=True)) > 100:
                content_container = container
                break
        
        # 如果没有找到明确的内容容器，查找包含最多文本的div
        if not content_container:
            divs = soup.find_all('div')
            if divs:
                content_container = max(divs, key=lambda d: len(d.get_text(strip=True)))
        
        if content_container:
            # 提取段落
            paragraphs = []
            for element in content_container.find_all(['p', 'div', 'span']):
                text = element.get_text(strip=True)
                if (text and len(text) > 20 and 
                    not any(skip in text for skip in ['上一篇', '下一篇', '返回', '关于', '联系'])):
                    paragraphs.append(text)
            
            if paragraphs:
                content_parts.append(('正文', '\n\n'.join(paragraphs)))
        
        return content_parts

    def save_images(self, soup, article_dir, title, article_url):
        """保存文章图片"""
        saved_count = 0
        
        for img in soup.find_all('img'):
            if saved_count >= 5:  # 限制图片数量
                break
                
            src = img.get('src')
            if not src:
                continue
                
            # 构建完整的图片URL
            if src.startswith('/'):
                img_url = f"https://ep.ycwb.com{src}"
            elif src.startswith('http'):
                img_url = src
            else:
                img_url = urljoin(article_url, src)
            
            # 过滤掉明显的装饰性图片
            if any(skip in img_url.lower() for skip in ['logo', 'icon', 'banner', 'ad', 'button', '.gif']):
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
                    self.logger.info(f"      已保存图片: {img_name}")
                    
            except Exception as e:
                self.logger.warning(f"      保存图片失败: {img_url}, 错误: {str(e)}")
        
        if saved_count > 0:
            self.logger.info(f"      共保存 {saved_count} 张图片")

    def process_article(self, article_url, date_str, version_code, version_name, title):
        """处理单篇文章"""
        try:
            # 检查文章是否已爬取
            if self.is_article_crawled(date_str, article_url, title):
                self.logger.info(f"    文章已存在，跳过: {title}")
                return self.crawled_records[f"{date_str}_{article_url}_{title}"]['file_path']

            resp = self.safe_request(article_url)
            if not resp:
                return None

            soup = BeautifulSoup(resp.text, 'html.parser')
            
            # 提取内容
            content_parts = self.extract_content(soup)
            
            if not content_parts:
                self.logger.warning(f"未能提取到内容: {title}")
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
                
                for content_type, content in content_parts:
                    if content_type == '正文':
                        f.write(content)
                    else:
                        f.write(f"{content_type}：{content}\n")

            # 尝试保存图片
            self.save_images(soup, article_dir, title, article_url)
            
            # 标记文章为已爬取并立即保存记录
            self.mark_article_crawled(date_str, article_url, title, article_file)
            self.save_crawled_records()  # 立即保存记录
            
            self.logger.info(f"    已保存文章: {title}")
            return True

        except Exception as e:
            self.logger.error(f"处理文章失败: {title}, 错误: {str(e)}")
            return False

    def process_version(self, version, date_str):
        """处理单个版面"""
        try:
            self.logger.info(f"\n开始处理版面: {version['code']} - {version['name']}")
            
            articles = self.get_articles_from_version(version['url'])
            
            if not articles:
                self.logger.warning(f"  版面 {version['code']} 没有找到文章")
                return
            
            self.logger.info(f"  找到 {len(articles)} 篇文章")
            
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
            
            self.logger.info(f"  版面 {version['code']} 成功处理 {success_count}/{len(articles)} 篇文章")

        except Exception as e:
            self.logger.error(f"处理版面失败: {version['code']}, 错误: {str(e)}")

    def crawl_today(self):
        """爬取今天的新闻"""
        today = datetime.now().strftime('%Y%m%d')
        self.logger.info(f"\n开始爬取 {today} 的新闻...")
        self.run(today, today)

    def run(self, start_date, end_date):
        """运行爬虫"""
        try:
            if isinstance(start_date, str):
                start = datetime.strptime(start_date, '%Y-%m-%d')
            else:
                start = start_date
                
            if isinstance(end_date, str):
                end = datetime.strptime(end_date, '%Y-%m-%d')
            else:
                end = end_date
                
            date = start
            
            while date <= end:
                date_str = date.strftime('%Y%m%d')
                self.logger.info(f"\n{'='*60}")
                self.logger.info(f"开始处理 {date_str} 的数据...")
                self.logger.info(f"{'='*60}")

                versions = self.get_version_list(date_str)
                if versions:
                    for version in versions:
                        self.process_version(version, date_str)
                else:
                    self.logger.warning(f"未找到 {date_str} 的任何版面")

                # 每处理完一天的数据就保存一次记录
                self.save_crawled_records()
                
                date += timedelta(days=1)
                time.sleep(5)  # 每天之间暂停5秒
                
            self.logger.info("爬取完成！")
            
        except Exception as e:
            self.logger.error(f"爬虫运行出错: {str(e)}")
        finally:
            self.save_crawled_records()

def run_spider():
    """运行爬虫的定时任务"""
    spider = YangchengEveningNewsIncrementalSpider()
    spider.crawl_today()
    print(f"完成定时爬取任务: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == '__main__':
    # 测试模式
    TEST_MODE = True
    spider = YangchengEveningNewsIncrementalSpider()
    
    if TEST_MODE:
        print("=== 测试模式 ===")
        # 使用固定的测试日期
        test_date = '2025-07-03'  # 使用一个确定存在的日期
        print(f"测试爬取日期: {test_date}")
        spider.run(test_date, test_date)
    else:
        # 正常模式：设置定时任务
        schedule.every().day.at("06:00").do(run_spider)  # 早上6点
        schedule.every().day.at("10:00").do(run_spider)  # 上午10点
        schedule.every().day.at("14:30").do(run_spider)  # 下午2点30分
        schedule.every().day.at("18:00").do(run_spider)  # 下午6点
        schedule.every().day.at("22:00").do(run_spider)  # 晚上10点
        
        print(f"增量爬虫已启动，将在每天 06:00、10:00、14:30、18:00、22:00 运行...")
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