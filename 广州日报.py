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
import logging
from pathlib import Path

class GuangzhouDailySpider:
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
        self.request_delay = 2
        
        # 确保基础目录存在
        Path(self.base_dir).mkdir(exist_ok=True)
        
        # 设置日志
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)

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
        """获取主页面"""
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

            self.logger.info(f"在版面中找到 {len(articles)} 篇文章")
            return articles
            
        except Exception as e:
            self.logger.error(f"获取文章列表失败: {str(e)}")
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
        try:
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
                
                # 先写入作者和字数信息
                for content_type, content in content_parts:
                    if content_type in ['作者', '字数']:
                        f.write(f"{content_type}：{content}\n")
                
                # 再写入正文
                for content_type, content in content_parts:
                    if content_type == '正文':
                        f.write("\n" + content + "\n")

            # 尝试保存图片
            self.save_images(soup, article_dir, title, article_url)
            
            self.logger.info(f"    已保存文章: {title}")
            return True

        except Exception as e:
            self.logger.error(f"处理文章失败: {title}, 错误: {str(e)}")
            return False

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
                    self.logger.info(f"      已保存图片: {img_name}")
                    
            except Exception as e:
                self.logger.warning(f"      保存图片失败: {img_url}, 错误: {str(e)}")
        
        if saved_count > 0:
            self.logger.info(f"      共保存 {saved_count} 张图片")

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

    def run(self, start_date, end_date):
        """运行爬虫"""
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
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

            date += timedelta(days=1)
            
        self.logger.info("爬取完成！")

if __name__ == '__main__':
    spider = GuangzhouDailySpider()
    start_date = '2025-07-07'
    end_date = '2025-07-07'
    print(f"开始爬取从 {start_date} 到 {end_date} 的新闻...")
    spider.run(start_date, end_date)
