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

class PeoplesDailySpider:
    def __init__(self):
        self.headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Referer': 'http://paper.people.com.cn/',
            'Connection': 'keep-alive',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache',
            'Upgrade-Insecure-Requests': '1',
            'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"'
        }
        
        self.layout_url = "http://paper.people.com.cn/rmrb/pc/layout"  # 版面布局URL
        self.content_url = "http://paper.people.com.cn/rmrb/pc/content"  # 文章内容URL
        self.base_dir = "人民日报"
        self.max_workers = 5
        self.request_delay = 1

    def safe_request(self, url, max_retry=3):
        """安全的网络请求函数"""
        for attempt in range(max_retry):
            try:
                time.sleep(random.uniform(2, 5))
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
                time.sleep(5 * (attempt + 1))
        return None

    def get_version_list(self, date_str, current_url=None):
        """获取版面列表"""
        try:
            formatted_date = f"{date_str[:6]}/{date_str[6:]}"  # YYYYMM/DD
            
            # 使用传入的URL或构建初始URL
            url = current_url or f"{self.layout_url}/{formatted_date}/node_01.html"
            print(f"├── 🔍 访问版面: {url}")
            
            resp = self.safe_request(url)
            if not resp:
                return []

            soup = BeautifulSoup(resp.text, 'html.parser')
            versions = []
            
            # 获取所有版面链接
            for link in soup.find_all('a'):
                href = link.get('href', '')
                text = link.text.strip()
                
                # 检查是否是版面链接
                if 'node_' in href and '.html' in href and '版：' in text:
                    version_code = text.split('版：')[0].strip()
                    version_name = text.split('版：')[1].strip()
                    version_url = f"{self.layout_url}/{formatted_date}/node_{int(version_code):02d}.html"
                    
                    if not any(v['code'] == f"{int(version_code):02d}" for v in versions):
                        versions.append({
                            'code': f"{int(version_code):02d}",
                            'name': version_name,
                            'url': version_url
                        })
                        print(f"│   ├── ✓ 找到版面: {version_code}_{version_name}")
            
            # 如果没有找到版面，尝试直接遍历版面号
            if not versions:
                print("│   ├── 尝试遍历版面...")
                for i in range(1, 21):  # 假设最多20个版面
                    version_url = f"{self.layout_url}/{formatted_date}/node_{i:02d}.html"
                    try:
                        resp = self.safe_request(version_url)
                        if resp and resp.status_code == 200:
                            # 从页面标题获取版面名称
                            soup = BeautifulSoup(resp.text, 'html.parser')
                            title = soup.find('title')
                            if title:
                                title_text = title.text.strip()
                                if '：' in title_text:
                                    version_name = title_text.split('：')[1].split('_')[0]
                                else:
                                    version_name = f"第{i:02d}版"
                                
                                versions.append({
                                    'code': f"{i:02d}",
                                    'name': version_name,
                                    'url': version_url
                                })
                                print(f"│   ├── ✓ 找到版面: {i:02d}_{version_name}")
                    except Exception:
                        continue
            
            # 按版面号排序
            versions.sort(key=lambda x: int(x['code']))
            
            if versions:
                print(f"\n├── 📋 共找到 {len(versions)} 个版面")
            else:
                print(f"├── ⚠️ 未找到任何版面，请检查网址是否正确")
            
            return versions
            
        except Exception as e:
            print(f"├── ❌ 获取版面列表失败: {str(e)}")
            return []

    def create_article_dir(self, date_str, version_code, version_name, title):
        """创建文章目录结构"""
        date_dir = os.path.join(self.base_dir, date_str)
        version_dir = os.path.join(date_dir, f"{version_code}_{version_name}")
        article_dir = os.path.join(version_dir, self.clean_filename(title))
        os.makedirs(article_dir, exist_ok=True)
        return article_dir

    def clean_filename(self, filename):
        """清理文件名中的非法字符"""
        invalid_chars = '<>:"/\\|?*'
        for char in invalid_chars:
            filename = filename.replace(char, '_')
        return filename.strip()

    def process_article(self, article_url, date_str, version_code, version_name):
        """处理单篇文章"""
        try:
            resp = self.safe_request(article_url)
            if not resp:
                return None

            soup = BeautifulSoup(resp.text, 'html.parser')
            
            # 获取文章标题和内容
            title = None
            content = None
            
            # 1. 尝试从meta标签获取标题
            meta_title = soup.find('meta', {'name': 'ArticleTitle'}) or soup.find('meta', {'property': 'og:title'})
            if meta_title:
                title = meta_title.get('content', '').strip()
            
            # 2. 尝试从标题标签获取
            if not title:
                # 首先尝试特定的class名称
                for class_name in ['article-title', 'title', 'art_title', 'main-title']:
                    title_elem = soup.find(class_=class_name)
                    if title_elem:
                        title = title_elem.get_text(strip=True)
                        if title:
                            break
                
                # 如果还没找到，尝试h1-h3标签
                if not title:
                    for tag in ['h1', 'h2', 'h3']:
                        title_elem = soup.find(tag)
                        if title_elem:
                            title = title_elem.get_text(strip=True)
                            if title:
                                break
                
                # 如果还没找到，尝试包含"title"或"heading"的class
                if not title:
                    for elem in soup.find_all(class_=True):
                        classes = elem.get('class', [])
                        if any('title' in c.lower() or 'heading' in c.lower() for c in classes):
                            title = elem.get_text(strip=True)
                            if title:
                                break
            
            # 3. 尝试从页面结构中查找标题
            if not title:
                # 查找页面中最显著的文本（通常是标题）
                candidates = []
                for tag in soup.find_all(['div', 'p', 'span']):
                    text = tag.get_text(strip=True)
                    if text and 2 < len(text) < 100:  # 标题通常不会太长或太短
                        # 检查是否有标题特征（字体大小、粗体等）
                        style = tag.get('style', '').lower()
                        if ('font-size' in style and 'px' in style) or 'font-weight' in style:
                            candidates.append((text, len(tag.find_all()), tag.get('style', '')))
                
                if candidates:
                    # 选择最可能是标题的文本（优先考虑样式特征和嵌套深度）
                    candidates.sort(key=lambda x: (-len(x[2]), x[1]))  # 样式多的优先，嵌套少的优先
                    title = candidates[0][0]
            
            # 4. 如果还是没找到标题，尝试使用传入的标题
            if not title and 'title' in article_url:
                # 从URL中提取标题部分
                title_match = re.search(r'/([^/]+?)(?:\.html?)?$', article_url)
                if title_match:
                    title = title_match.group(1)
                    title = title.replace('_', ' ').replace('-', ' ')
            
            if not title:
                print(f"    │   └── ❌ 未找到文章标题")
                return None
            
            # 清理标题
            title = re.sub(r'\s+', ' ', title)  # 合并多个空格
            title = title.strip('　 \t\r\n')  # 移除中英文空格和换行符
            if not title:
                print(f"    │   └── ❌ 标题为空")
                return None
            
            # 查找文章内容
            # 1. 尝试找特定ID的内容区域
            content = soup.find('div', id='ozoom') or soup.find('div', id='articleContent')
            
            # 2. 尝试找特定class的内容区域
            if not content:
                for class_name in ['article', 'article-content', 'text', 'content']:
                    content = soup.find(class_=class_name)
                    if content and len(content.get_text(strip=True)) > 200:
                        break
            
            # 3. 如果还没找到，查找最长的文本块
            if not content:
                max_length = 200  # 最小内容长度阈值
                for div in soup.find_all('div'):
                    text = div.get_text(strip=True)
                    if len(text) > max_length:
                        max_length = len(text)
                        content = div
            
            if content:
                # 清理内容
                for tag in content.find_all(['script', 'style']):
                    tag.decompose()
                
                text = content.get_text(separator='\n', strip=True)
                
                # 保存文章文本
                clean_title = self.clean_filename(title)
                article_dir = self.create_article_dir(date_str, version_code, version_name, clean_title)
                text_file = os.path.join(article_dir, f"{clean_title}.txt")
                
                with open(text_file, 'w', encoding='utf-8') as f:
                    f.write(f"标题：{title}\n")
                    f.write(f"日期：{date_str}\n")
                    f.write(f"版面：{version_code}_{version_name}\n")
                    f.write(f"链接：{article_url}\n\n")
                    f.write("正文：\n")
                    f.write(text)
                
                print(f"    │   ├── 📄 已保存: {title[:30]}...")
                
                # 保存文章中的图片
                saved_images = 0
                # 只查找文章内容区域中的图片
                images = content.find_all('img')
                
                for i, img in enumerate(images, 1):
                    # 获取图片URL
                    img_url = img.get('src', '')
                    if not img_url:
                        continue
                    
                    # 过滤掉装饰性图片
                    # 1. 检查图片尺寸（如果有）
                    width = img.get('width', '0')
                    height = img.get('height', '0')
                    try:
                        w = int(width) if str(width).isdigit() else 0
                        h = int(height) if str(height).isdigit() else 0
                        if 0 < w < 50 or 0 < h < 50:  # 过滤掉小图标
                            continue
                    except ValueError:
                        pass
                    
                    # 2. 检查图片URL关键词
                    skip_keywords = ['icon', 'logo', 'button', 'bg', 'background', 'banner', 'nav', 
                                  'd1.gif', 'd.gif', 'dot', 'line', 'split', 'div']
                    if any(keyword in img_url.lower() for keyword in skip_keywords):
                        continue
                    
                    # 3. 检查图片alt和title
                    img_alt = img.get('alt', '').strip()
                    img_title = img.get('title', '').strip()
                    if any(keyword in (img_alt + img_title).lower() for keyword in skip_keywords):
                        continue
                        
                    # 处理相对URL
                    if not img_url.startswith('http'):
                        if img_url.startswith('//'):
                            img_url = 'http:' + img_url
                        elif img_url.startswith('/'):
                            img_url = 'http://paper.people.com.cn' + img_url
                        else:
                            base_url = '/'.join(article_url.split('/')[:-1])
                            img_url = f"{base_url}/{img_url}"
                    
                    # 获取图片说明文字
                    caption = img_alt or img_title or f'图片_{i}'
                    caption = self.clean_filename(caption)
                    
                    # 获取图片扩展名
                    img_ext = os.path.splitext(img_url)[1]
                    if not img_ext or img_ext.lower() not in ['.jpg', '.jpeg', '.png', '.gif']:
                        img_ext = '.jpg'  # 默认扩展名
                    
                    # 保存在文章目录下
                    img_path = os.path.join(article_dir, f"{caption}{img_ext}")
                    
                    # 如果文件已存在，添加序号
                    if os.path.exists(img_path):
                        img_path = os.path.join(article_dir, f"{caption}_{i}{img_ext}")
                    
                    # 下载并保存图片
                    try:
                        img_resp = self.safe_request(img_url)
                        if img_resp and img_resp.content:
                            # 检查文件大小
                            if len(img_resp.content) < 1024:  # 跳过小于1KB的图片
                                continue
                            
                            with open(img_path, 'wb') as f:
                                f.write(img_resp.content)
                            saved_images += 1
                    except Exception as e:
                        continue
                
                if saved_images > 0:
                    print(f"    │   ├── 🖼️ 已保存 {saved_images} 张图片")
                print(f"    │   └── ✅ 完成")
                return text_file
            
            print(f"    │   └── ❌ 未找到文章内容: {title[:30]}...")
            return None
            
        except Exception as e:
            print(f"    │   └── ❌ 处理失败: {str(e)}")
            return None

    def download_image(self, url, filepath):
        """下载图片"""
        try:
            resp = self.safe_request(url)
            if resp and resp.content:
                with open(filepath, 'wb') as f:
                    f.write(resp.content)
                return True
            return False
        except Exception as e:
            print(f"    ├── ⚠️ 下载图片失败: {url}")
            print(f"    ├── ⚠️ 错误: {str(e)}")
            return False

    def get_articles_from_version(self, version_url):
        """获取版面中的所有文章链接"""
        try:
            resp = self.safe_request(version_url)
            if not resp:
                return []

            soup = BeautifulSoup(resp.text, 'html.parser')
            articles = []
            
            # 提取日期信息从版面URL
            date_match = re.search(r'/(\d{6})/(\d{2})/', version_url)
            if not date_match:
                return []
                
            yyyymm, dd = date_match.groups()
            
            # 查找所有可能的文章链接
            for link in soup.find_all(['a', 'div']):  # 同时查找a标签和div标签
                # 检查是否是文章链接
                onclick = link.get('onclick', '')
                href = link.get('href', '')
                
                # 获取标题
                title = None
                # 1. 从文本内容获取
                title_text = link.get_text(strip=True)
                if title_text and len(title_text) > 2:  # 忽略太短的标题
                    title = title_text
                # 2. 从title属性获取
                if not title:
                    title = link.get('title', '').strip()
                
                if not title:
                    continue
                
                # 尝试获取content_id
                content_id = None
                
                # 1. 从onclick中获取
                onclick_match = re.search(r'content_(\d+)', onclick)
                if onclick_match:
                    content_id = onclick_match.group(1)
                
                # 2. 从href中获取
                if not content_id and href:
                    href_match = re.search(r'content_(\d+)', href)
                    if href_match:
                        content_id = href_match.group(1)
                
                # 3. 从父元素或子元素中查找
                if not content_id:
                    # 检查父元素
                    parent = link.parent
                    if parent:
                        parent_onclick = parent.get('onclick', '')
                        parent_match = re.search(r'content_(\d+)', parent_onclick)
                        if parent_match:
                            content_id = parent_match.group(1)
                    
                    # 检查子元素
                    if not content_id:
                        for child in link.find_all(['a', 'div']):
                            child_onclick = child.get('onclick', '')
                            child_match = re.search(r'content_(\d+)', child_onclick)
                            if child_match:
                                content_id = child_match.group(1)
                                break
                
                if content_id and title:
                    article_url = f"{self.content_url}/{yyyymm}/{dd}/content_{content_id}.html"
                    if not any(a['url'] == article_url for a in articles):  # 避免重复
                        articles.append({
                            'title': title,
                            'url': article_url
                        })
                        print(f"    ├── 📄 找到文章: {title}")
            
            return articles
            
        except Exception as e:
            print(f"    ├── ❌ 获取文章列表失败: {str(e)}")
            return []

    def process_version(self, version, date_str):
        """处理单个版面"""
        try:
            print(f"\n└── 版面: {version['code']}_{version['name']}")
            
            # 获取版面中的所有文章
            articles = self.get_articles_from_version(version['url'])
            if not articles:
                print(f"    └── ⚠️ 未找到文章")
                return
            
            # 处理每篇文章
            for article in articles:
                try:
                    self.process_article(article['url'], date_str, version['code'], version['name'])
                    time.sleep(self.request_delay)
                except Exception as e:
                    print(f"    ├── ❌ 处理文章失败: {article['title']}")
                    print(f"    └── 错误: {str(e)}")
            
        except Exception as e:
            print(f"└── ❌ 处理版面失败: {version['url']}")
            print(f"    └── 错误: {str(e)}")

    def run(self, start_date, end_date):
        """运行爬虫"""
        try:
            date_range = pd.date_range(start=start_date, end=end_date, freq='D')
            
            for date in date_range:
                date_str = date.strftime('%Y%m%d')
                print(f"\n📅 {date_str}")
                
                # 获取版面列表
                versions = self.get_version_list(date_str)
                if not versions:
                    print(f"└── ❌ 未获取到版面列表")
                    continue

                # 处理每个版面
                for version in versions:
                    self.process_version(version, date_str)
                    time.sleep(self.request_delay)

                print(f"\n└── ✅ 完成 {date_str} 的数据处理")
                time.sleep(5)

        except Exception as e:
            print(f"\n❌ 爬虫运行出错: {str(e)}")
        finally:
            print("\n🏁 爬取任务完成！")

if __name__ == '__main__':
    # 测试模式
    TEST_MODE = True
    spider = PeoplesDailySpider()
    
    if TEST_MODE:
        print("=== 测试模式 ===")
        # 使用固定的测试日期
        test_date = '2025-07-02'  # 使用一个确定存在的日期
        print(f"测试爬取日期: {test_date}")
        spider.run(test_date, test_date)
    else:
        # 直接指定日期范围
        start_date = '2025-07-01'
        end_date = '2025-07-02'
        print(f"开始爬取从 {start_date} 到 {end_date} 的新闻...")
        spider.run(start_date, end_date)
