#!/usr/bin/env python
# -*- coding: UTF-8 -*-
import os
import json
import logging
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import schedule
import pandas as pd
from bs4 import BeautifulSoup
from 人民日报 import PeoplesDailySpider

class IncrementalPeoplesDailySpider(PeoplesDailySpider):
    def __init__(self):
        super().__init__()
        self.record_file = "rmrb_crawled_articles.json"
        self.crawled_records = self.load_crawled_records()
        self.setup_logging()

    def setup_logging(self):
        """设置日志"""
        log_dir = "logs"
        os.makedirs(log_dir, exist_ok=True)
        
        log_file = os.path.join(log_dir, f"rmrb_spider_{datetime.now().strftime('%Y%m%d')}.log")
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s [%(levelname)s] %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )

    def load_crawled_records(self):
        """加载已爬取的文章记录"""
        try:
            if os.path.exists(self.record_file):
                with open(self.record_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            return {}
        except Exception as e:
            logging.error(f"加载爬取记录失败: {str(e)}")
            return {}

    def save_crawled_records(self):
        """保存已爬取的文章记录"""
        try:
            with open(self.record_file, 'w', encoding='utf-8') as f:
                json.dump(self.crawled_records, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logging.error(f"保存爬取记录失败: {str(e)}")

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
            
            # 2. 尝试从h1或h2标签获取标题
            if not title:
                title_elem = soup.find('h1') or soup.find('h2')
                if title_elem:
                    title = title_elem.get_text(strip=True)
            
            # 3. 尝试从特定class获取标题
            if not title:
                for class_name in ['article-title', 'title', 'art_title', 'main-title', 'title_word']:
                    title_elem = soup.find(class_=class_name)
                    if title_elem:
                        title = title_elem.get_text(strip=True)
                        if title:
                            break

            # 4. 尝试从页面标题获取
            if not title:
                page_title = soup.find('title')
                if page_title:
                    title_text = page_title.get_text(strip=True)
                    # 移除网站名称等额外信息
                    title = title_text.split('_')[0].split('-')[0].split('|')[0].strip()
            
            if not title:
                logging.error(f"未找到文章标题: {article_url}")
                return None

            # 检查文章是否已爬取
            if self.is_article_crawled(date_str, article_url, title):
                logging.info(f"文章已存在，跳过: {title}")
                return self.crawled_records[f"{date_str}_{article_url}_{title}"]['file_path']
            
            # 查找文章内容
            content = None
            
            # 1. 尝试从id获取内容
            for content_id in ['ozoom', 'articleContent', 'article', 'content', 'mainContent']:
                content = soup.find('div', id=content_id)
                if content and len(content.get_text(strip=True)) > 100:
                    break
            
            # 2. 尝试从class获取内容
            if not content:
                for class_name in ['article', 'article-content', 'text', 'content', 'article_content']:
                    content = soup.find(['div', 'article'], class_=class_name)
                    if content and len(content.get_text(strip=True)) > 100:
                        break

            # 3. 尝试查找最长的文本块
            if not content:
                text_blocks = soup.find_all(['div', 'article', 'section'])
                if text_blocks:
                    content = max(text_blocks, key=lambda x: len(x.get_text(strip=True)))
                    if len(content.get_text(strip=True)) < 100:
                        content = None

            if not content:
                logging.error(f"未找到文章内容: {article_url}")
                return None

            # 清理内容
            for tag in content.find_all(['script', 'style', 'iframe', 'button', 'input', 'meta']):
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
            
            logging.info(f"已保存文章: {title}")
            
            # 保存文章中的图片
            saved_images = 0
            images = content.find_all('img')
            
            for i, img in enumerate(images, 1):
                img_url = img.get('src', '')
                if not img_url:
                    continue
                
                # 过滤掉装饰性图片
                skip_keywords = ['icon', 'logo', 'button', 'bg', 'background', 'banner', 'nav', 
                               'd1.gif', 'd.gif', 'dot', 'line', 'split', 'div']
                if any(keyword in img_url.lower() for keyword in skip_keywords):
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
                img_alt = img.get('alt', '').strip()
                img_title = img.get('title', '').strip()
                caption = img_alt or img_title or f'图片_{i}'
                caption = self.clean_filename(caption)
                
                # 保存图片
                img_path = os.path.join(article_dir, f"{caption}.jpg")
                if os.path.exists(img_path):
                    img_path = os.path.join(article_dir, f"{caption}_{i}.jpg")
                
                if self.download_image(img_url, img_path):
                    saved_images += 1
            
            if saved_images > 0:
                logging.info(f"已保存 {saved_images} 张图片")
            
            # 标记文章为已爬取
            self.mark_article_crawled(date_str, article_url, title, text_file)
            self.save_crawled_records()  # 每篇文章保存后就更新记录
            
            return text_file
            
        except Exception as e:
            logging.error(f"处理文章失败: {article_url}, 错误: {str(e)}")
            return None

    def crawl_today(self):
        """爬取今天的新闻"""
        today = datetime.now().strftime('%Y-%m-%d')
        logging.info(f"开始爬取 {today} 的新闻...")
        self.run(today, today)

    def run(self, start_date, end_date):
        """运行爬虫（增量版本）"""
        try:
            date_range = pd.date_range(start=start_date, end=end_date, freq='D')
            
            for date in date_range:
                date_str = date.strftime('%Y%m%d')
                logging.info(f"\n📅 开始处理 {date_str}")
                
                versions = self.get_version_list(date_str)
                if not versions:
                    logging.error(f"未获取到版面列表")
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
                            logging.error(f"版面处理任务失败: {str(e)}")

                self.save_crawled_records()
                logging.info(f"完成 {date_str} 的数据处理")
                time.sleep(5)

        except Exception as e:
            logging.error(f"爬虫运行出错: {str(e)}")
        finally:
            self.save_crawled_records()
            logging.info("已完成爬取任务！")

def run_spider():
    """运行爬虫的定时任务"""
    spider = IncrementalPeoplesDailySpider()
    spider.crawl_today()
    logging.info(f"完成定时爬取任务: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == '__main__':
    # 测试模式
    TEST_MODE = False
    spider = IncrementalPeoplesDailySpider()
    
    if TEST_MODE:
        print("=== 测试模式 ===")
        # 使用固定的测试日期
        test_date = '2025-06-30'  # 使用一个确定存在的日期
        print(f"测试爬取日期: {test_date}")
        spider.run(test_date, test_date)
    else:
        # 正常模式：设置定时任务
        schedule.every().day.at("08:35").do(run_spider)  # 凌晨2点
        schedule.every().day.at("10:00").do(run_spider)  # 上午10点
        schedule.every().day.at("22:31").do(run_spider)  # 晚上6点
        
        logging.info(f"增量爬虫已启动，将在每天 02:00、10:00、18:00 运行...")
        logging.info(f"当前时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 先执行一次，爬取当天的内容
        logging.info("执行首次爬取...")
        run_spider()
        
        # 运行定时任务循环
        while True:
            try:
                schedule.run_pending()
                time.sleep(60)  # 每分钟检查一次
            except KeyboardInterrupt:
                logging.info("\n爬虫已停止运行")
                break
            except Exception as e:
                logging.error(f"发生错误: {str(e)}")
                time.sleep(300)  # 发生错误时等待5分钟后继续 