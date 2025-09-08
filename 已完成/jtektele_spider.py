#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
捷太格特电子官网爬虫
专门爬取：软件下载、认证标志下载、YKAN次世代HMI介绍视频第二页
支持包含'/'字符的文件名下载
"""

import os
import re
import time
import requests
from urllib.parse import urljoin, urlparse, unquote
from bs4 import BeautifulSoup
import logging
from pathlib import Path
import hashlib

class JtekteleSpider:
    def __init__(self, base_url="https://www.jtektele.com.cn"):
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
        
        # 设置下载目录
        self.download_dir = Path("downloads/jtektele")
        self.download_dir.mkdir(parents=True, exist_ok=True)
        
        # 设置日志
        self.setup_logging()
        
    def setup_logging(self):
        """设置日志"""
        log_file = self.download_dir / "jtektele_spider.log"
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
    def safe_filename(self, filename):
        """处理包含'/'等特殊字符的文件名"""
        # 替换Windows和Unix系统不允许的字符
        filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
        # 处理多个连续的下划线
        filename = re.sub(r'_+', '_', filename)
        # 移除首尾的下划线
        filename = filename.strip('_')
        return filename
    
    def get_page_content(self, url):
        """获取页面内容"""
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            response.encoding = 'utf-8'
            return response.text
        except Exception as e:
            self.logger.error(f"获取页面失败 {url}: {e}")
            return None
    
    def download_file(self, file_url, filename, category):
        """下载文件"""
        try:
            # 创建分类目录
            category_dir = self.download_dir / category
            category_dir.mkdir(exist_ok=True)
            
            # 处理文件名
            safe_filename = self.safe_filename(filename)
            file_path = category_dir / safe_filename
            
            # 如果文件已存在，跳过下载
            if file_path.exists():
                self.logger.info(f"文件已存在，跳过: {safe_filename}")
                return True
            
            # 下载文件
            self.logger.info(f"开始下载: {filename}")
            response = self.session.get(file_url, stream=True, timeout=60)
            response.raise_for_status()
            
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            self.logger.info(f"下载完成: {safe_filename}")
            return True
            
        except Exception as e:
            self.logger.error(f"下载失败 {filename}: {e}")
            return False
    
    def crawl_software_downloads(self):
        """爬取软件下载页面"""
        self.logger.info("开始爬取软件下载页面...")
        
        # 软件下载页面URL
        software_url = f"{self.base_url}/index.php/download/software"
        content = self.get_page_content(software_url)
        
        if not content:
            return
        
        soup = BeautifulSoup(content, 'html.parser')
        
        # 查找下载链接
        download_links = soup.find_all('a', href=True)
        
        for link in download_links:
            href = link.get('href')
            if href and any(ext in href.lower() for ext in ['.exe', '.zip', '.rar', '.pdf', '.doc', '.xls']):
                file_url = urljoin(software_url, href)
                filename = link.get_text(strip=True) or os.path.basename(href)
                
                if filename:
                    self.download_file(file_url, filename, "软件下载")
                    time.sleep(1)  # 避免请求过快
    
    def crawl_certification_downloads(self):
        """爬取认证标志下载页面"""
        self.logger.info("开始爬取认证标志下载页面...")
        
        # 认证标志下载页面URL
        cert_url = f"{self.base_url}/index.php/download/certification"
        content = self.get_page_content(cert_url)
        
        if not content:
            return
        
        soup = BeautifulSoup(content, 'html.parser')
        
        # 查找下载链接
        download_links = soup.find_all('a', href=True)
        
        for link in download_links:
            href = link.get('href')
            if href and any(ext in href.lower() for ext in ['.pdf', '.jpg', '.png', '.zip', '.rar']):
                file_url = urljoin(cert_url, href)
                filename = link.get_text(strip=True) or os.path.basename(href)
                
                if filename:
                    self.download_file(file_url, filename, "认证标志下载")
                    time.sleep(1)
    
    def crawl_ykan_videos_page2(self):
        """爬取YKAN次世代HMI介绍视频第二页"""
        self.logger.info("开始爬取YKAN次世代HMI介绍视频第二页...")
        
        # 第二页URL
        video_url = "https://www.jtektele.com.cn/index.php/video/40/2"
        content = self.get_page_content(video_url)
        
        if not content:
            return
        
        soup = BeautifulSoup(content, 'html.parser')
        
        # 查找视频链接
        video_links = soup.find_all('a', href=True)
        
        for link in video_links:
            href = link.get('href')
            text = link.get_text(strip=True)
            
            # 检查是否是YKAN相关视频
            if href and ('ykan' in text.lower() or 'YKAN' in text):
                file_url = urljoin(video_url, href)
                filename = text or os.path.basename(href)
                
                if filename:
                    self.download_file(file_url, filename, "YKAN视频第二页")
                    time.sleep(1)
        
        # 也查找页面中的其他下载链接
        download_links = soup.find_all('a', href=True)
        
        for link in download_links:
            href = link.get('href')
            if href and any(ext in href.lower() for ext in ['.mp4', '.avi', '.mov', '.wmv', '.flv', '.pdf', '.zip']):
                file_url = urljoin(video_url, href)
                filename = link.get_text(strip=True) or os.path.basename(href)
                
                if filename:
                    self.download_file(file_url, filename, "YKAN视频第二页")
                    time.sleep(1)
    
    def crawl_all(self):
        """爬取所有内容"""
        self.logger.info("开始爬取捷太格特电子官网...")
        
        try:
            # 爬取软件下载
            self.crawl_software_downloads()
            time.sleep(2)
            
            # 爬取认证标志下载
            self.crawl_certification_downloads()
            time.sleep(2)
            
            # 爬取YKAN视频第二页
            self.crawl_ykan_videos_page2()
            
            self.logger.info("所有内容爬取完成！")
            
        except Exception as e:
            self.logger.error(f"爬取过程中出现错误: {e}")
    
    def get_download_summary(self):
        """获取下载统计信息"""
        total_files = 0
        category_counts = {}
        
        for category_dir in self.download_dir.iterdir():
            if category_dir.is_dir():
                files = list(category_dir.glob('*'))
                category_counts[category_dir.name] = len(files)
                total_files += len(files)
        
        self.logger.info(f"下载统计:")
        self.logger.info(f"总文件数: {total_files}")
        for category, count in category_counts.items():
            self.logger.info(f"  {category}: {count} 个文件")
        
        return total_files, category_counts

def main():
    """主函数"""
    spider = JtekteleSpider()
    
    # 开始爬取
    spider.crawl_all()
    
    # 显示统计信息
    spider.get_download_summary()

if __name__ == "__main__":
    main()
