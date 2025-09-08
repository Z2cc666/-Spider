#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
捷太格特电子官网特定模块爬虫
专门爬取：视频、软件下载、认证标志下载
"""

import os
import re
import requests
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
import time
import json
from pathlib import Path

class JtekSpecificSpider:
    def __init__(self):
        self.base_url = "https://www.jtektele.com"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
        # 创建下载目录
        self.download_dir = Path("/srv/downloads/approved/光洋/资料下载")
        self.download_dir.mkdir(parents=True, exist_ok=True)
        
        # 子目录
        (self.download_dir / "教学视频").mkdir(exist_ok=True)
        (self.download_dir / "软件下载").mkdir(exist_ok=True)
        (self.download_dir / "认证标志").mkdir(exist_ok=True)
        
    def download_file(self, url, filepath, file_type="文件"):
        """下载文件"""
        try:
            print(f"正在下载{file_type}: {url}")
            response = self.session.get(url, stream=True, timeout=30)
            response.raise_for_status()
            
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            print(f"✓ {file_type}下载完成: {filepath}")
            return True
            
        except Exception as e:
            print(f"✗ {file_type}下载失败: {url} - {str(e)}")
            return False
    
    def extract_video_info(self, video_div):
        """提取视频信息"""
        video_info = {}
        
        # 查找视频链接 - 只下载mp4格式
        video_links = video_div.find_all('a', href=True)
        for link in video_links:
            href = link.get('href')
            if href and href.lower().endswith('.mp4'):
                video_info['url'] = urljoin(self.base_url, href)
                break
        
        # 查找视频标题
        title_elem = video_div.find(['h3', 'h4', 'h5', 'strong'])
        if title_elem:
            video_info['title'] = title_elem.get_text(strip=True)
        else:
            video_info['title'] = "未命名视频"
            
        return video_info
    
    def download_videos(self, html_content):
        """下载视频文件"""
        print("\n=== 开始下载视频 ===")
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # 查找视频区域
        video_sections = soup.find_all('div', class_='inner_fl_video')
        
        downloaded_count = 0
        for section in video_sections:
            videos = section.find_all('div', class_='fl_video')
            
            for video in videos:
                video_info = self.extract_video_info(video)
                
                if 'url' in video_info:
                    # 生成文件名
                    filename = re.sub(r'[<>:"/\\|?*]', '_', video_info['title'])
                    filename = f"{filename}.mp4"
                    filepath = self.download_dir / "教学视频" / filename
                    
                    if self.download_file(video_info['url'], filepath, "视频"):
                        downloaded_count += 1
                    
                    time.sleep(1)  # 避免请求过快
        
        print(f"视频下载完成，共下载 {downloaded_count} 个文件")
        return downloaded_count
    
    def download_software(self, html_content):
        """下载软件文件"""
        print("\n=== 开始下载软件 ===")
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # 查找软件下载区域
        software_sections = soup.find_all('div', class_='inner_fl_downs')
        
        downloaded_count = 0
        for section in software_sections:
            # 查找软件下载链接
            software_links = section.find_all('a', href=True)
            
            for link in software_links:
                href = link.get('href')
                if href and any(ext in href.lower() for ext in ['.exe', '.zip', '.rar', '.msi', '.dmg']):
                    # 获取软件名称
                    software_name = link.get_text(strip=True)
                    if not software_name:
                        software_name = "未命名软件"
                    
                    # 生成文件名
                    filename = re.sub(r'[<>:"/\\|?*]', '_', software_name)
                    if not any(ext in href.lower() for ext in ['.exe', '.zip', '.rar', '.msi', '.dmg']):
                        filename += ".zip"  # 默认扩展名
                    
                    filepath = self.download_dir / "软件下载" / filename
                    
                    if self.download_file(href, filepath, "软件"):
                        downloaded_count += 1
                    
                    time.sleep(1)
        
        print(f"软件下载完成，共下载 {downloaded_count} 个文件")
        return downloaded_count
    
    def download_certificates(self, html_content):
        """下载认证标志文件"""
        print("\n=== 开始下载认证标志 ===")
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # 查找认证标志下载区域
        cert_sections = soup.find_all('div', class_='inner_fl_downs')
        
        downloaded_count = 0
        for section in cert_sections:
            # 查找PDF下载链接
            pdf_links = section.find_all('a', href=True)
            
            for link in pdf_links:
                href = link.get('href')
                if href and href.lower().endswith('.pdf'):
                    # 获取认证名称
                    cert_name = link.get_text(strip=True)
                    if not cert_name:
                        cert_name = "未命名认证"
                    
                    # 生成文件名
                    filename = re.sub(r'[<>:"/\\|?*]', '_', cert_name)
                    if not filename.lower().endswith('.pdf'):
                        filename += ".pdf"
                    
                    filepath = self.download_dir / "认证标志" / filename
                    
                    if self.download_file(href, filepath, "认证标志"):
                        downloaded_count += 1
                    
                    time.sleep(1)
        
        print(f"认证标志下载完成，共下载 {downloaded_count} 个文件")
        return downloaded_count
    
    def crawl_from_html(self, html_content):
        """从HTML内容爬取"""
        print("开始从HTML内容爬取捷太格特电子官网...")
        
        total_downloaded = 0
        
        # 下载视频
        video_count = self.download_videos(html_content)
        total_downloaded += video_count
        
        # 下载软件
        software_count = self.download_software(html_content)
        total_downloaded += software_count
        
        # 下载认证标志
        cert_count = self.download_certificates(html_content)
        total_downloaded += cert_count
        
        print(f"\n=== 爬取完成 ===")
        print(f"总计下载: {total_downloaded} 个文件")
        print(f"视频: {video_count} 个")
        print(f"软件: {software_count} 个")
        print(f"认证标志: {cert_count} 个")
        print(f"文件保存在: {self.download_dir.absolute()}")
        
        return {
            'videos': video_count,
            'software': software_count,
            'certificates': cert_count,
            'total': total_downloaded
        }
    
    def crawl_from_url(self, url):
        """从URL爬取"""
        try:
            print(f"正在获取页面: {url}")
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            return self.crawl_from_html(response.text)
            
        except Exception as e:
            print(f"获取页面失败: {str(e)}")
            return None

def main():
    """主函数"""
    spider = JtekSpecificSpider()
    
    # 示例HTML内容（您可以将实际的HTML内容粘贴到这里）
    sample_html = """
    <!-- 这里粘贴您提供的HTML内容 -->
    """
    
    print("捷太格特电子官网特定模块爬虫")
    print("=" * 50)
    
    # 选择爬取方式
    choice = input("请选择爬取方式:\n1. 从HTML内容爬取\n2. 从URL爬取\n请输入选择 (1 或 2): ").strip()
    
    if choice == "1":
        print("\n请将HTML内容粘贴到下面（输入完成后按Ctrl+D结束）:")
        html_content = ""
        try:
            while True:
                line = input()
                html_content += line + "\n"
        except EOFError:
            pass
        
        if html_content.strip():
            spider.crawl_from_html(html_content)
        else:
            print("HTML内容为空，无法爬取")
    
    elif choice == "2":
        url = input("请输入要爬取的URL: ").strip()
        if url:
            spider.crawl_from_url(url)
        else:
            print("URL为空，无法爬取")
    
    else:
        print("无效选择")

if __name__ == "__main__":
    main()
