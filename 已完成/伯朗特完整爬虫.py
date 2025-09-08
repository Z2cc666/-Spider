#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import os, time, json, requests, chardet
import hmac, base64, urllib.parse, hashlib
from datetime import datetime, date
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import pickle
import re
import shutil
import platform

# Selenium相关导入
try:
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    from selenium.common.exceptions import TimeoutException, NoSuchElementException
    from webdriver_manager.chrome import ChromeDriverManager
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False
    print("⚠️ Selenium未安装，无法使用浏览器自动化")

class BorunterCompleteSpider:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3',
        }
        
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        
        self.base_url = "https://www.borunte.com"
        
        # 7个下载模块的配置
        self.download_modules = {
            "案例下载": {
                "url": "https://www.borunte.com/downloadmaterial",
                "selector": "案例下载",
                "category": "案例资料"
            },
            "文件下载": {
                "url": "https://www.borunte.com/downloadmaterial", 
                "selector": "文件下载",
                "category": "技术文档"
            },
            "图片下载": {
                "url": "https://www.borunte.com/downloadmaterial",
                "selector": "图片下载", 
                "category": "产品图片"
            },
            "视频下载": {
                "url": "https://www.borunte.com/downloadmaterial",
                "selector": "视频下载",
                "category": "产品视频"
            },
            "工装资料下载": {
                "url": "https://www.borunte.com/downloadmaterial",
                "selector": "工装资料下载",
                "category": "工装资料"
            },
            "机器人3D模型下载": {
                "url": "https://www.borunte.com/downloadmaterial",
                "selector": "机器人3D模型下载", 
                "category": "机器人3D模型"
            },
            "机械手3D模型下载": {
                "url": "https://www.borunte.com/downloadmaterial",
                "selector": "机械手3D模型下载",
                "category": "机械手3D模型"
            }
        }
        
        # 根据环境选择存储路径
        if platform.system() == "Darwin":  # Mac系统（本地测试）
            self.base_dir = os.path.join(os.getcwd(), "downloads", "伯朗特")
        elif platform.system() == "Windows":  # Windows系统（本地测试）
            self.base_dir = os.path.join(os.getcwd(), "downloads", "伯朗特")
        else:  # Linux系统（服务器环境）
            self.base_dir = "/srv/downloads/approved/伯朗特"
            
        self.processed_urls = self.load_processed_urls()
        self.processed_files = self.load_processed_files()
        self.new_files = []
        self.updated_files = []
        self.debug = True
        
        # 钉钉配置
        self.ACCESS_TOKEN = "1a431b6482e59ec652a6eab37705d50fd282dc426f0b829b3eb9a9c1b277ed24"
        self.SECRET = "SECc5e2168011dd97d4c511e06832d172f31635ef635771668e4e6003fce23c07bb"
        
        # 判断是否首次运行
        self.is_first_run = not os.path.exists(os.path.join(self.base_dir, 'processed_urls.pkl'))
        
        # 时间过滤条件：2024年11月1日
        self.filter_date = datetime(2024, 11, 1)
        
        # Selenium驱动
        self.driver = None

    def setup_selenium(self):
        """设置Selenium WebDriver"""
        if not SELENIUM_AVAILABLE:
            print("❌ Selenium不可用，无法使用浏览器自动化")
            return False
            
        try:
            print("🌐 初始化Chrome浏览器...")
            
            chrome_options = Options()
            chrome_options.add_argument('--headless')  # 无头模式
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--disable-web-security')
            chrome_options.add_argument('--disable-features=VizDisplayCompositor')
            chrome_options.add_argument('--window-size=1920,1080')
            chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
            chrome_options.add_argument('--ignore-certificate-errors')
            chrome_options.add_argument('--ignore-ssl-errors')
            chrome_options.add_argument('--ignore-certificate-errors-spki-list')
            # 强制设置中文语言
            chrome_options.add_argument('--lang=zh-CN')
            chrome_options.add_argument('--accept-lang=zh-CN,zh,en-US,en')
            chrome_options.add_experimental_option('prefs', {
                'intl.accept_languages': 'zh-CN,zh,en-US,en'
            })
            
            # 根据系统类型配置Chrome路径
            if platform.system() == "Darwin":  # Mac系统
                chrome_path = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
                if os.path.exists(chrome_path):
                    chrome_options.binary_location = chrome_path
            elif platform.system() == "Linux":  # Linux服务器
                # 在Linux服务器上，Chrome通常安装在这些位置
                possible_chrome_paths = [
                    "/usr/bin/google-chrome",
                    "/usr/bin/google-chrome-stable", 
                    "/usr/bin/chromium-browser",
                    "/usr/bin/chromium"
                ]
                for chrome_path in possible_chrome_paths:
                    if os.path.exists(chrome_path):
                        chrome_options.binary_location = chrome_path
                        break
            
            # 使用webdriver-manager自动下载驱动，添加重试机制
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    print(f"🔄 尝试安装ChromeDriver (第{attempt + 1}次/共{max_retries}次)...")
                    
                    # 设置webdriver-manager的超时和缓存
                    os.environ['WDM_LOG_LEVEL'] = '0'  # 减少日志输出
                    os.environ['WDM_TIMEOUT'] = '60'   # 设置60秒超时
                    
                    service = Service(ChromeDriverManager().install())
                    self.driver = webdriver.Chrome(service=service, options=chrome_options)
                    print("✅ Chrome浏览器初始化成功")
                    return True
                    
                except Exception as e:
                    print(f"⚠️ 第{attempt + 1}次尝试失败: {e}")
                    if attempt < max_retries - 1:
                        import time
                        print(f"⏳ 等待5秒后重试...")
                        time.sleep(5)
                    else:
                        print("❌ 所有重试都失败了，尝试使用系统chromedriver...")
                        
                        # 尝试使用系统已安装的chromedriver
                        try:
                            system_drivers = [
                                "/usr/bin/chromedriver",
                                "/usr/local/bin/chromedriver",
                                "chromedriver"  # PATH中的chromedriver
                            ]
                            
                            for driver_path in system_drivers:
                                try:
                                    if driver_path == "chromedriver" or os.path.exists(driver_path):
                                        print(f"🔧 尝试使用系统chromedriver: {driver_path}")
                                        service = Service(driver_path)
                                        self.driver = webdriver.Chrome(service=service, options=chrome_options)
                                        print("✅ 使用系统chromedriver初始化成功")
                                        return True
                                except Exception as sys_e:
                                    print(f"⚠️ 系统chromedriver失败: {sys_e}")
                                    continue
                                    
                        except Exception as final_e:
                            print(f"❌ 系统chromedriver也失败了: {final_e}")
            
            return False
                
        except Exception as e:
            print(f"❌ Selenium设置失败: {e}")
            return False

    def load_processed_urls(self):
        """加载已处理的URL"""
        processed_file = os.path.join(self.base_dir, 'processed_urls.pkl')
        if os.path.exists(processed_file):
            try:
                with open(processed_file, 'rb') as f:
                    return pickle.load(f)
            except:
                pass
        return set()

    def save_processed_urls(self):
        """保存已处理的URL"""
        os.makedirs(self.base_dir, exist_ok=True)
        processed_file = os.path.join(self.base_dir, 'processed_urls.pkl')
        try:
            with open(processed_file, 'wb') as f:
                pickle.dump(self.processed_urls, f)
        except Exception as e:
            print(f"保存processed_urls失败: {e}")

    def load_processed_files(self):
        """加载已处理文件的元数据"""
        metadata_file = os.path.join(self.base_dir, 'file_metadata.json')
        if os.path.exists(metadata_file):
            try:
                with open(metadata_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except:
                pass
        return {}

    def save_processed_files(self):
        """保存已处理文件的元数据"""
        os.makedirs(self.base_dir, exist_ok=True)
        metadata_file = os.path.join(self.base_dir, 'file_metadata.json')
        try:
            with open(metadata_file, 'w', encoding='utf-8') as f:
                json.dump(self.processed_files, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"保存file_metadata失败: {e}")

    def get_page_content_selenium(self, url):
        """使用Selenium获取页面内容"""
        try:
            print(f"🌐 访问: {url}")
            
            # 先访问首页设置中文语言
            if 'borunte.com' in url:
                # 尝试访问中文版本
                base_url = "https://www.borunte.com"
                if url != base_url:
                    print("🌏 设置中文语言环境...")
                    self.driver.get(base_url)
                    time.sleep(2)
                    
                    # 尝试查找并点击中文语言选择
                    try:
                        # 查找可能的语言切换按钮
                        lang_elements = self.driver.find_elements(By.XPATH, "//*[contains(text(), '中文') or contains(text(), 'CN') or contains(text(), '简体')]")
                        if lang_elements:
                            lang_elements[0].click()
                            time.sleep(2)
                            print("✅ 已切换到中文语言")
                    except:
                        print("⚠️ 未找到语言切换按钮，继续使用默认语言")
            
            self.driver.get(url)
            
            # 等待页面加载
            time.sleep(3)
            
            # 滚动页面确保所有内容加载
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
            
            # 获取页面源码
            html_content = self.driver.page_source
            print(f"📄 获取到页面内容: {len(html_content)} 字符")
            
            return html_content
            
        except Exception as e:
            print(f"❌ Selenium获取页面失败: {e}")
            return None

    def find_module_section(self, soup, module_name):
        """在页面中找到特定模块的区域"""
        try:
            # 查找包含模块名称的标题元素
            section_element = soup.find(string=re.compile(re.escape(module_name)))
            if section_element:
                # 找到包含此文本的父容器
                parent = section_element.parent
                while parent and parent.name != 'div':
                    parent = parent.parent
                
                if parent:
                    print(f"✅ 找到 {module_name} 模块区域")
                    return parent
            
            print(f"⚠️ 未找到 {module_name} 模块区域")
            return None
            
        except Exception as e:
            print(f"❌ 查找模块区域失败: {module_name} - {e}")
            return None

    def extract_file_list_from_module(self, module_section, module_name):
        """从模块区域提取文件列表（不获取详情）"""
        files = []
        
        try:
            # 查找文件列表的li元素
            li_elements = module_section.find_all('li', {'data-id': True, 'data-type': True})
            
            print(f"📋 在 {module_name} 中找到 {len(li_elements)} 个文件")
            
            for elem in li_elements:
                file_info = self.extract_basic_file_info(elem, module_name)
                if file_info:
                    files.append(file_info)
                    
            print(f"✅ 从 {module_name} 提取到 {len(files)} 个文件基本信息")
            
        except Exception as e:
            print(f"❌ 提取文件列表失败: {module_name} - {e}")
            
        return files

    def extract_basic_file_info(self, element, module_name):
        """提取文件基本信息（不包含详情页面信息）"""
        try:
            file_info = {
                'data_id': element.get('data-id'),
                'data_type': element.get('data-type'),
                'module': module_name,
                'category': self.download_modules[module_name]['category']
            }
            
            # 提取标题
            title_elem = element.select_one('p[title]')
            if title_elem:
                file_info['title'] = title_elem.get('title', '').strip()
            else:
                p_elem = element.select_one('div.left p')
                if p_elem:
                    file_info['title'] = p_elem.get_text().strip()
            
            # 提取文件大小
            text = element.get_text()
            size_match = re.search(r'文件大小[：:]?\s*(\d+(?:\.\d+)?)\s*(MB|KB|GB)', text, re.IGNORECASE)
            if size_match:
                file_info['size'] = f"{size_match.group(1)}{size_match.group(2).upper()}"
            
            # 提取下载次数
            hits_elem = element.select_one('.hits_t')
            if hits_elem:
                file_info['download_count'] = hits_elem.get_text().strip()
            
            return file_info if file_info.get('title') else None
            
        except Exception as e:
            if self.debug:
                print(f"提取基本文件信息失败: {e}")
            return None

    def get_file_detail_info(self, file_info):
        """点击详情按钮获取文件详细信息"""
        try:
            print(f"🔍 获取详情: {file_info['title'][:50]}...")
            
            # 在页面中查找对应的详情按钮
            detail_button = self.driver.find_element(
                By.CSS_SELECTOR, 
                f'li[data-id="{file_info["data_id"]}"] .downbtn'
            )
            
            # 滚动到按钮位置并点击
            self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", detail_button)
            time.sleep(1)
            
            # 使用ActionChains进行精确点击
            from selenium.webdriver.common.action_chains import ActionChains
            actions = ActionChains(self.driver)
            actions.move_to_element(detail_button).click().perform()
            
            # 等待详情页面加载
            time.sleep(3)
            
            # 获取详情页面内容
            detail_html = self.driver.page_source
            
            # 提取更新时间（基于实际HTML结构）
            update_time = self.extract_update_time_from_detail(detail_html)
            file_info['update_time'] = update_time
            
            # 检查时间过滤
            if self.is_file_after_november_2024(update_time):
                print(f"✅ 文件符合时间条件: {file_info['title'][:30]}... ({update_time})")
                
                # 提取立即下载链接（基于实际HTML结构）
                download_url = self.extract_download_url_from_detail(detail_html)
                if download_url:
                    file_info['download_url'] = download_url
                    print(f"🔗 获取到下载链接: {download_url}")
                
                # 关闭弹窗
                self.close_popup()
                
                return True
            else:
                print(f"⏰ 文件不符合时间条件: {file_info['title'][:30]}... ({update_time})")
                self.close_popup()
                return False
                
        except Exception as e:
            print(f"❌ 获取详情失败: {file_info['title'][:30]}... - {e}")
            self.close_popup()  # 确保弹窗被关闭
            return False

    def extract_update_time_from_detail(self, html_content):
        """从详情页面中提取更新时间（基于实际HTML结构）"""
        try:
            # 基于实际HTML结构：更新时间：2023-04-27 14:35:44
            time_patterns = [
                r'更新时间[：:]?\s*(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})',
                r'更新时间[：:]?\s*(\d{4}-\d{2}-\d{2})',
                r'(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})',
                r'(\d{4}-\d{2}-\d{2})'
            ]
            
            for pattern in time_patterns:
                match = re.search(pattern, html_content)
                if match:
                    found_time = match.group(1)
                    print(f"🎯 找到更新时间: {found_time}")
                    return found_time
            
            print("⚠️ 未找到更新时间")
            return None
            
        except Exception as e:
            if self.debug:
                print(f"提取更新时间失败: {e}")
            return None

    def extract_download_url_from_detail(self, html_content):
        """从详情页面中提取下载链接（基于实际HTML结构）"""
        try:
            # 基于实际HTML结构：<a href="./file/packages/download/1682577340768.zip" download="...">立即下载</a>
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # 查找立即下载按钮
            download_link = soup.find('a', string='立即下载')
            if download_link:
                href = download_link.get('href')
                if href:
                    # 转换为绝对URL
                    if href.startswith('./'):
                        download_url = urljoin(self.base_url, href[2:])
                    elif href.startswith('/'):
                        download_url = urljoin(self.base_url, href)
                    else:
                        download_url = href
                    
                    print(f"🔗 找到下载链接: {download_url}")
                    return download_url
            
            # 备用方案：查找包含download属性的链接
            download_links = soup.find_all('a', {'download': True})
            for link in download_links:
                href = link.get('href')
                if href and ('file' in href or 'download' in href):
                    if href.startswith('./'):
                        download_url = urljoin(self.base_url, href[2:])
                    elif href.startswith('/'):
                        download_url = urljoin(self.base_url, href)
                    else:
                        download_url = href
                    
                    print(f"🔗 备用方案找到下载链接: {download_url}")
                    return download_url
            
            print("⚠️ 未找到下载链接")
            return None
            
        except Exception as e:
            if self.debug:
                print(f"提取下载链接失败: {e}")
            return None

    def extract_update_time_from_popup(self, soup):
        """从弹窗中提取更新时间（保留兼容性）"""
        return self.extract_update_time_from_detail(str(soup))

    def extract_download_url_from_onclick(self, onclick_text):
        """从onclick事件中提取下载URL"""
        try:
            # 分析onclick事件，提取实际下载URL
            if 'http' in onclick_text:
                url_match = re.search(r'https?://[^\'")\s]+', onclick_text)
                if url_match:
                    return url_match.group()
            
            # 如果没有直接URL，可能需要构造
            id_match = re.search(r'[\'"]([\w\d]+)[\'"]', onclick_text)
            if id_match:
                file_id = id_match.group(1)
                return f"https://www.borunte.com/download/{file_id}"
            
            return None
            
        except Exception as e:
            if self.debug:
                print(f"提取下载URL失败: {e}")
            return None

    def close_popup(self):
        """关闭弹窗（基于实际HTML结构）"""
        try:
            print("🔄 关闭详情弹窗...")
            
            # 基于实际HTML结构，关闭按钮是：<span aria-hidden="true" class="glyphicon glyphicon-remove"></span>
            close_selectors = [
                '.glyphicon-remove',  # 实际的关闭按钮
                '.goback',  # 关闭按钮的父容器
                '.close', '.btn-close', '[aria-label="Close"]',
                '.modal-close', '.popup-close', '.dialog-close'
            ]
            
            for selector in close_selectors:
                try:
                    close_elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    for close_btn in close_elements:
                        if close_btn.is_displayed():
                            close_btn.click()
                            time.sleep(1)
                            print(f"✅ 使用选择器关闭: {selector}")
                            return True
                except:
                    continue
            
            # 如果没有找到关闭按钮，按ESC键
            from selenium.webdriver.common.keys import Keys
            self.driver.find_element(By.TAG_NAME, 'body').send_keys(Keys.ESCAPE)
            time.sleep(1)
            print("✅ 使用ESC键关闭弹窗")
            return True
            
        except Exception as e:
            if self.debug:
                print(f"⚠️ 关闭弹窗失败: {e}")
            return False

    def is_file_after_november_2024(self, update_time_str):
        """判断文件是否为2024年11月1日之后更新"""
        if not update_time_str:
            return False  # 没有时间信息的文件不包含
            
        try:
            # 解析时间
            time_formats = [
                '%Y-%m-%d %H:%M:%S',
                '%Y-%m-%d',
                '%Y/%m/%d %H:%M:%S',
                '%Y/%m/%d'
            ]
            
            parsed_time = None
            for fmt in time_formats:
                try:
                    parsed_time = datetime.strptime(update_time_str.strip(), fmt)
                    break
                except ValueError:
                    continue
            
            if parsed_time:
                return parsed_time >= self.filter_date
            
        except Exception as e:
            if self.debug:
                print(f"时间解析失败: {update_time_str} - {e}")
        
        return False



    def navigate_to_page(self, page_num, module_name):
        """导航到指定页面 - 修复版本"""
        try:
            if page_num == 1:
                return True  # 已经在第一页
            
            print(f"🔄 {module_name} - 导航到第{page_num}页...")
            
            # 等待页面稳定
            time.sleep(2)
            
            # 修复后的导航逻辑：更精确的模块分页定位
            script = f"""
            var moduleName = '{module_name}';
            var targetPage = '{page_num}';
            
            console.log('开始查找页码按钮:', targetPage);
            
            // 方法1: 首先找到当前活跃的文件列表区域
            var activeFileListArea = null;
            var allFileLists = document.querySelectorAll('[class*="list"], [id*="list"], .content');
            
            // 找到包含最多可见文件项目的区域
            var maxVisibleFiles = 0;
            for (var i = 0; i < allFileLists.length; i++) {{
                var area = allFileLists[i];
                var visibleFiles = area.querySelectorAll('li[data-id]').length;
                if (visibleFiles > maxVisibleFiles && area.offsetWidth > 0 && area.offsetHeight > 0) {{
                    maxVisibleFiles = visibleFiles;
                    activeFileListArea = area;
                }}
            }}
            
            console.log('找到活跃文件列表区域，包含文件数:', maxVisibleFiles);
            
            var foundButton = null;
            
            // 方法2: 在整个页面查找所有页码按钮，但要验证其关联性
            var allPageButtons = document.querySelectorAll('li, button, a, span');
            var candidateButtons = [];
            
            for (var i = 0; i < allPageButtons.length; i++) {{
                var btn = allPageButtons[i];
                var text = btn.textContent.trim();
                
                // 严格匹配页码
                if (text === targetPage && btn.offsetWidth > 0 && btn.offsetHeight > 0) {{
                    var rect = btn.getBoundingClientRect();
                    
                    // 排除明显不相关的按钮（比如页面顶部的导航）
                    if (rect.top > 150 && rect.top < window.innerHeight - 50) {{
                        // 检查按钮是否可点击
                        var isClickable = !btn.disabled && 
                                        !btn.classList.contains('disabled') &&
                                        !btn.classList.contains('current') &&
                                        !btn.classList.contains('active');
                        
                        // 检查按钮的父容器是否像分页容器
                        var parent = btn.parentElement;
                        var parentClass = parent ? parent.className.toLowerCase() : '';
                        var isPaginationParent = parentClass.includes('page') || 
                                               parentClass.includes('pagination') ||
                                               parentClass.includes('number') ||
                                               parent.querySelectorAll('li, a, button').length >= 3;
                        
                        candidateButtons.push({{
                            element: btn,
                            rect: rect,
                            isClickable: isClickable,
                            isPaginationParent: isPaginationParent,
                            parentClass: parentClass,
                            score: 0
                        }});
                    }}
                }}
            }}
            
            console.log('找到候选按钮数量:', candidateButtons.length);
            
            // 方法3: 对候选按钮进行评分，选择最合适的
            for (var i = 0; i < candidateButtons.length; i++) {{
                var candidate = candidateButtons[i];
                var score = 0;
                
                // 可点击性加分
                if (candidate.isClickable) score += 10;
                
                // 分页容器特征加分
                if (candidate.isPaginationParent) score += 15;
                
                // 位置合理性加分（页面中下部分）
                if (candidate.rect.top > window.innerHeight * 0.4) score += 5;
                
                // 如果有活跃文件区域，计算与文件区域的距离
                if (activeFileListArea) {{
                    var fileAreaRect = activeFileListArea.getBoundingClientRect();
                    var verticalDistance = Math.abs(candidate.rect.top - fileAreaRect.bottom);
                    
                    // 距离文件区域越近越好（分页通常在文件列表下方）
                    if (verticalDistance < 300) {{
                        score += Math.max(0, 10 - verticalDistance / 30);
                    }}
                }}
                
                candidate.score = score;
                console.log('按钮评分:', {{
                    text: candidate.element.textContent.trim(),
                    score: score,
                    parentClass: candidate.parentClass,
                    isClickable: candidate.isClickable
                }});
            }}
            
            // 选择得分最高的按钮
            if (candidateButtons.length > 0) {{
                candidateButtons.sort(function(a, b) {{ return b.score - a.score; }});
                var bestCandidate = candidateButtons[0];
                
                if (bestCandidate.score > 5) {{ // 至少要有基本分数
                    foundButton = bestCandidate.element;
                    console.log('选择最佳按钮:', {{
                        text: foundButton.textContent.trim(),
                        score: bestCandidate.score,
                        parentClass: bestCandidate.parentClass
                    }});
                }}
            }}
            
            // 方法4: 如果还是没找到，尝试通过JavaScript事件查找
            if (!foundButton) {{
                console.log('尝试通过事件查找分页按钮...');
                var allClickableElements = document.querySelectorAll('[onclick], [data-page], [href*="page"]');
                
                for (var i = 0; i < allClickableElements.length; i++) {{
                    var elem = allClickableElements[i];
                    if (elem.textContent.trim() === targetPage && 
                        elem.offsetWidth > 0 && elem.offsetHeight > 0) {{
                        foundButton = elem;
                        console.log('通过事件找到按钮');
                        break;
                    }}
                }}
            }}
            
            // 执行点击
            if (foundButton) {{
                try {{
                    // 先滚动到按钮位置
                    foundButton.scrollIntoView({{block: 'center', behavior: 'smooth'}});
                    
                    // 等待滚动完成后点击
                    setTimeout(function() {{
                        try {{
                            foundButton.click();
                            console.log('页码按钮点击成功');
                        }} catch (e) {{
                            console.log('直接点击失败，尝试事件触发:', e);
                            var event = new MouseEvent('click', {{
                                view: window,
                                bubbles: true,
                                cancelable: true
                            }});
                            foundButton.dispatchEvent(event);
                        }}
                    }}, 1000);
                    
                    // 返回按钮信息
                    return JSON.stringify({{
                        success: true,
                        text: foundButton.textContent.trim(),
                        tagName: foundButton.tagName,
                        className: foundButton.className,
                        id: foundButton.id,
                        score: candidateButtons.find(c => c.element === foundButton)?.score || 0
                    }});
                    
                }} catch (e) {{
                    console.log('点击执行失败:', e);
                    return JSON.stringify({{success: false, error: e.toString()}});
                }}
            }}
            
            return JSON.stringify({{success: false, reason: 'no_button_found', candidates: candidateButtons.length}});
            """
            
            result = self.driver.execute_script(script)
            
            if result:
                try:
                    result_obj = json.loads(result)
                    if result_obj.get('success'):
                        print(f"✅ 成功找到并点击{module_name}模块的第{page_num}页按钮")
                        print(f"   按钮信息: {result_obj.get('tagName')} '{result_obj.get('text')}' (得分: {result_obj.get('score')})")
                        
                        # 等待页面加载和内容更新
                        time.sleep(5)  # 增加等待时间
                        
                        # 验证页面是否真的变化了
                        verification_script = f"""
                        // 检查URL是否包含页码参数
                        var url = window.location.href;
                        if (url.includes('page={page_num}') || url.includes('p={page_num}')) {{
                            return 'url_changed';
                        }}
                        
                        // 检查是否有页码指示器显示当前页
                        var currentPageIndicators = document.querySelectorAll('.current, .active, .selected');
                        for (var i = 0; i < currentPageIndicators.length; i++) {{
                            var indicator = currentPageIndicators[i];
                            if (indicator.textContent.trim() === '{page_num}') {{
                                return 'page_indicator_changed';
                            }}
                        }}
                        
                        return 'page_loaded';
                        """
                        
                        verification = self.driver.execute_script(verification_script)
                        print(f"   页面验证: {verification}")
                        
                        return True
                    else:
                        print(f"❌ 点击失败: {result_obj.get('reason', 'unknown')}")
                        if result_obj.get('candidates', 0) > 0:
                            print(f"   找到了{result_obj['candidates']}个候选按钮，但评分都不够高")
                        return False
                except json.JSONDecodeError:
                    print(f"✅ 导航操作执行完成（返回格式解析失败）")
                    time.sleep(5)
                    return True
            else:
                print(f"⚠️ 未找到{module_name}模块第{page_num}页的分页按钮")
                return False
            
        except Exception as e:
            print(f"❌ 导航到第{page_num}页失败: {e}")
            return False

    def check_content_duplication(self, current_files, previous_files, page_num):
        """检查内容重复的改进版本"""
        if page_num == 1 or not previous_files:
            return False, 0.0  # 第一页或没有上一页数据时不检查
        
        if not current_files:
            print(f"⚠️ 第{page_num}页没有检测到任何文件，可能是翻页失败")
            return True, 1.0  # 没有文件时认为是重复（翻页失败）
        
        try:
            # 使用更精确的文件标识
            current_identifiers = []
            previous_identifiers = []
            
            for f in current_files:
                # 组合多个字段作为唯一标识
                identifier = f"{f.get('data_id', '')}__{f.get('title', '')}__{f.get('size', '')}"
                current_identifiers.append(identifier)
            
            for f in previous_files:
                identifier = f"{f.get('data_id', '')}__{f.get('title', '')}__{f.get('size', '')}"
                previous_identifiers.append(identifier)
            
            # 计算重复率
            if len(current_identifiers) > 0 and len(previous_identifiers) > 0:
                common_files = set(current_identifiers) & set(previous_identifiers)
                duplicate_ratio = len(common_files) / len(current_identifiers)
                
                print(f"🔍 页面重复检测详情:")
                print(f"   当前页文件数: {len(current_identifiers)}")
                print(f"   上一页文件数: {len(previous_identifiers)}")
                print(f"   重复文件数: {len(common_files)}")
                print(f"   重复率: {duplicate_ratio:.2%}")
                
                # 调整重复率阈值：只有90%以上才认为是真正重复
                is_duplicate = duplicate_ratio >= 0.90
                
                if is_duplicate:
                    print(f"📋 重复的文件标题:")
                    for identifier in common_files:
                        title = identifier.split('__')[1] if '__' in identifier else identifier
                        print(f"     - {title[:50]}...")
                
                return is_duplicate, duplicate_ratio
            
            return False, 0.0
            
        except Exception as e:
            print(f"❌ 重复检测失败: {e}")
            return False, 0.0

    def switch_to_module(self, module_name):
        """切换到指定模块区域 - 改进版本"""
        try:
            print(f"🔄 切换到模块: {module_name}")
            
            # 等待页面稳定
            time.sleep(2)
            
            # 改进的模块切换逻辑
            script = f"""
            var moduleName = '{module_name}';
            console.log('查找模块:', moduleName);
            
            // 先尝试查找精确匹配的标签页或按钮
            var elements = document.querySelectorAll('a, button, span, div, li');
            var foundElement = null;
            var maxScore = 0;
            
            for (var i = 0; i < elements.length; i++) {{
                var elem = elements[i];
                var text = elem.textContent.trim();
                
                // 检查是否是可见元素
                if (elem.offsetWidth > 0 && elem.offsetHeight > 0) {{
                    var score = 0;
                    
                    // 精确匹配最高分
                    if (text === moduleName) {{
                        score = 20;
                    }}
                    // 包含匹配
                    else if (text.includes(moduleName)) {{
                        score = 15;
                    }}
                    // 部分匹配
                    else if (moduleName.includes(text) && text.length > 2) {{
                        score = 10;
                    }}
                    
                    // 如果是可点击元素，加分
                    if (elem.tagName === 'A' || elem.tagName === 'BUTTON' || 
                        elem.onclick || elem.classList.contains('tab') ||
                        elem.classList.contains('btn') || elem.style.cursor === 'pointer') {{
                        score += 5;
                    }}
                    
                    // 如果在导航区域，加分
                    var parent = elem.parentElement;
                    while (parent && parent !== document.body) {{
                        var parentClass = parent.className.toLowerCase();
                        if (parentClass.includes('nav') || parentClass.includes('tab') || 
                            parentClass.includes('menu') || parentClass.includes('header')) {{
                            score += 3;
                            break;
                        }}
                        parent = parent.parentElement;
                    }}
                    
                    if (score > maxScore) {{
                        maxScore = score;
                        foundElement = elem;
                    }}
                }}
            }}
            
            if (foundElement && maxScore >= 10) {{
                console.log('找到匹配元素:', foundElement.textContent.trim(), '得分:', maxScore);
                
                // 滚动到元素位置
                foundElement.scrollIntoView({{block: 'center', behavior: 'smooth'}});
                
                // 等待滚动完成后点击
                setTimeout(function() {{
                    try {{
                        foundElement.click();
                        console.log('模块切换点击成功');
                    }} catch (e) {{
                        console.log('直接点击失败，尝试事件触发:', e);
                        var event = new MouseEvent('click', {{
                            view: window,
                            bubbles: true,
                            cancelable: true
                        }});
                        foundElement.dispatchEvent(event);
                    }}
                }}, 500);
                
                return true;
            }}
            
            console.log('未找到合适的模块切换元素');
            return false;
            """
            
            success = self.driver.execute_script(script)
            
            if success:
                time.sleep(5)  # 等待页面切换和内容加载
                print(f"✅ 成功切换到 {module_name} 模块")
                return True
            else:
                print(f"⚠️ 未找到 {module_name} 模块切换按钮，使用当前页面")
                return True
                
        except Exception as e:
            print(f"❌ 切换模块失败: {module_name} - {e}")
            return True  # 继续使用当前页面

    def debug_page_state(self, page_num, module_name):
        """调试页面状态，输出当前页面信息"""
        try:
            script = f"""
            console.log('=== 页面调试信息 ===');
            console.log('当前页码: {page_num}');
            console.log('模块名称: {module_name}');
            console.log('当前URL:', window.location.href);
            
            // 查找当前页面的文件数量
            var fileItems = document.querySelectorAll('li[data-id]');
            console.log('当前页面文件数量:', fileItems.length);
            
            // 查找分页相关元素
            var paginationElements = document.querySelectorAll('[class*="page"], [class*="pagination"]');
            console.log('分页容器数量:', paginationElements.length);
            
            // 输出当前活跃的页码指示器
            var activePageIndicators = document.querySelectorAll('.current, .active, .selected');
            console.log('活跃页码指示器:');
            for (var i = 0; i < activePageIndicators.length; i++) {{
                var indicator = activePageIndicators[i];
                console.log(' - ', indicator.textContent.trim(), indicator.className);
            }}
            
            // 查找所有页码按钮
            var pageButtons = document.querySelectorAll('li, button, a, span');
            var pageNumbers = [];
            for (var i = 0; i < pageButtons.length; i++) {{
                var btn = pageButtons[i];
                var text = btn.textContent.trim();
                var num = parseInt(text);
                if (!isNaN(num) && num >= 1 && num <= 20 && btn.offsetWidth > 0) {{
                    pageNumbers.push(num);
                }}
            }}
            console.log('可见页码:', pageNumbers.sort((a,b) => a-b));
            
            // 输出前几个文件的标题（用于重复检测）
            var sampleTitles = [];
            for (var i = 0; i < Math.min(fileItems.length, 3); i++) {{
                var item = fileItems[i];
                var titleElem = item.querySelector('p[title], div.left p');
                if (titleElem) {{
                    sampleTitles.push(titleElem.textContent.trim().substring(0, 30));
                }}
            }}
            console.log('前3个文件标题样本:', sampleTitles);
            
            return {{
                fileCount: fileItems.length,
                pageNumbers: pageNumbers,
                sampleTitles: sampleTitles,
                url: window.location.href
            }};
            """
            
            debug_info = self.driver.execute_script(script)
            print(f"🔧 调试信息 - 第{page_num}页:")
            print(f"   文件数量: {debug_info.get('fileCount', 0)}")
            print(f"   可见页码: {debug_info.get('pageNumbers', [])}")
            print(f"   样本标题: {debug_info.get('sampleTitles', [])}")
            print(f"   当前URL: {debug_info.get('url', 'N/A')}")
            
            return debug_info
            
        except Exception as e:
            print(f"⚠️ 调试信息获取失败: {e}")
            return {}

    def get_total_pages_for_module(self, module_name):
        """获取模块的总页数"""
        try:
            # 使用JavaScript查找分页信息
            script = """
            // 查找分页信息
            var paginationElements = document.querySelectorAll('[class*="page"], [class*="pagination"], [id*="page"]');
            var maxPage = 1;
            var foundPages = [];
            
            for (var i = 0; i < paginationElements.length; i++) {
                var elem = paginationElements[i];
                if (elem.offsetWidth > 0 && elem.offsetHeight > 0) {
                    // 查找数字页码
                    var pageNumbers = elem.querySelectorAll('a, li, span, button');
                    for (var j = 0; j < pageNumbers.length; j++) {
                        var text = pageNumbers[j].textContent.trim();
                        var num = parseInt(text);
                        if (!isNaN(num) && num > 0 && num <= 50) {  // 限制最大页数
                            foundPages.push(num);
                            if (num > maxPage) {
                                maxPage = num;
                            }
                        }
                    }
                }
            }
            
            console.log('检测到的页码:', foundPages.sort((a,b) => a-b));
            return maxPage;
            """
            
            total_pages = self.driver.execute_script(script)
            print(f"🔍 检测到 {module_name} 模块可能有 {total_pages} 页")
            
            # 如果检测不到，默认尝试5页
            return max(total_pages, 5)
            
        except Exception as e:
            print(f"⚠️ 获取总页数失败: {e}")
            return 5  # 默认尝试5页

    def crawl_module_with_pagination(self, module_name, module_config):
        """爬取单个模块的所有页面 - 修复版本"""
        print(f"\n🔍 开始爬取模块: {module_name}")
        print("-" * 50)
        
        try:
            all_files = []
            previous_page_files = []
            
            # 访问页面
            html_content = self.get_page_content_selenium(module_config['url'])
            if not html_content:
                print(f"❌ 无法获取 {module_name} 页面内容")
                return []
            
            # 切换到指定模块
            self.switch_to_module(module_name)
            
            # 获取该模块的总页数
            total_pages = self.get_total_pages_for_module(module_name)
            print(f"📊 预计总页数: {total_pages}")
            
            # 动态遍历页面：检测实际页数，防止无限循环
            page_num = 1
            max_attempts = min(total_pages + 5, 20)  # 限制最大尝试次数
            consecutive_failures = 0
            
            while page_num <= max_attempts:
                print(f"\n📖 {module_name} - 处理第 {page_num} 页")
                
                # 导航到指定页面
                if page_num > 1:
                    # 导航前先输出调试信息
                    print(f"🔧 导航前状态检查:")
                    pre_debug_info = self.debug_page_state(page_num - 1, module_name)
                    
                    navigation_success = self.navigate_to_page(page_num, module_name)
                    if not navigation_success:
                        consecutive_failures += 1
                        print(f"⚠️ 无法导航到第{page_num}页，连续失败次数: {consecutive_failures}")
                        
                        if consecutive_failures >= 2:
                            print(f"📊 连续{consecutive_failures}次导航失败，{module_name} 实际共有 {page_num-1} 页")
                            break
                        
                        page_num += 1
                        continue
                    else:
                        consecutive_failures = 0  # 重置失败计数
                        
                        # 给页面更多时间来加载新内容
                        print(f"⏳ 等待第{page_num}页内容加载...")
                        time.sleep(3)
                        
                        # 导航后输出调试信息
                        print(f"🔧 导航后状态检查:")
                        post_debug_info = self.debug_page_state(page_num, module_name)
                        
                        # 比较导航前后的变化
                        if pre_debug_info.get('sampleTitles') == post_debug_info.get('sampleTitles'):
                            print(f"⚠️ 警告：导航后页面内容未发生变化！")
                        else:
                            print(f"✅ 导航成功：页面内容已更新")
                
                # 获取页面内容并提取文件
                html_content = self.driver.page_source
                soup = BeautifulSoup(html_content, 'html.parser')
                
                # 查找模块区域
                module_section = self.find_module_section(soup, module_name) or soup
                
                # 提取文件列表
                current_page_files = self.extract_file_list_from_module(module_section, module_name)
                print(f"📋 第{page_num}页检测到 {len(current_page_files)} 个文件")
                
                # 如果第一页没有文件，可能模块切换失败
                if page_num == 1 and len(current_page_files) == 0:
                    print(f"⚠️ 第一页没有文件，可能模块切换失败，重新尝试切换到 {module_name}")
                    self.switch_to_module(module_name)
                    time.sleep(3)
                    
                    # 重新获取页面内容
                    html_content = self.driver.page_source
                    soup = BeautifulSoup(html_content, 'html.parser')
                    module_section = self.find_module_section(soup, module_name) or soup
                    current_page_files = self.extract_file_list_from_module(module_section, module_name)
                    print(f"📋 重新获取后第{page_num}页检测到 {len(current_page_files)} 个文件")
                
                # 使用改进的重复检测
                if page_num > 1:
                    is_duplicate, duplicate_ratio = self.check_content_duplication(
                        current_page_files, previous_page_files, page_num
                    )
                    
                    if is_duplicate:
                        print(f"⚠️ 检测到第{page_num}页与第{page_num-1}页内容重复率{duplicate_ratio:.1%}，停止翻页")
                        print(f"📊 {module_name} 实际共有 {page_num-1} 页有效内容")
                        break
                
                # 记录当前页面的文件列表供下次比较
                previous_page_files = current_page_files.copy()
                
                # 处理每个文件
                valid_files = []
                for i, file_info in enumerate(current_page_files, 1):
                    print(f"  🔍 [{i}/{len(current_page_files)}] {file_info['title'][:40]}...")
                    
                    if self.get_file_detail_info(file_info):
                        valid_files.append(file_info)
                        
                        # 检查文件状态
                        file_key = f"{file_info['module']}_{file_info['title']}"
                        if file_key not in self.processed_files:
                            self.new_files.append(file_info)
                            print(f"    ✅ 新文件")
                        elif self.check_file_update(file_info):
                            self.updated_files.append(file_info)
                            print(f"    🔄 更新文件")
                        else:
                            print(f"    ⏭️ 无变化")
                        
                        self.processed_files[file_key] = file_info
                    else:
                        print(f"    ⏰ 不符合时间条件")
                    
                    time.sleep(0.5)  # 减少延时
                
                all_files.extend(valid_files)
                print(f"📋 第{page_num}页找到 {len(valid_files)} 个符合条件的文件")
                
                page_num += 1  # 处理下一页
                time.sleep(2)  # 页面间隔
            
            print(f"✅ {module_name} 爬取完成，总共 {len(all_files)} 个符合条件的文件")
            return all_files
            
        except Exception as e:
            print(f"❌ 爬取 {module_name} 失败: {e}")
            import traceback
            traceback.print_exc()
            return []

    def check_file_update(self, file_info):
        """检查文件是否有更新"""
        file_key = f"{file_info['module']}_{file_info['title']}"
        
        if file_key in self.processed_files:
            old_info = self.processed_files[file_key]
            
            # 比较更新时间
            if old_info.get('update_time') != file_info.get('update_time'):
                print(f"🔄 检测到文件更新: {file_info['title'][:30]}...")
                return True
        
        return False

    def smart_categorize_file(self, file_info):
        """智能文件分类 - 根据文件标题内容进行二次分类"""
        title = file_info.get('title', '').lower()
        original_category = file_info.get('category', '其他资料')
        
        # 电控柜关键词（应归类为技术文档，不是工装资料）
        electrical_cabinet_keywords = [
            '电控柜', 'electrical control cabinet', '控制柜', 'control cabinet'
        ]
        
        # 工装资料关键词（移除电控柜相关）
        tooling_keywords = [
            'tools and fixtures', '工装', '夹具', '治具', '升降平台', 
            '拖动设备', '视觉系统', '相机', '传感器', '执行器',
            '雾化器', '切刀', '剪刀', '吸盘', '夹爪', '法兰', '分张器',
            '补偿器', '示教器', '编码器', '跟踪器', '校准设备'
        ]
        
        # 产品图片关键词
        image_keywords = [
            'pictures', '图片', '宣传图', '效果图', '展台', '展厅', 
            '广告', '名片', '模板', '产品家族'
        ]
        
        # 3D模型关键词
        model_keywords = [
            '3d模型', '3d model', 'cad', '模型'
        ]
        
        # 技术文档关键词（排除工装类）
        doc_keywords = [
            '说明书', '手册', '操作', '故障', '排查', '保养', '认证',
            '证书', '规格书', '技术规格', '离线编程', 'instructions',
            'manual', 'troubleshooting', 'specification'
        ]
        
        # 优先检查电控柜（归类为技术文档）
        for keyword in electrical_cabinet_keywords:
            if keyword in title:
                print(f"📄 重新分类为技术文档: {file_info['title'][:30]}...")
                return '技术文档'
        
        # 检查是否为工装资料
        for keyword in tooling_keywords:
            if keyword in title:
                print(f"🔧 重新分类为工装资料: {file_info['title'][:30]}...")
                return '工装资料'
        
        # 检查是否为产品图片
        for keyword in image_keywords:
            if keyword in title:
                print(f"🖼️ 重新分类为产品图片: {file_info['title'][:30]}...")
                return '产品图片'
        
        # 检查是否为3D模型
        for keyword in model_keywords:
            if keyword in title:
                if '机器人' in title or 'robot' in title:
                    print(f"🤖 重新分类为机器人3D模型: {file_info['title'][:30]}...")
                    return '机器人3D模型'
                elif '机械手' in title or 'manipulator' in title:
                    print(f"🦾 重新分类为机械手3D模型: {file_info['title'][:30]}...")
                    return '机械手3D模型'
        
        # 保持原分类或使用技术文档作为默认
        return original_category

    def download_file(self, file_info):
        """下载文件"""
        try:
            if not file_info.get('download_url'):
                print(f"⚠️ 跳过无下载链接的文件: {file_info.get('title', 'Unknown')}")
                return False
            
            # 智能分类
            category = self.smart_categorize_file(file_info)
            title = file_info['title']
            filename = self.clean_filename(title)
            
            # 根据文件大小信息确定扩展名
            if file_info.get('size'):
                size_str = file_info['size']
                if 'PDF' in size_str.upper():
                    file_ext = '.pdf'
                else:
                    file_ext = '.rar'  # 默认
            else:
                file_ext = '.rar'
            
            if not filename.endswith(file_ext):
                filename += file_ext
            
            save_path = os.path.join(self.base_dir, category, filename)
            
            # 增量逻辑：检查文件是否已存在
            if os.path.exists(save_path):
                print(f"文件已存在，跳过下载: {title}")
                return False
            
            if file_info['download_url'] in self.processed_urls:
                print(f"URL已处理过，跳过: {title}")
                return False
            
            print(f"📥 开始下载: {title}")
            print(f"🔗 URL: {file_info['download_url']}")
            
            # 确保目录存在
            os.makedirs(os.path.dirname(save_path), exist_ok=True)
            
            # 下载文件
            response = self.session.get(file_info['download_url'], stream=True, timeout=60)
            response.raise_for_status()
            
            with open(save_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            file_size = os.path.getsize(save_path)
            print(f"✅ 下载完成: {title} ({file_size} bytes)")
            
            # 记录已处理的URL
            self.processed_urls.add(file_info['download_url'])
            
            # 添加到新文件列表
            file_info['path'] = save_path
            file_info['size_bytes'] = file_size
            
            return True
            
        except Exception as e:
            print(f"❌ 下载失败: {file_info.get('title', 'Unknown')} - {e}")
            return False

    def clean_filename(self, filename):
        """清理文件名"""
        # 移除或替换非法字符
        illegal_chars = ['/', '\\', ':', '*', '?', '"', '<', '>', '|']
        for char in illegal_chars:
            filename = filename.replace(char, '_')
        return filename.strip()[:100]  # 限制长度

    def send_dingtalk_notification(self, message):
        """发送钉钉通知"""
        try:
            timestamp = str(round(time.time() * 1000))
            string_to_sign = f'{timestamp}\n{self.SECRET}'
            hmac_code = hmac.new(self.SECRET.encode(), string_to_sign.encode(), hashlib.sha256).digest()
            sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))

            url = f'https://oapi.dingtalk.com/robot/send?access_token={self.ACCESS_TOKEN}&timestamp={timestamp}&sign={sign}'
            headers = {'Content-Type': 'application/json'}
            data = {
                "msgtype": "text",
                "text": {"content": message},
                "at": {"isAtAll": False}
            }

            response = requests.post(url, json=data, headers=headers)
            print(f"📨 钉钉通知响应：{response.status_code} {response.text}")
            return response.status_code == 200
        except Exception as e:
            print(f"钉钉通知发送失败: {e}")
            return False

    def reorganize_existing_files(self):
        """重新整理已存在的文件分类"""
        print("\n🔧 开始重新整理现有文件分类...")
        
        try:
            moved_files = []
            
            # 遍历所有子目录
            for root, dirs, files in os.walk(self.base_dir):
                for filename in files:
                    if filename in ['processed_urls.pkl', 'file_metadata.json']:
                        continue
                    
                    file_path = os.path.join(root, filename)
                    relative_path = os.path.relpath(file_path, self.base_dir)
                    current_category = os.path.dirname(relative_path) if os.path.dirname(relative_path) else '其他资料'
                    
                    # 创建模拟的file_info用于分类判断
                    file_info = {
                        'title': os.path.splitext(filename)[0],  # 去除扩展名
                        'category': current_category
                    }
                    
                    # 获取智能分类结果
                    new_category = self.smart_categorize_file(file_info)
                    
                    # 如果分类发生变化，移动文件
                    if new_category != current_category:
                        new_dir = os.path.join(self.base_dir, new_category)
                        new_path = os.path.join(new_dir, filename)
                        
                        # 确保目标目录存在
                        os.makedirs(new_dir, exist_ok=True)
                        
                        # 移动文件
                        if not os.path.exists(new_path):
                            shutil.move(file_path, new_path)
                            moved_files.append({
                                'filename': filename,
                                'from': current_category,
                                'to': new_category
                            })
                            print(f"📁 移动文件: {filename}")
                            print(f"   从: {current_category} -> 到: {new_category}")
                        else:
                            print(f"⚠️ 目标文件已存在，跳过: {filename}")
            
            if moved_files:
                print(f"\n✅ 文件重新整理完成！移动了 {len(moved_files)} 个文件")
                
                # 按分类汇总移动的文件
                move_summary = {}
                for move in moved_files:
                    key = f"{move['from']} -> {move['to']}"
                    if key not in move_summary:
                        move_summary[key] = []
                    move_summary[key].append(move['filename'])
                
                print("\n📊 移动汇总:")
                for move_type, filenames in move_summary.items():
                    print(f"  {move_type}: {len(filenames)} 个文件")
                    for filename in filenames[:3]:  # 只显示前3个
                        print(f"    - {filename}")
                    if len(filenames) > 3:
                        print(f"    ... 还有 {len(filenames) - 3} 个文件")
                
                return moved_files
            else:
                print("✅ 所有文件分类正确，无需移动")
                return []
                
        except Exception as e:
            print(f"❌ 文件重新整理失败: {e}")
            return []

    def send_notifications(self):
        """发送通知"""
        try:
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # 如果有新文件或更新文件，发送通知
            if self.new_files or self.updated_files:
                message = f"""✅ 伯朗特完整爬虫 检测完成

📊 检测结果:
  新增文件: {len(self.new_files)} 个
  更新文件: {len(self.updated_files)} 个

📅 时间范围: 仅2024年11月1日后的文件
🔍 检测模块: 案例下载、文件下载、图片下载、视频下载、工装资料下载、机器人3D模型下载、机械手3D模型下载

"""
                # 添加新文件明细
                if self.new_files:
                    message += "🆕 新增文件:\n"
                    for file_info in self.new_files[:10]:  # 最多显示10个
                        update_time = file_info.get('update_time', '未知')
                        message += f"  📄 {file_info['title'][:40]} ({update_time})\n"
                    if len(self.new_files) > 10:
                        message += f"  ... 还有 {len(self.new_files) - 10} 个文件\n"
                
                # 添加更新文件明细
                if self.updated_files:
                    message += "\n🔄 更新文件:\n"
                    for file_info in self.updated_files[:10]:  # 最多显示10个
                        update_time = file_info.get('update_time', '未知')
                        message += f"  📄 {file_info['title'][:40]} ({update_time})\n"
                    if len(self.updated_files) > 10:
                        message += f"  ... 还有 {len(self.updated_files) - 10} 个文件\n"
                
                message += f"""
📁 文件存放路径: /srv/downloads/approved/伯朗特/
⏰ 检测时间: {current_time}"""
                
                self.send_dingtalk_notification(message)
            else:
                # 没有新文件或更新
                message = f"""✅ 伯朗特完整爬虫 检测完成

📊 检测结果: 无新增或更新文件
📅 时间范围: 仅2024年11月1日后的文件
⏰ 检测时间: {current_time}"""
                
                self.send_dingtalk_notification(message)
                
        except Exception as e:
            print(f"发送通知失败: {e}")

    def run(self):
        """主运行函数"""
        print("🚀 伯朗特完整爬虫启动...")
        print("🎯 目标: 7个下载模块的完整检测")
        print("📅 时间过滤: 仅2024年11月1日之后的文件")
        print("🔍 功能: 详情页面检测 + 分页支持 + 时间过滤 + 钉钉通知")
        print()
        
        # 创建目录
        os.makedirs(self.base_dir, exist_ok=True)
        
        try:
            # 首先重新整理现有文件分类
            if not self.is_first_run:
                moved_files = self.reorganize_existing_files()
                if moved_files:
                    print(f"🔧 已重新整理 {len(moved_files)} 个文件的分类")
            
            # 设置Selenium
            if not self.setup_selenium():
                print("❌ 无法启动Selenium，爬取终止")
                return
            
            all_files = []
            
            # 遍历所有模块
            for module_name, module_config in self.download_modules.items():
                try:
                    files = self.crawl_module_with_pagination(module_name, module_config)
                    all_files.extend(files)
                    time.sleep(5)  # 模块间隔
                except Exception as e:
                    print(f"❌ 模块 {module_name} 爬取异常: {e}")
                    continue
            
            print(f"\n📊 检测总结:")
            print(f"  符合条件文件: {len(all_files)}")
            print(f"  新增文件: {len(self.new_files)}")
            print(f"  更新文件: {len(self.updated_files)}")
            
            # 下载新文件和更新文件
            download_files = self.new_files + self.updated_files
            success_count = 0
            
            if download_files:
                print(f"\n📥 开始下载 {len(download_files)} 个文件...")
                for i, file_info in enumerate(download_files, 1):
                    print(f"\n[{i}/{len(download_files)}] 下载文件...")
                    if self.download_file(file_info):
                        success_count += 1
                    time.sleep(1)  # 下载间隔
            
            print(f"\n✅ 爬取完成! 成功下载 {success_count} 个文件")
            
            # 保存处理记录
            self.save_processed_urls()
            self.save_processed_files()
            
            # 发送通知
            self.send_notifications()
            
        except Exception as e:
            print(f"❌ 爬取过程出错: {e}")
            if self.debug:
                import traceback
                traceback.print_exc()
        
        finally:
            if self.driver:
                self.driver.quit()
                print("🔒 浏览器已关闭")

def reorganize_files_only():
    """仅运行文件重新整理功能"""
    print("🔧 伯朗特文件分类整理工具")
    print("重新整理已下载文件的分类")
    print("=" * 60)
    
    spider = BorunterCompleteSpider()
    moved_files = spider.reorganize_existing_files()
    
    if moved_files:
        print(f"\n🎉 整理完成！移动了 {len(moved_files)} 个文件")
        
        # 发送整理完成通知
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        message = f"""🔧 伯朗特文件分类整理完成
        
📊 整理结果: 重新分类了 {len(moved_files)} 个文件
⏰ 整理时间: {current_time}

详细分类调整请查看控制台输出。"""
        
        spider.send_dingtalk_notification(message)
    else:
        print("\n✅ 所有文件分类都正确，无需调整")

def main():
    print("🚀 伯朗特完整爬虫")
    print("基于用户操作指引的完整实现")
    print("=" * 60)
    
    import sys
    
    # 检查运行模式
    if len(sys.argv) > 1:
        if sys.argv[1] == "reorganize":
            print("🔧 运行文件重新整理模式")
            reorganize_files_only()
            return
        elif sys.argv[1] == "test":
            print("🧪 进入测试模式")
    
    if not SELENIUM_AVAILABLE:
        print("❌ 无法使用Selenium版本")
        print("📋 请安装: pip install selenium webdriver-manager")
        return
    
    spider = BorunterCompleteSpider()
    
    # 测试模式：只测试文件下载模块
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        # 临时修改模块列表，只包含文件下载
        original_modules = spider.download_modules.copy()
        spider.download_modules = {
            "文件下载": original_modules["文件下载"]
        }
        print("🎯 只测试文件下载模块")
    
    spider.run()

if __name__ == "__main__":
    main()
