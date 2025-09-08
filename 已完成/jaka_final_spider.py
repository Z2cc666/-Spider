#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
JAKA完整爬虫最终版本 - 基于网站结构分析的完整实现
支持：分类识别、文件提取、时间过滤、下载、钉钉通知
"""

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
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False
    print("⚠️ Selenium未安装，无法使用浏览器自动化")

class JakaFinalSpider:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3',
        }
        
        # 登录信息
        self.login_phone = "17757623065"
        self.login_password = "a1234567"
        
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        
        self.base_url = "https://www.jaka.com"
        
        # 基于实际分析结果的完整模块配置（包含所有子分类）
        self.download_modules = {
            "说明书": {
                "url": "https://www.jaka.com/download",
                "selector": "说明书",
                "category": "说明书"
            },
            # 宣传册和产品选型手册的子分类
            "宣传册": {
                "url": "https://www.jaka.com/download",
                "selector": "宣传册",
                "category": "宣传册",
                "parent": "宣传册和产品选型手册"
            },
            "产品选型手册": {
                "url": "https://www.jaka.com/download",
                "selector": "产品选型手册",
                "category": "产品选型手册",
                "parent": "宣传册和产品选型手册"
            },
            "硬件用户手册": {
                "url": "https://www.jaka.com/download", 
                "selector": "硬件用户手册",
                "category": "硬件用户手册"
            },
            "备件手册": {
                "url": "https://www.jaka.com/download",
                "selector": "备件手册",
                "category": "备件手册"
            },
            "服务手册": {
                "url": "https://www.jaka.com/download",
                "selector": "服务手册",
                "category": "服务手册"
            },
            # 模型及图纸的子分类
            "2D-DWG": {
                "url": "https://www.jaka.com/download",
                "selector": "2D-DWG",
                "category": "2D-DWG",
                "parent": "模型及图纸"
            },
            "3D-可拖拽": {
                "url": "https://www.jaka.com/download",
                "selector": "3D-可拖拽",
                "category": "3D-可拖拽",
                "parent": "模型及图纸"
            },
            "3D-STEP": {
                "url": "https://www.jaka.com/download",
                "selector": "3D-STEP",
                "category": "3D-STEP",
                "parent": "模型及图纸"
            },
            # 软件的子分类
            "WebApp": {
                "url": "https://www.jaka.com/download",
                "selector": "WebApp",
                "category": "WebApp",
                "parent": "软件"
            },
            "V1.7.2 JAKA App": {
                "url": "https://www.jaka.com/download",
                "selector": "V1.7.2 JAKA App",
                "category": "V1.7.2 JAKA App",
                "parent": "软件"
            },
            "V1.5 JAKA Zu App": {
                "url": "https://www.jaka.com/download",
                "selector": "V1.5 JAKA Zu App",
                "category": "V1.5 JAKA Zu App",
                "parent": "软件"
            },
            "V1.7.1 JAKA App": {
                "url": "https://www.jaka.com/download",
                "selector": "V1.7.1 JAKA App",
                "category": "V1.7.1 JAKA App",
                "parent": "软件"
            },
            "V1.4 JAKA Zu App": {
                "url": "https://www.jaka.com/download",
                "selector": "V1.4 JAKA Zu App",
                "category": "V1.4 JAKA Zu App",
                "parent": "软件"
            },
            "JAKA Lens 2D": {
                "url": "https://www.jaka.com/download",
                "selector": "JAKA Lens 2D",
                "category": "JAKA Lens 2D",
                "parent": "软件"
            },
            # 二次开发的子分类
            "Addon": {
                "url": "https://www.jaka.com/download",
                "selector": "Addon",
                "category": "Addon",
                "parent": "二次开发"
            },
            "SDK": {
                "url": "https://www.jaka.com/download",
                "selector": "SDK",
                "category": "SDK",
                "parent": "二次开发"
            },
            "TCP": {
                "url": "https://www.jaka.com/download",
                "selector": "TCP",
                "category": "TCP",
                "parent": "二次开发"
            },
            "ROS": {
                "url": "https://www.jaka.com/download",
                "selector": "ROS",
                "category": "ROS",
                "parent": "二次开发"
            },
            # 认证的子分类
            "管理体系认证": {
                "url": "https://www.jaka.com/download",
                "selector": "管理体系认证",
                "category": "管理体系认证",
                "parent": "认证"
            },
            "声明": {
                "url": "https://www.jaka.com/download",
                "selector": "声明",
                "category": "声明",
                "parent": "认证"
            },
            "示例程序": {
                "url": "https://www.jaka.com/download",
                "selector": "示例程序",
                "category": "示例程序"
            },
            "培训视频及课件": {
                "url": "https://www.jaka.com/download",
                "selector": "培训视频及课件",
                "category": "培训视频及课件"
            },
            "白皮书和蓝皮书": {
                "url": "https://www.jaka.com/download",
                "selector": "白皮书和蓝皮书",
                "category": "白皮书和蓝皮书"
            }
        }
        
        # 根据环境选择存储路径
        if platform.system() == "Darwin":  # Mac系统（本地测试）
            self.base_dir = os.path.join(os.getcwd(), "downloads", "节卡")
        elif platform.system() == "Windows":  # Windows系统（本地测试）
            self.base_dir = os.path.join(os.getcwd(), "downloads", "节卡")
        else:  # Linux系统（服务器环境）
            self.base_dir = "/srv/downloads/approved/节卡"
            
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
            chrome_options.add_argument('--headless')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--window-size=1920,1080')
            
            # 使用临时目录作为用户数据目录
            import tempfile
            user_data_dir = tempfile.mkdtemp(prefix="chrome_jaka_")
            chrome_options.add_argument(f'--user-data-dir={user_data_dir}')
            print(f"📁 使用用户数据目录: {user_data_dir}")
            
            # 配置下载设置
            prefs = {
                'intl.accept_languages': 'zh-CN,zh,en-US,en',
                'download.default_directory': self.base_dir,
                'download.prompt_for_download': False,
                'download.directory_upgrade': True,
                'safebrowsing.enabled': True,
                'profile.default_content_settings.popups': 0,
                'profile.default_content_setting_values.automatic_downloads': 1,
                'profile.content_settings.pattern_pairs.*,*.popups': 0,
                'profile.managed_default_content_settings.popups': 0
            }
            chrome_options.add_experimental_option('prefs', prefs)
            
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
            
            # 强制清理残留的Chrome进程，避免冲突
            import subprocess
            try:
                print("🧹 清理残留的浏览器进程...")
                subprocess.run(['pkill', '-f', 'chrome'], capture_output=True, check=False)
                subprocess.run(['pkill', '-f', 'chromedriver'], capture_output=True, check=False)
                subprocess.run(['pkill', '-f', 'msedge'], capture_output=True, check=False)
                time.sleep(2)  # 等待进程完全退出
            except Exception as e:
                print(f"⚠️ 清理进程时出错: {e}")
            
            # 优先尝试使用系统已安装的chromedriver
            print("🔧 优先使用系统ChromeDriver...")
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
            
            print("❌ 系统ChromeDriver不可用，程序无法继续运行")
            print("💡 请确保已正确安装ChromeDriver到系统PATH中")
            
            return False
                
        except Exception as e:
            print(f"❌ Selenium设置失败: {e}")
            return False

    def perform_login(self):
        """执行登录操作"""
        try:
            if not hasattr(self, 'login_phone') or not hasattr(self, 'login_password'):
                print("❌ 未设置登录信息")
                return False
            
            print("🔍 查找登录入口...")
            
            # 访问主页
            self.driver.get("https://www.jaka.com")
            time.sleep(3)
            
            # 尝试找到登录按钮 - 更全面的选择器
            login_selectors = [
                "//a[contains(text(), '登录')]",
                "//button[contains(text(), '登录')]", 
                "//span[contains(text(), '登录')]",
                "//div[contains(text(), '登录')]",
                "//*[contains(@class, 'login')]",
                "//a[contains(@href, 'login')]",
                "//a[contains(@href, 'signin')]",
                "//*[contains(text(), '注册/登录')]",
                "//*[contains(text(), '登录/注册')]",
                ".login-btn",
                ".btn-login",
                "#login",
                "#loginBtn"
            ]
            
            # 使用JavaScript查找登录按钮，更灵活
            login_script = """
            var loginElement = null;
            var loginTexts = ['登录', '注册/登录', '登录/注册', 'login', 'signin'];
            
            // 查找所有可能的登录元素
            var allElements = document.querySelectorAll('a, button, span, div');
            for (var i = 0; i < allElements.length; i++) {
                var element = allElements[i];
                var text = element.textContent.trim();
                var href = element.href || '';
                var className = element.className.toLowerCase();
                
                // 检查文本匹配
                for (var j = 0; j < loginTexts.length; j++) {
                    if (text.includes(loginTexts[j]) || href.includes('login') || 
                        className.includes('login') || element.id.includes('login')) {
                        
                        // 检查元素是否可见
                        var rect = element.getBoundingClientRect();
                        if (rect.width > 0 && rect.height > 0) {
                            console.log('找到登录元素:', text, 'href:', href, 'class:', className);
                            loginElement = element;
                            break;
                        }
                    }
                }
                if (loginElement) break;
            }
            
            return loginElement;
            """
            
            login_element = self.driver.execute_script(login_script)
            
            if not login_element:
                print("❌ 未找到登录按钮，尝试查看页面内容...")
                # 输出页面中可能的登录相关文本，帮助调试
                page_text = self.driver.find_element(By.TAG_NAME, "body").text
                print("页面文本片段:", page_text[:500])
                return False
            
            # 点击登录按钮
            print("🔑 点击登录按钮...")
            self.driver.execute_script("arguments[0].click();", login_element)
            time.sleep(5)  # 增加等待时间，让登录页面完全加载
            
            # 使用JavaScript查找手机号输入框
            phone_script = """
            var phoneInput = null;
            var inputs = document.querySelectorAll('input');
            
            for (var i = 0; i < inputs.length; i++) {
                var input = inputs[i];
                var type = input.type.toLowerCase();
                var placeholder = (input.placeholder || '').toLowerCase();
                var name = (input.name || '').toLowerCase();
                var id = (input.id || '').toLowerCase();
                
                // 检查是否是手机号输入框
                if (type === 'tel' || 
                    placeholder.includes('手机') || placeholder.includes('电话') || placeholder.includes('phone') ||
                    name.includes('phone') || name.includes('mobile') ||
                    id.includes('phone') || id.includes('mobile')) {
                    
                    // 检查输入框是否可见
                    var rect = input.getBoundingClientRect();
                    if (rect.width > 0 && rect.height > 0) {
                        console.log('找到手机号输入框:', placeholder, 'type:', type, 'name:', name);
                        phoneInput = input;
                        break;
                    }
                }
            }
            
            // 如果没找到特定的手机号输入框，查找第一个可见的text输入框
            if (!phoneInput) {
                for (var i = 0; i < inputs.length; i++) {
                    var input = inputs[i];
                    if (input.type === 'text' || input.type === '') {
                        var rect = input.getBoundingClientRect();
                        if (rect.width > 0 && rect.height > 0) {
                            console.log('使用第一个文本输入框作为手机号输入框');
                            phoneInput = input;
                            break;
                        }
                    }
                }
            }
            
            return phoneInput;
            """
            
            phone_input = self.driver.execute_script(phone_script)
            
            if not phone_input:
                print("❌ 未找到手机号输入框")
                return False
            
            # 输入手机号
            print("📱 输入手机号...")
            phone_input.clear()
            phone_input.send_keys(self.login_phone)
            time.sleep(1)
            
            # 首先尝试切换到密码登录标签（如果存在）
            print("🔄 尝试切换到密码登录标签...")
            
            # 先检查当前页面上是否有登录相关的标签
            page_debug_script = """
            var allText = [];
            var allElements = document.querySelectorAll('*');
            for (var i = 0; i < allElements.length; i++) {
                var text = allElements[i].textContent.trim();
                if (text && (text.includes('登录') || text.includes('密码') || text.includes('验证码'))) {
                    allText.push(text);
                }
            }
            return allText.slice(0, 10); // 返回前10个相关文本
            """
            
            debug_texts = self.driver.execute_script(page_debug_script)
            print(f"🔍 当前页面登录相关文本: {debug_texts}")
            
            password_tab_script = """
            var passwordTab = null;
            var allElements = document.querySelectorAll('*');
            
            for (var i = 0; i < allElements.length; i++) {
                var element = allElements[i];
                var text = element.textContent.trim();
                
                // 查找包含"密码登录"的元素
                if (text === '密码登录' || text.includes('密码登录')) {
                    var rect = element.getBoundingClientRect();
                    if (rect.width > 0 && rect.height > 0) {
                        console.log('找到密码登录标签:', text);
                        passwordTab = element;
                        break;
                    }
                }
            }
            
            return passwordTab;
            """
            
            password_tab = self.driver.execute_script(password_tab_script)
            
            if password_tab:
                print("🔑 点击密码登录标签...")
                self.driver.execute_script("arguments[0].click();", password_tab)
                time.sleep(3)  # 增加等待时间，让标签切换完成
                print("✅ 已切换到密码登录模式")
            else:
                print("ℹ️ 未找到密码登录标签，可能已经在密码登录模式")
            
            # 使用JavaScript查找密码输入框（增加详细调试）
            password_script = """
            var passwordInput = null;
            var debugInfo = [];
            var inputs = document.querySelectorAll('input');
            
            debugInfo.push('总共找到 ' + inputs.length + ' 个输入框');
            
            for (var i = 0; i < inputs.length; i++) {
                var input = inputs[i];
                var type = input.type.toLowerCase();
                var placeholder = (input.placeholder || '').toLowerCase();
                var name = (input.name || '').toLowerCase();
                var id = (input.id || '').toLowerCase();
                var className = (input.className || '').toLowerCase();
                
                var rect = input.getBoundingClientRect();
                var isVisible = rect.width > 0 && rect.height > 0;
                
                // 记录所有输入框的信息用于调试
                debugInfo.push('输入框' + i + ': type=' + type + ', placeholder=' + placeholder + 
                              ', name=' + name + ', id=' + id + ', visible=' + isVisible);
                
                // 检查是否是密码输入框
                if (type === 'password' || 
                    placeholder.includes('密码') || placeholder.includes('password') ||
                    name.includes('password') || name.includes('pwd') ||
                    id.includes('password') || id.includes('pwd') ||
                    className.includes('password')) {
                    
                    if (isVisible) {
                        debugInfo.push('找到可见的密码输入框: ' + i);
                        passwordInput = input;
                        break;
                    } else {
                        debugInfo.push('找到但不可见的密码输入框: ' + i);
                    }
                }
            }
            
            return {input: passwordInput, debug: debugInfo};
            """
            
            result = self.driver.execute_script(password_script)
            
            # 输出调试信息
            if result and 'debug' in result:
                print("🔍 密码输入框调试信息:")
                for debug_line in result['debug']:
                    print(f"  {debug_line}")
            
            password_input = result['input'] if result and 'input' in result else None
            
            if not password_input:
                print("❌ 未找到密码输入框")
                return False
            
            # 输入密码
            print("🔐 输入密码...")
            password_input.clear()
            password_input.send_keys(self.login_password)
            time.sleep(1)
            
            # 使用JavaScript查找登录提交按钮
            submit_script = """
            var submitButton = null;
            var buttons = document.querySelectorAll('button, input[type="submit"], a');
            
            for (var i = 0; i < buttons.length; i++) {
                var button = buttons[i];
                var text = button.textContent.trim();
                var type = (button.type || '').toLowerCase();
                var className = button.className.toLowerCase();
                
                // 检查是否是登录提交按钮
                if (text.includes('登录') || text.includes('提交') || text.includes('login') ||
                    type === 'submit' || className.includes('submit') || className.includes('login')) {
                    
                    // 检查按钮是否可见
                    var rect = button.getBoundingClientRect();
                    if (rect.width > 0 && rect.height > 0) {
                        console.log('找到登录提交按钮:', text, 'type:', type, 'class:', className);
                        submitButton = button;
                        break;
                    }
                }
            }
            
            return submitButton;
            """
            
            submit_button = self.driver.execute_script(submit_script)
            
            if not submit_button:
                print("❌ 未找到登录提交按钮")
                return False
            
            # 点击登录
            print("✅ 提交登录...")
            self.driver.execute_script("arguments[0].click();", submit_button)
            time.sleep(5)
            
            # 检查登录是否成功
            print("🔍 检查登录状态...")
            current_url = self.driver.current_url
            page_source = self.driver.page_source
            
            # 使用JavaScript检查登录状态
            login_check_script = """
            var isLoggedIn = false;
            var pageText = document.body.textContent.toLowerCase();
            var currentUrl = window.location.href.toLowerCase();
            
            // 检查登录成功的标志
            var successIndicators = [
                pageText.includes('退出'),
                pageText.includes('logout'),
                pageText.includes('用户中心'),
                pageText.includes('个人中心'),
                pageText.includes('我的'),
                pageText.includes('账户'),
                currentUrl.includes('user'),
                currentUrl.includes('profile'),
                currentUrl.includes('dashboard')
            ];
            
            // 检查是否有用户相关的元素
            var userElements = document.querySelectorAll('[class*="user"], [class*="profile"], [id*="user"]');
            if (userElements.length > 0) {
                successIndicators.push(true);
            }
            
            // 检查是否不再有登录表单
            var loginForms = document.querySelectorAll('form[action*="login"], .login-form, #loginForm');
            var loginInputs = document.querySelectorAll('input[type="password"]');
            if (loginForms.length === 0 && loginInputs.length === 0) {
                successIndicators.push(true);
            }
            
            isLoggedIn = successIndicators.some(function(indicator) { return indicator; });
            
            console.log('登录检查结果:', isLoggedIn);
            console.log('成功指标:', successIndicators);
            console.log('当前URL:', currentUrl);
            
            return isLoggedIn;
            """
            
            is_logged_in = self.driver.execute_script(login_check_script)
            
            if is_logged_in:
                print("✅ 登录成功")
                return True
            else:
                print("❌ 登录可能失败，检查页面内容...")
                print(f"当前URL: {current_url}")
                # 输出页面片段用于调试
                body_text = self.driver.find_element(By.TAG_NAME, "body").text
                print("页面内容片段:", body_text[:300])
                return False
                
        except Exception as e:
            print(f"❌ 登录过程出错: {e}")
            import traceback
            traceback.print_exc()
            return False

    def check_if_login_required(self):
        """检查当前页面是否需要登录"""
        try:
            # 使用JavaScript检查页面是否显示需要登录的信息
            login_required_script = """
            var pageText = document.body.textContent.toLowerCase();
            var needsLogin = false;
            
            // 检查需要登录的标志
            var loginRequiredIndicators = [
                pageText.includes('请先登录'),
                pageText.includes('需要登录'),
                pageText.includes('登录后下载'),
                pageText.includes('请登录'),
                pageText.includes('未登录'),
                document.querySelector('.login-required') !== null,
                document.querySelector('.need-login') !== null
            ];
            
            needsLogin = loginRequiredIndicators.some(function(indicator) { return indicator; });
            
            console.log('登录需求检查:', needsLogin);
            return needsLogin;
            """
            
            needs_login = self.driver.execute_script(login_required_script)
            return needs_login
            
        except Exception as e:
            print(f"检查登录需求失败: {e}")
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
            print(f"🌐 访问JAKA官网: {url}")
            
            self.driver.get(url)
            
            # 等待页面加载
            time.sleep(10)
            
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

    def switch_to_module(self, module_name):
        """切换到指定模块，支持层级结构"""
        try:
            print(f"🔄 切换到模块: {module_name}")
            
            # 检查是否有父级模块需要先展开
            module_config = self.download_modules.get(module_name, {})
            parent_module = module_config.get('parent')
            
            # 等待页面稳定
            time.sleep(2)
            
            # 如果有父级模块，先展开父级
            if parent_module:
                print(f"🔍 检测到父级模块: {parent_module}，先尝试展开")
                parent_success = self._expand_parent_module(parent_module)
                if not parent_success:
                    print(f"⚠️ 无法展开父级模块: {parent_module}")
                    # 继续尝试直接查找子模块
            
            # 查找并点击目标模块
            script = f"""
            var moduleName = '{module_name}';
            var parentModule = '{parent_module or ''}';
            console.log('查找JAKA模块:', moduleName, '父级:', parentModule);
            
            var foundElement = null;
            var bestScore = 0;
            
            // 首先查找所有包含目标文本的元素
            var allElements = document.querySelectorAll('*');
            var candidateElements = [];
            
            for (var i = 0; i < allElements.length; i++) {{
                var element = allElements[i];
                var text = element.textContent.trim();
                
                // 检查元素是否可见
                var style = window.getComputedStyle(element);
                if (style.display === 'none' || style.visibility === 'hidden') {{
                    continue;
                }}
                
                // 精确匹配目标模块名
                if (text === moduleName) {{
                    candidateElements.push({{element: element, score: 20, reason: '精确匹配'}});
                }}
                // 子元素文本精确匹配
                else if (element.children.length === 0 && text === moduleName) {{
                    candidateElements.push({{element: element, score: 18, reason: '子元素精确匹配'}});
                }}
            }}
            
            console.log('找到候选元素数量:', candidateElements.length);
            
            // 评分每个候选元素
            for (var c = 0; c < candidateElements.length; c++) {{
                var candidate = candidateElements[c];
                var element = candidate.element;
                var score = candidate.score;
                
                var rect = element.getBoundingClientRect();
                
                // 检查元素是否在可见区域
                if (rect.width === 0 || rect.height === 0) {{
                    continue;
                }}
                
                // 左侧位置加分（侧边栏）
                if (rect.left < window.innerWidth * 0.4) {{
                    score += 10;
                }}
                
                // 可点击元素加分
                if (element.tagName === 'A' || element.onclick || 
                    element.style.cursor === 'pointer' ||
                    window.getComputedStyle(element).cursor === 'pointer') {{
                    score += 8;
                }}
                
                // 检查父级结构（导航区域）
                var parent = element.parentElement;
                var navLevel = 0;
                while (parent && parent !== document.body && navLevel < 5) {{
                    var className = parent.className.toLowerCase();
                    if (className.includes('nav') || className.includes('menu') || 
                        className.includes('sidebar') || className.includes('list')) {{
                        score += 5;
                        break;
                    }}
                    parent = parent.parentElement;
                    navLevel++;
                }}
                
                // 特别处理子模块：检查是否在父级模块的上下文中
                if (parentModule && (moduleName === '宣传册' || moduleName === '产品选型手册')) {{
                    var parentElement = element.parentElement;
                    var level = 0;
                    while (parentElement && level < 6) {{
                        var parentText = parentElement.textContent;
                        if (parentText.includes(parentModule)) {{
                            score += 15; // 在正确父级下的子模块
                            console.log('找到子模块在父级下:', parentModule);
                            break;
                        }}
                        parentElement = parentElement.parentElement;
                        level++;
                    }}
                    
                    // 检查元素是否有缩进样式（子模块特征）
                    var computedStyle = window.getComputedStyle(element);
                    if (computedStyle.marginLeft && parseFloat(computedStyle.marginLeft) > 10) {{
                        score += 5;
                    }}
                    if (computedStyle.paddingLeft && parseFloat(computedStyle.paddingLeft) > 20) {{
                        score += 5;
                    }}
                }}
                
                console.log('候选元素评分:', element.textContent.trim(), 'score:', score, 'reason:', candidate.reason);
                
                if (score > bestScore) {{
                    bestScore = score;
                    foundElement = element;
                }}
            }}
            
            if (foundElement && bestScore >= 10) {{
                console.log('选中最佳元素:', foundElement.textContent.trim(), '最终得分:', bestScore);
                
                // 滚动到元素位置
                foundElement.scrollIntoView({{block: 'center', behavior: 'smooth'}});
                
                // 等待滚动完成后点击
                setTimeout(function() {{
                    try {{
                        foundElement.click();
                        console.log('模块切换点击成功');
                    }} catch (e) {{
                        console.log('直接点击失败，尝试事件触发:', e);
                        // 尝试点击父元素
                        var clickableParent = foundElement.parentElement;
                        while (clickableParent && clickableParent !== document.body) {{
                            if (clickableParent.onclick || clickableParent.tagName === 'A' || clickableParent.tagName === 'BUTTON') {{
                                clickableParent.click();
                                console.log('通过父元素点击成功');
                                return;
                            }}
                            clickableParent = clickableParent.parentElement;
                        }}
                        
                        // 最后尝试事件触发
                        var event = new MouseEvent('click', {{
                            view: window,
                            bubbles: true,
                            cancelable: true
                        }});
                        foundElement.dispatchEvent(event);
                    }}
                }}, 800);
                
                return true;
            }}
            
            console.log('未找到合适的JAKA模块切换元素');
            return false;
            """
            
            success = self.driver.execute_script(script)
            
            if success:
                time.sleep(8)  # 等待页面切换和内容加载
                
                # 验证切换是否成功（更宽松的验证）
                verify_script = f"""
                var moduleName = '{module_name}';
                
                // 检查页面是否显示了正确的模块内容
                var isCorrectModule = false;
                
                // 方法1: 检查活跃状态（精确匹配和包含匹配）
                var activeElements = document.querySelectorAll('.active, .current, .selected, [class*="active"], [class*="current"]');
                for (var i = 0; i < activeElements.length; i++) {{
                    var text = activeElements[i].textContent.trim();
                    if (text === moduleName || text.includes(moduleName)) {{
                        console.log('找到活跃元素匹配:', text);
                        isCorrectModule = true;
                        break;
                    }}
                }}
                
                // 方法2: 检查页面标题和内容区域
                var contentElements = document.querySelectorAll('h1, h2, h3, .title, .module-title, .content-title, [class*="title"]');
                for (var i = 0; i < contentElements.length; i++) {{
                    var text = contentElements[i].textContent.trim();
                    if (text.includes(moduleName)) {{
                        console.log('找到标题匹配:', text);
                        isCorrectModule = true;
                        break;
                    }}
                }}
                
                // 方法3: 检查URL变化
                if (window.location.href.includes(moduleName) || window.location.hash.includes(moduleName)) {{
                    console.log('URL包含模块名');
                    isCorrectModule = true;
                }}
                
                // 方法4: 检查页面是否有相关内容变化（更宽松）
                if (!isCorrectModule) {{
                    // 检查页面中是否出现了目标模块相关的内容
                    var allText = document.body.textContent;
                    
                    // 简化模块名进行匹配
                    var simpleModuleName = moduleName.replace(/[0-9.]/g, '').trim();
                    if (simpleModuleName && allText.includes(simpleModuleName)) {{
                        console.log('页面内容包含简化模块名:', simpleModuleName);
                        isCorrectModule = true;
                    }}
                    
                    // 检查是否有文件列表或下载内容出现
                    var downloadElements = document.querySelectorAll('[class*="download"], [class*="file"], .file-list, .download-list, a[href*="download"]');
                    if (downloadElements.length > 0) {{
                        console.log('发现下载相关元素，可能切换成功');
                        isCorrectModule = true;
                    }}
                    
                    // 特别处理子模块：如果是宣传册或产品选型手册，检查页面内容变化
                    if (moduleName === '宣传册' || moduleName === '产品选型手册') {{
                        // 检查页面是否有新的文件或内容出现
                        var contentElements = document.querySelectorAll('a, .item, .content, [class*="item"]');
                        var hasNewContent = false;
                        
                        for (var i = 0; i < contentElements.length; i++) {{
                            var elem = contentElements[i];
                            var text = elem.textContent.trim();
                            var href = elem.href || '';
                            
                            // 检查是否包含相关关键词
                            if (text.includes('PDF') || text.includes('下载') || 
                                href.includes('.pdf') || href.includes('download') ||
                                text.includes('宣传') || text.includes('选型') || text.includes('手册')) {{
                                hasNewContent = true;
                                console.log('发现子模块相关内容:', text);
                                break;
                            }}
                        }}
                        
                        if (hasNewContent) {{
                            isCorrectModule = true;
                        }}
                        
                        // 最后的宽松检查：页面内容是否发生了变化（至少有一些新内容）
                        var pageContentLength = document.body.textContent.length;
                        if (pageContentLength > 10000) {{ // 页面有足够的内容
                            console.log('页面内容丰富，假设切换成功');
                            isCorrectModule = true;
                        }}
                    }}
                }}
                
                console.log('模块切换验证结果:', isCorrectModule, '模块名:', moduleName);
                return isCorrectModule;
                """
                
                is_switched = self.driver.execute_script(verify_script)
                
                if is_switched:
                    print(f"✅ 成功切换到 {module_name} 模块")
                    return True
                else:
                    print(f"⚠️ 模块切换验证失败，可能未成功切换到 {module_name}")
                    return False
            else:
                print(f"⚠️ 未找到 {module_name} 模块切换按钮")
                return False
                
        except Exception as e:
            print(f"❌ 切换模块失败: {module_name} - {e}")
            return False

    def _expand_parent_module(self, parent_module):
        """展开父级模块菜单"""
        try:
            print(f"🔍 尝试展开父级模块: {parent_module}")
            
            script = f"""
            var parentModuleName = '{parent_module}';
            console.log('查找父级模块:', parentModuleName);
            
            // 查找父级模块元素
            var foundParent = null;
            var bestScore = 0;
            
            var allElements = document.querySelectorAll('*');
            for (var i = 0; i < allElements.length; i++) {{
                var element = allElements[i];
                var text = element.textContent.trim();
                
                // 检查是否匹配父级模块名
                if (text === parentModuleName) {{
                    var rect = element.getBoundingClientRect();
                    
                    // 检查元素是否可见
                    if (rect.width === 0 || rect.height === 0) {{
                        continue;
                    }}
                    
                    var score = 0;
                    
                    // 左侧位置加分（侧边栏）
                    if (rect.left < window.innerWidth * 0.4) {{
                        score += 10;
                    }}
                    
                    // 可点击元素加分
                    if (element.tagName === 'A' || element.onclick || 
                        window.getComputedStyle(element).cursor === 'pointer') {{
                        score += 15;
                    }}
                    
                    // 检查类名是否包含标题相关的类
                    var className = element.className.toLowerCase();
                    if (className.includes('tit') || className.includes('title') ||
                        className.includes('first') || className.includes('tab')) {{
                        score += 8;
                    }}
                    
                    // 检查父元素是否有列表或容器
                    var parentElement = element.parentElement;
                    if (parentElement) {{
                        var parentClass = parentElement.className.toLowerCase();
                        if (parentClass.includes('first') || parentClass.includes('box') ||
                            parentClass.includes('tab') || parentClass.includes('nav')) {{
                            score += 5;
                        }}
                        
                        // 检查是否有子模块列表（隐藏状态）
                        var siblings = parentElement.parentElement ? parentElement.parentElement.children : [];
                        for (var j = 0; j < siblings.length; j++) {{
                            var sibling = siblings[j];
                            var siblingClass = sibling.className.toLowerCase();
                            var siblingText = sibling.textContent;
                            if ((siblingClass.includes('sec') || siblingClass.includes('list')) &&
                                (siblingText.includes('宣传册') || siblingText.includes('产品选型手册'))) {{
                                score += 10; // 找到对应的子模块列表
                                break;
                            }}
                        }}
                    }}
                    
                    console.log('父级模块候选:', text, 'score:', score, 'tagName:', element.tagName, 'className:', element.className);
                    
                    if (score > bestScore) {{
                        bestScore = score;
                        foundParent = element;
                    }}
                }}
            }}
            
            if (foundParent && bestScore >= 5) {{
                console.log('找到父级模块元素:', foundParent.textContent.trim());
                
                // 滚动到元素位置
                foundParent.scrollIntoView({{block: 'center', behavior: 'smooth'}});
                
                // 尝试点击展开
                setTimeout(function() {{
                    try {{
                        // 先尝试点击父元素本身
                        foundParent.click();
                        console.log('父级模块点击成功');
                        
                        // 如果有展开图标，也尝试点击
                        var parentContainer = foundParent.parentElement;
                        if (parentContainer) {{
                            var expandIcons = parentContainer.querySelectorAll('.icon, .arrow, .plus, [class*="expand"]');
                            for (var i = 0; i < expandIcons.length; i++) {{
                                try {{
                                    expandIcons[i].click();
                                    console.log('展开图标点击成功');
                                }} catch (e) {{
                                    console.log('展开图标点击失败:', e);
                                }}
                            }}
                        }}
                        
                    }} catch (e) {{
                        console.log('父级模块点击失败:', e);
                    }}
                }}, 500);
                
                return true;
            }}
            
            console.log('未找到父级模块元素');
            return false;
            """
            
            success = self.driver.execute_script(script)
            
            if success:
                time.sleep(3)  # 等待展开动画完成
                print(f"✅ 成功展开父级模块: {parent_module}")
                return True
            else:
                print(f"⚠️ 未能展开父级模块: {parent_module}")
                return False
                
        except Exception as e:
            print(f"❌ 展开父级模块失败: {parent_module} - {e}")
            return False

    def extract_detailed_files_from_module(self, module_name):
        """从当前模块页面提取详细文件信息"""
        try:
            print(f"📋 从 {module_name} 模块提取详细文件信息...")
            
            # 等待内容加载
            time.sleep(5)
            
            # 使用宽松的验证逻辑
            module_verify_script = f"""
            var moduleName = '{module_name}';
            
            // 对于子模块，使用更宽松的验证策略
            var isCorrectModule = false;
            
            // 方法1: 检查是否有相关文件内容出现
            var contentElements = document.querySelectorAll('a, .item, .content, [class*="item"], [href*="download"], [href*=".pdf"]');
            for (var i = 0; i < contentElements.length; i++) {{
                var elem = contentElements[i];
                var text = elem.textContent.trim();
                var href = elem.href || '';
                
                if (text.includes('PDF') || text.includes('下载') || 
                    href.includes('.pdf') || href.includes('download')) {{
                    console.log('发现下载内容，模块切换成功');
                    isCorrectModule = true;
                    break;
                }}
            }}
            
            // 方法2: 检查子模块特有内容
            if (!isCorrectModule && (moduleName === '宣传册' || moduleName === '产品选型手册')) {{
                var allText = document.body.textContent;
                if (allText.includes('宣传') || allText.includes('选型') || allText.includes('手册')) {{
                    console.log('发现子模块相关内容');
                    isCorrectModule = true;
                }}
            }}
            
            // 方法3: 检查页面内容是否丰富（有足够内容说明切换成功）
            if (!isCorrectModule) {{
                var pageLength = document.body.textContent.length;
                if (pageLength > 5000) {{
                    console.log('页面内容丰富，假设切换成功');
                    isCorrectModule = true;
                }}
            }}
            
            console.log('模块验证结果:', isCorrectModule, '模块:', moduleName);
            return isCorrectModule;
            """
            
            is_correct_module = self.driver.execute_script(module_verify_script)
            if not is_correct_module:
                print(f"⚠️ 模块切换验证失败，当前页面不是 {module_name} 模块")
                return []
            
            print(f"✅ 模块切换验证成功，当前在 {module_name} 模块")
            
            # 使用改进的方法：只查找当前显示区域的文件
            files_script = f"""
            var files = [];
            var targetModule = '{module_name}';
            
            // 查找当前显示的内容区域
            var contentArea = null;
            var possibleContentAreas = [
                '.content-area',
                '.file-list',
                '.download-content', 
                '.module-content',
                '#content',
                '.main-content'
            ];
            
            for (var i = 0; i < possibleContentAreas.length; i++) {{
                var area = document.querySelector(possibleContentAreas[i]);
                if (area && area.offsetWidth > 0 && area.offsetHeight > 0) {{
                    contentArea = area;
                    break;
                }}
            }}
            
            // 如果没找到特定内容区域，使用可见的最大区域
            if (!contentArea) {{
                var allDivs = document.querySelectorAll('div');
                var maxArea = 0;
                for (var i = 0; i < allDivs.length; i++) {{
                    var div = allDivs[i];
                    var rect = div.getBoundingClientRect();
                    var area = rect.width * rect.height;
                    if (area > maxArea && rect.width > 500 && rect.height > 300) {{
                        maxArea = area;
                        contentArea = div;
                    }}
                }}
            }}
            
            if (!contentArea) {{
                console.log('未找到内容区域，使用整个页面');
                contentArea = document.body;
            }}
            
            console.log('内容区域:', contentArea.className || contentArea.tagName);
            
            // 在内容区域内查找PDF文档容器
            var pdfContainers = contentArea.querySelectorAll('.pdf, .pdf_list, .file-item, [class*="doc"], .download-item');
            
            console.log('找到PDF容器数量:', pdfContainers.length);
            
            pdfContainers.forEach(function(container, index) {{
                if (container.offsetWidth > 0 && container.offsetHeight > 0) {{
                    var fileInfo = {{}};
                    var containerText = container.textContent.trim();
                    
                    // 提取标题
                    var titleElements = container.querySelectorAll('.tit, .title, .name, .pdf_l, h1, h2, h3, h4, h5, h6');
                    for (var i = 0; i < titleElements.length; i++) {{
                        var elem = titleElements[i];
                        var text = elem.textContent.trim();
                        if (text && text.length > 3 && text.length < 100 && 
                            !text.includes('预览') && !text.includes('下载') && 
                            !text.match(/^\\d{{4}}[\\/\\-]\\d{{1,2}}[\\/\\-]\\d{{1,2}}$/)) {{
                            fileInfo.title = text;
                            break;
                        }}
                    }}
                    
                    // 如果没找到专门的标题元素，从容器文本中提取
                    if (!fileInfo.title) {{
                        var cleanText = containerText
                            .replace(/\\d{{4}}[\\/\\-]\\d{{1,2}}[\\/\\-]\\d{{1,2}}/g, '')
                            .replace(/预览|下载|pdf|PDF/g, '')
                            .trim();
                        if (cleanText.length > 3 && cleanText.length < 100) {{
                            fileInfo.title = cleanText;
                        }}
                    }}
                    
                    // 查找日期
                    var dateMatch = containerText.match(/(20\\d{{2}}[\\/\\-]\\d{{1,2}}[\\/\\-]\\d{{1,2}})/);
                    if (dateMatch) {{
                        fileInfo.update_time = dateMatch[1];
                    }}
                    
                    // 查找下载按钮
                    var downloadButton = container.querySelector('a.down, .download-btn, [class*="down"]');
                    if (downloadButton && downloadButton.textContent.includes('下载')) {{
                        fileInfo.download_button = {{
                            className: downloadButton.className,
                            text: downloadButton.textContent.trim()
                        }};
                        fileInfo.has_download = true;
                    }}
                    
                    // 查找文件大小
                    var sizeMatch = containerText.match(/(\\d+(?:\\.\\d+)?\\s*(MB|KB|GB|M|K|G))/i);
                    if (sizeMatch) {{
                        fileInfo.file_size = sizeMatch[1];
                    }}
                    
                    // 确定文件类型
                    if (containerText.toLowerCase().includes('pdf')) {{
                        fileInfo.file_type = 'PDF';
                    }}
                    
                    if (fileInfo.title) {{
                        files.push(fileInfo);
                        console.log('找到文件:', fileInfo.title, '有下载按钮:', !!fileInfo.has_download);
                    }}
                }}
            }});
            
            console.log('提取完成，总文件数:', files.length);
            return files;
            """
            
            files = self.driver.execute_script(files_script)
            
            print(f"📋 从 {module_name} 提取到 {len(files)} 个文件:")
            
            valid_files = []
            for i, file_info in enumerate(files, 1):
                if file_info.get('title'):
                    # 添加模块和分类信息
                    file_info['module'] = module_name
                    file_info['category'] = self.download_modules[module_name]['category']
                    # 添加父模块信息
                    parent_module = self.download_modules[module_name].get('parent')
                    if parent_module:
                        file_info['parent_module'] = parent_module
                    else:
                        file_info['parent_module'] = file_info['category']
                    
                    print(f"  📄 [{i}] {file_info['title'][:60]}...")
                    if file_info.get('update_time'):
                        print(f"       📅 更新时间: {file_info['update_time']}")
                    if file_info.get('file_size'):
                        print(f"       📊 文件大小: {file_info['file_size']}")
                    if file_info.get('has_download'):
                        print(f"       🔗 下载按钮: 已找到")
                    else:
                        print(f"       ⚠️ 无下载按钮")
                    
                    valid_files.append(file_info)
            
            print(f"✅ 从 {module_name} 提取到 {len(valid_files)} 个有效文件")
            return valid_files
            
        except Exception as e:
            print(f"❌ 提取文件信息失败: {module_name} - {e}")
            return []

    def is_file_after_november_2024(self, update_time_str, module_name=None):
        """判断文件是否为2024年11月1日之后更新"""
        if not update_time_str:
            return True  # 没有时间信息的文件也要下载
            
        # 对重要模块放宽时间限制
        important_modules = ["培训视频及课件", "白皮书和蓝皮书", "示例程序"]
        if module_name in important_modules:
            # 对这些模块使用更宽松的时间过滤：2024年1月1日之后
            filter_date = datetime(2024, 1, 1)
        else:
            filter_date = self.filter_date  # 其他模块仍使用11月1日
            
        try:
            # 解析时间
            time_formats = [
                '%Y-%m-%d %H:%M:%S',
                '%Y-%m-%d',
                '%Y/%m/%d %H:%M:%S',
                '%Y/%m/%d',
                '%Y.%m.%d',
                '%Y年%m月%d日'
            ]
            
            # 清理时间字符串
            cleaned_time = re.sub(r'[^\d\-/年月日\.\s:]', '', update_time_str)
            
            parsed_time = None
            for fmt in time_formats:
                try:
                    parsed_time = datetime.strptime(cleaned_time.strip(), fmt)
                    break
                except ValueError:
                    continue
            
            if parsed_time:
                return parsed_time >= filter_date
            else:
                # 如果时间解析失败，也包含这个文件
                return True
            
        except Exception as e:
            if self.debug:
                print(f"时间解析失败: {update_time_str} - {e}")
        
        return True  # 出错时也包含文件

    def crawl_module(self, module_name, module_config):
        """爬取单个模块"""
        print(f"\n🔍 开始爬取JAKA模块: {module_name}")
        print("-" * 50)
        
        try:
            # 访问页面
            html_content = self.get_page_content_selenium(module_config['url'])
            if not html_content:
                print(f"❌ 无法获取 {module_name} 页面内容")
                return []
            
            # 切换到指定模块
            if not self.switch_to_module(module_name):
                print(f"⚠️ 无法切换到 {module_name} 模块，跳过")
                return []
            
            # 提取文件列表
            all_files = self.extract_detailed_files_from_module(module_name)
            
            # 处理每个文件
            valid_files = []
            for i, file_info in enumerate(all_files, 1):
                print(f"  🔍 [{i}/{len(all_files)}] 检查: {file_info['title'][:40]}...")
                
                # 检查时间过滤
                if self.is_file_after_november_2024(file_info.get('update_time'), module_name):
                    print(f"    ✅ 符合时间条件: {file_info.get('update_time', 'N/A')}")
                    valid_files.append(file_info)
                    
                    # 检查文件状态
                    file_key = f"{file_info['module']}_{file_info['title']}"
                    if file_key not in self.processed_files:
                        self.new_files.append(file_info)
                        print(f"    🆕 新文件")
                    elif self.check_file_update(file_info):
                        self.updated_files.append(file_info)
                        print(f"    🔄 更新文件")
                    else:
                        print(f"    ⏭️ 无变化")
                    
                    self.processed_files[file_key] = file_info
                else:
                    print(f"    ⏰ 不符合时间条件: {file_info.get('update_time', 'N/A')}")
                
                time.sleep(0.5)
            
            print(f"✅ {module_name} 爬取完成，找到 {len(valid_files)} 个符合条件的文件")
            return valid_files
            
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

    def download_file(self, file_info, max_retries=2):
        """下载文件，支持重试机制"""
        for attempt in range(max_retries + 1):
            try:
                if attempt > 0:
                    print(f"🔄 第{attempt + 1}次尝试下载: {file_info.get('title', 'Unknown')}")
                
                if not file_info.get('has_download'):
                    print(f"⚠️ 跳过无下载按钮的文件: {file_info.get('title', 'Unknown')}")
                    return False
                
                category = file_info['category']
                title = file_info['title']
                
                # 使用原始文件名，只清理不安全的字符
                filename = self.clean_filename(title)
            
                # 根据文件类型确定扩展名
                if file_info.get('file_type') == 'PDF':
                    file_ext = '.pdf'
                else:
                    file_ext = '.zip'  # 默认
                
                # 确保文件名有正确的扩展名
                if not filename.lower().endswith(file_ext.lower()):
                    filename += file_ext
                
                # 检查文件是否已存在
                file_key = f"{file_info['module']}_{title}"
                if file_key in self.processed_urls:
                    print(f"文件已处理过，跳过: {title}")
                    return False
            
                print(f"📥 开始下载: {title}")
                
                # 确保目录存在（按父模块分目录）
                parent_module = file_info.get('parent_module', category)
                save_dir = os.path.join(self.base_dir, parent_module)
                os.makedirs(save_dir, exist_ok=True)
                
                # 记录开始时间和现有文件
                download_start_time = time.time()
                
                # 检查实际下载目录（Chrome下载到base_dir）
                download_dir = self.base_dir
                existing_files = set()
                if os.path.exists(download_dir):
                    try:
                        existing_files = set(os.listdir(download_dir))
                    except:
                        pass
                else:
                    # 如果目录不存在，创建它
                    try:
                        os.makedirs(download_dir, exist_ok=True)
                    except:
                        pass
            
                # 使用JavaScript点击下载按钮
                download_script = f"""
            var title = '{title}';
            console.log('开始查找下载按钮，目标文件:', title);
            
            // 查找所有可能的下载按钮
            var downloadSelectors = [
                'a.down',
                '.download-btn',
                '[class*="down"]',
                'a[href*="download"]',
                'a[href*=".pdf"]',
                'a:contains("下载")',
                '.btn-download',
                '.download-link'
            ];
            
            var allButtons = [];
            downloadSelectors.forEach(function(selector) {{
                try {{
                    var elements = document.querySelectorAll(selector);
                    for (var i = 0; i < elements.length; i++) {{
                        allButtons.push(elements[i]);
                    }}
                }} catch (e) {{
                    // 忽略不支持的选择器
                }}
            }});
            
            // 也查找包含"下载"文本的链接
            var allLinks = document.querySelectorAll('a');
            for (var i = 0; i < allLinks.length; i++) {{
                var link = allLinks[i];
                if (link.textContent.includes('下载') || link.textContent.includes('PDF')) {{
                    allButtons.push(link);
                }}
            }}
            
            console.log('找到候选下载按钮数量:', allButtons.length);
            
            // 去重
            var uniqueButtons = [];
            var seenButtons = new Set();
            for (var i = 0; i < allButtons.length; i++) {{
                var button = allButtons[i];
                var buttonId = button.outerHTML;
                if (!seenButtons.has(buttonId)) {{
                    seenButtons.add(buttonId);
                    uniqueButtons.push(button);
                }}
            }}
            
            console.log('去重后按钮数量:', uniqueButtons.length);
            
            var bestButton = null;
            var bestScore = 0;
            
            // 评分每个按钮
            for (var i = 0; i < uniqueButtons.length; i++) {{
                var button = uniqueButtons[i];
                var score = 0;
                
                // 检查按钮是否可见
                var rect = button.getBoundingClientRect();
                if (rect.width === 0 || rect.height === 0) {{
                    continue;
                }}
                
                var buttonText = button.textContent.trim().toLowerCase();
                var href = button.href || '';
                
                // 基础分：包含下载关键词
                if (buttonText.includes('下载')) {{
                    score += 20;
                }}
                if (buttonText.includes('pdf')) {{
                    score += 15;
                }}
                if (href.includes('.pdf')) {{
                    score += 25;
                }}
                if (href.includes('download')) {{
                    score += 20;
                }}
                
                // 查找按钮的容器，看是否与目标文件相关
                var container = button.closest('.pdf, .pdf_list, .file-item, .download-item, [class*="pdf"], [class*="file"]');
                if (!container) {{
                    // 向上查找3级父元素
                    var parent = button.parentElement;
                    var level = 0;
                    while (parent && level < 3) {{
                        var className = parent.className.toLowerCase();
                        if (className.includes('pdf') || className.includes('file') || className.includes('item')) {{
                            container = parent;
                            break;
                        }}
                        parent = parent.parentElement;
                        level++;
                    }}
                }}
                
                // 检查容器内容是否匹配文件标题
                if (container) {{
                    var containerText = container.textContent;
                    
                    // 提取标题关键词进行匹配
                    var titleWords = title.split(/[\\s\\-_,，。]+/).filter(function(word) {{
                        return word.length > 2 && !word.match(/^\\\\d+$/);
                    }});
                    
                    var matchCount = 0;
                    for (var j = 0; j < titleWords.length && j < 5; j++) {{
                        if (containerText.includes(titleWords[j])) {{
                            matchCount++;
                        }}
                    }}
                    
                    if (matchCount > 0) {{
                        score += matchCount * 10; // 每个匹配的关键词加10分
                        console.log('容器匹配得分:', matchCount * 10, '关键词:', titleWords.slice(0, 5));
                    }}
                    
                    // 检查是否是第一个或最后一个按钮（通常是目标按钮）
                    var containerButtons = container.querySelectorAll('a, button');
                    var buttonIndex = Array.from(containerButtons).indexOf(button);
                    if (buttonIndex === 0 || buttonIndex === containerButtons.length - 1) {{
                        score += 5;
                    }}
                }}
                
                // 检查按钮类名
                var className = button.className.toLowerCase();
                if (className.includes('down')) {{
                    score += 10;
                }}
                if (className.includes('pdf')) {{
                    score += 10;
                }}
                
                // 位置加分：右侧按钮通常是下载按钮
                if (rect.right > window.innerWidth * 0.7) {{
                    score += 5;
                }}
                
                console.log('按钮评分:', buttonText, 'score:', score, 'href:', href);
                
                if (score > bestScore) {{
                    bestScore = score;
                    bestButton = button;
                }}
            }}
            
            if (bestButton && bestScore > 10) {{
                console.log('选中最佳下载按钮:', bestButton.textContent, '得分:', bestScore);
                
                // 滚动到按钮位置
                bestButton.scrollIntoView({{block: 'center', behavior: 'smooth'}});
                
                // 等待滚动完成后点击
                setTimeout(function() {{
                    try {{
                        // 确保按钮仍然可见
                        var currentRect = bestButton.getBoundingClientRect();
                        if (currentRect.width > 0 && currentRect.height > 0) {{
                            bestButton.click();
                            console.log('下载按钮点击成功');
                        }} else {{
                            throw new Error('按钮不可见');
                        }}
                    }} catch (e) {{
                        console.log('直接点击失败，尝试事件触发:', e);
                        var event = new MouseEvent('click', {{
                            view: window,
                            bubbles: true,
                            cancelable: true
                        }});
                        bestButton.dispatchEvent(event);
                        console.log('事件触发完成');
                    }}
                }}, 1500);
                
                return true;
            }} else {{
                console.log('未找到合适的下载按钮，最高得分:', bestScore);
                return false;
            }}
                """
                
                success = self.driver.execute_script(download_script)
                
                if success:
                    print(f"✅ 点击下载按钮成功")
                
                                        # 智能等待下载完成
                    print("⏳ 等待下载完成...")
                    download_completed = False
                    max_wait_time = 60  # 最大等待60秒，对于大文件需要更长时间
                    check_interval = 2  # 每2秒检查一次
                    wait_count = 0

                    while wait_count < max_wait_time and not download_completed:
                        time.sleep(check_interval)
                        wait_count += check_interval
                        
                        print(f"⏳ 检查下载进度... ({wait_count}/{max_wait_time}秒)")
                        
                        # 检查Chrome默认下载目录中的新文件
                        current_files = []
                        crdownload_files = []
                        
                        try:
                            if os.path.exists(download_dir):
                                current_files_in_dir = set(os.listdir(download_dir))
                                new_files = current_files_in_dir - existing_files
                                
                                for file in new_files:
                                    file_path = os.path.join(download_dir, file)
                                    if os.path.isfile(file_path):
                                        mtime = os.path.getmtime(file_path)
                                        # 检查是否是下载开始后创建的文件
                                        if mtime >= download_start_time - 5:  # 允许5秒误差
                                            if file.endswith('.crdownload'):
                                                crdownload_files.append(file_path)
                                                print(f"  📥 正在下载: {file}")
                                            else:
                                                current_files.append(file_path)
                                                print(f"  ✅ 完成文件: {file}")
                        except OSError:
                            continue
                    
                        # 如果有完成的文件，检查是否有效并移动到目标目录
                        if current_files:
                            latest_file = max(current_files, key=os.path.getmtime)
                            file_size = os.path.getsize(latest_file)
                            
                            # 检查文件大小是否合理（大于1KB）
                            if file_size > 1024:
                                # 使用期望的文件名和目录路径
                                target_path = os.path.join(save_dir, filename)
                                
                                # 移动文件到正确的目录并重命名
                                try:
                                    # 如果目标文件已存在，先删除
                                    if os.path.exists(target_path):
                                        os.remove(target_path)
                                    
                                    # 移动并重命名文件
                                    import shutil
                                    shutil.move(latest_file, target_path)
                                    print(f"📝 文件移动并重命名: {os.path.basename(latest_file)} -> {parent_module}/{filename}")
                                except Exception as e:
                                    print(f"⚠️ 移动文件失败: {str(e)}")
                                    # 如果移动失败，尝试复制
                                    try:
                                        import shutil
                                        shutil.copy2(latest_file, target_path)
                                        os.remove(latest_file)
                                        print(f"📝 文件复制并重命名: {os.path.basename(latest_file)} -> {parent_module}/{filename}")
                                    except Exception as e2:
                                        print(f"⚠️ 复制也失败，使用原文件: {str(e2)}")
                                        target_path = latest_file
                                
                                print(f"✅ 下载完成: {title}")
                                print(f"📁 文件路径: {target_path}")
                                print(f"📊 文件大小: {file_size:,} bytes")
                                
                                # 记录已处理
                                self.processed_urls.add(file_key)
                                
                                # 更新文件信息
                                file_info['path'] = target_path
                                file_info['size_bytes'] = file_size
                                
                                download_completed = True
                                return True

                            else:
                                print(f"⚠️ 文件太小，可能下载失败: {file_size} bytes")
                    
                        # 如果只有.crdownload文件，继续等待
                        elif crdownload_files:
                            # 检查.crdownload文件大小是否在增长
                            largest_crdownload = max(crdownload_files, key=os.path.getsize)
                            crdownload_size = os.path.getsize(largest_crdownload)
                            print(f"  📥 下载中: {crdownload_size:,} bytes")
                            
                            # 如果.crdownload文件很大，说明下载正在进行
                            if crdownload_size > 1024 * 100:  # 大于100KB
                                print(f"  🔄 下载进行中，文件大小: {crdownload_size:,} bytes")
                            
                            continue
                        else:
                            print(f"  🔍 未发现下载文件...")
                            
                            # 如果连续10秒都没有发现文件，可能下载失败
                            if wait_count >= 10:
                                print(f"  ⚠️ 连续{wait_count}秒未发现下载文件，可能下载失败")
                                break
                
                    # 最后一次检查：下载可能刚完成
                    if not download_completed:
                        print("🔍 最后一次检查下载结果...")
                        time.sleep(3)
                        
                        try:
                            # 检查Chrome默认下载目录
                            if os.path.exists(download_dir):
                                current_files_in_dir = set(os.listdir(download_dir))
                                new_files = current_files_in_dir - existing_files
                                
                                final_files = []
                                for file in new_files:
                                    file_path = os.path.join(download_dir, file)
                                    if os.path.isfile(file_path) and not file.endswith('.crdownload'):
                                        mtime = os.path.getmtime(file_path)
                                        if mtime >= download_start_time - 5:
                                            final_files.append(file_path)
                                
                                if final_files:
                                    latest_file = max(final_files, key=os.path.getmtime)
                                    file_size = os.path.getsize(latest_file)
                                    
                                    if file_size > 1024:  # 至少1KB
                                        # 使用期望的文件名和目录路径
                                        target_path = os.path.join(save_dir, filename)
                                        
                                        # 移动文件到正确的目录并重命名
                                        try:
                                            # 如果目标文件已存在，先删除
                                            if os.path.exists(target_path):
                                                os.remove(target_path)
                                            
                                            # 移动并重命名文件
                                            import shutil
                                            shutil.move(latest_file, target_path)
                                            print(f"📝 文件移动并重命名: {os.path.basename(latest_file)} -> {parent_module}/{filename}")
                                        except Exception as e:
                                            print(f"⚠️ 移动文件失败: {str(e)}")
                                            # 如果移动失败，尝试复制
                                            try:
                                                import shutil
                                                shutil.copy2(latest_file, target_path)
                                                os.remove(latest_file)
                                                print(f"📝 文件复制并重命名: {os.path.basename(latest_file)} -> {parent_module}/{filename}")
                                            except Exception as e2:
                                                print(f"⚠️ 复制也失败，使用原文件: {str(e2)}")
                                                target_path = latest_file
                                        
                                        print(f"✅ 延迟检测到下载完成: {title}")
                                        print(f"📁 文件路径: {target_path}")
                                        print(f"📊 文件大小: {file_size:,} bytes")
                                        
                                        self.processed_urls.add(file_key)
                                        file_info['path'] = target_path
                                        file_info['size_bytes'] = file_size
                                        
                                        return True
                        except OSError:
                            pass
                
                    print(f"❌ 下载超时或失败: {title}")
                    
                    # 检查是否需要登录
                    if attempt == 0:  # 只在第一次失败时检查登录
                        needs_login = self.check_if_login_required()
                        if needs_login:
                            print("🔑 检测到需要登录，尝试重新登录...")
                            login_success = self.perform_login()
                            if login_success:
                                print("✅ 重新登录成功，继续下载...")
                                # 重新访问下载页面
                                self.driver.get("https://www.jaka.com/download")
                                time.sleep(3)
                                # 重新切换到模块
                                if self.switch_to_module(file_info['module']):
                                    continue  # 重新尝试下载
                    
                    if attempt < max_retries:
                        print(f"⏳ {3}秒后重试...")
                        time.sleep(3)
                        continue
                    return False
                else:
                    print(f"❌ 未找到下载按钮: {title}")
                    
                    # 检查是否需要登录
                    if attempt == 0:  # 只在第一次失败时检查登录
                        needs_login = self.check_if_login_required()
                        if needs_login:
                            print("🔑 检测到需要登录，尝试重新登录...")
                            login_success = self.perform_login()
                            if login_success:
                                print("✅ 重新登录成功，继续下载...")
                                continue  # 重新尝试下载
                    
                    if attempt < max_retries:
                        print(f"⏳ {3}秒后重试...")
                        time.sleep(3)
                        continue
                    return False
                
            except Exception as e:
                print(f"❌ 下载失败 (尝试{attempt + 1}/{max_retries + 1}): {file_info.get('title', 'Unknown')} - {e}")
                if attempt < max_retries:
                    print(f"⏳ {5}秒后重试...")
                    time.sleep(5)
                    continue
                else:
                    import traceback
                    traceback.print_exc()
                    return False
        
        return False  # 所有重试都失败

    def clean_filename(self, filename):
        """清理文件名，确保在各种操作系统下都能正常使用"""
        if not filename:
            return "unknown_file"
        
        # 移除或替换非法字符
        illegal_chars = ['/', '\\', ':', '*', '?', '"', '<', '>', '|', '\n', '\r', '\t']
        cleaned = filename
        
        for char in illegal_chars:
            cleaned = cleaned.replace(char, '_')
        
        # 移除多余的空格和特殊字符
        cleaned = re.sub(r'\s+', ' ', cleaned)  # 多个空格变成一个
        cleaned = re.sub(r'[^\w\s\-_.()\[\]{}\u4e00-\u9fff]', '_', cleaned)  # 保留中文、字母、数字、常用符号
        
        # 移除开头和结尾的空格、点号、下划线
        cleaned = cleaned.strip(' ._-')
        
        # 限制长度，但保留重要信息
        if len(cleaned) > 100:
            # 保留前70个字符和后20个字符（通常包含扩展名）
            cleaned = cleaned[:70] + "..." + cleaned[-20:]
        
        # 确保不为空
        if not cleaned:
            cleaned = "unknown_file"
        
        return cleaned

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

    def send_notifications(self):
        """发送通知"""
        try:
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # 如果有新文件或更新文件，发送通知
            if self.new_files or self.updated_files:
                message = f"""✅ JAKA完整爬虫 检测完成

📊 检测结果:
  新增文件: {len(self.new_files)} 个
  更新文件: {len(self.updated_files)} 个

📅 时间范围: 仅2024年11月1日后的文件
🔍 检测模块: {', '.join(self.download_modules.keys())}

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
📁 文件存放路径: {self.base_dir}
⏰ 检测时间: {current_time}"""
                
                self.send_dingtalk_notification(message)
            else:
                # 没有新文件或更新
                message = f"""✅ JAKA完整爬虫 检测完成

📊 检测结果: 无新增或更新文件
📅 时间范围: 仅2024年11月1日后的文件
⏰ 检测时间: {current_time}"""
                
                self.send_dingtalk_notification(message)
                
        except Exception as e:
            print(f"发送通知失败: {e}")

    def run(self):
        """主运行函数"""
        print("🚀 JAKA完整爬虫启动...")
        print("🎯 目标: JAKA官网所有下载模块的完整检测")
        print("📅 时间过滤: 仅2024年11月1日之后的文件")
        print("🔍 功能: 分类识别 + 详情提取 + 时间过滤 + 钉钉通知")
        print()
        
        # 创建目录
        os.makedirs(self.base_dir, exist_ok=True)
        
        try:
            # 设置Selenium
            if not self.setup_selenium():
                print("❌ 无法启动Selenium，爬取终止")
                return
            
            # 执行登录
            print("🔑 开始登录流程...")
            login_success = self.perform_login()
            if login_success:
                print("✅ 登录成功，可以下载需要登录的文件")
            else:
                print("⚠️ 初次登录失败，稍后在需要时重新尝试登录")
                # 继续运行，在下载时如果遇到需要登录的情况会重新尝试
            
            all_files = []
            total_success_count = 0
            
            # 遍历所有模块，每个模块爬取后立即下载
            for module_name, module_config in self.download_modules.items():
                try:
                    print(f"\n🔍 开始处理模块: {module_name}")
                    
                    # 清空当前模块的文件列表
                    current_module_new_files = []
                    current_module_updated_files = []
                    
                    # 记录当前new_files和updated_files的长度
                    before_new_count = len(self.new_files)
                    before_updated_count = len(self.updated_files)
                    
                    # 爬取模块
                    files = self.crawl_module(module_name, module_config)
                    all_files.extend(files)
                    
                    # 获取当前模块新增的文件
                    current_module_new_files = self.new_files[before_new_count:]
                    current_module_updated_files = self.updated_files[before_updated_count:]
                    
                    # 立即下载当前模块的文件
                    module_download_files = current_module_new_files + current_module_updated_files
                    module_success_count = 0
                    
                    if module_download_files:
                        print(f"📥 开始下载 {module_name} 模块的 {len(module_download_files)} 个文件...")
                        for i, file_info in enumerate(module_download_files, 1):
                            print(f"\n[{i}/{len(module_download_files)}] {module_name}模块下载...")
                            if self.download_file(file_info):
                                module_success_count += 1
                                total_success_count += 1
                            time.sleep(1)  # 下载间隔
                        
                        print(f"✅ {module_name} 模块下载完成: {module_success_count}/{len(module_download_files)} 成功")
                    else:
                        print(f"⏭️ {module_name} 模块无需下载文件")
                    
                    time.sleep(3)  # 模块间隔
                    
                except Exception as e:
                    print(f"❌ 模块 {module_name} 处理异常: {e}")
                    continue
            
            print(f"\n📊 最终总结:")
            print(f"  总计符合条件文件: {len(all_files)}")
            print(f"  总计新增文件: {len(self.new_files)}")
            print(f"  总计更新文件: {len(self.updated_files)}")
            print(f"  总计成功下载: {total_success_count}")
            
            success_count = total_success_count
            
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

def main():
    print("🚀 JAKA完整爬虫最终版本")
    print("基于网站结构深度分析的完整实现")
    print("=" * 60)
    
    import sys
    
    if not SELENIUM_AVAILABLE:
        print("❌ 无法使用Selenium版本")
        print("📋 请安装: pip install selenium webdriver-manager")
        return
    
    spider = JakaFinalSpider()
    
    # 测试模式：只测试一个模块
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        # 临时修改模块列表，只包含说明书模块
        original_modules = spider.download_modules.copy()
        spider.download_modules = {
            "说明书": original_modules["说明书"]
        }
        print("🎯 测试模式：只爬取说明书模块")
    
    spider.run()

if __name__ == "__main__":
    main()
