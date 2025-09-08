#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
创优科技产品分类和文件夹重命名脚本
基于官网产品分类进行文件夹重命名
"""

import os
import sys
import json
import requests
from bs4 import BeautifulSoup
import re
import argparse
from urllib.parse import urljoin, urlparse
import time

class ChuangyouProductClassifier:
    def __init__(self, base_dir):
        self.base_dir = base_dir
        self.download_dir = os.path.join(base_dir, "产品资料下载")
        self.base_url = "https://www.cuhnj.com"
        
        # 从官网获取的产品分类映射
        self.official_product_series = {
            'SDVC10': {
                'name': 'SDVC10系列:自动稳压振动送料控制器',
                'models': ['SDVC10-XS', 'SDVC10-S', 'SDVC11-S', 'SDVC11-M', 'SDVC14-S'],
                'specs': {'SDVC10-XS': '2.5A', 'SDVC10-S': '4A', 'SDVC11-S': '4A', 'SDVC11-M': '5A', 'SDVC14-S': '4A'}
            },
            'SDVC20': {
                'name': 'SDVC20系列:智能数字调压振动送料控制器',
                'models': ['SDVC20-S', 'SDVC20-L', 'SDVC21-S', 'SDVC20-U', 'SDVC22-S', 'SDVC22-L', 'SDVC21-LP', 'SDVC21-XLP'],
                'specs': {'SDVC20-S': '5A', 'SDVC20-L': '10A', 'SDVC21-S': '5A', 'SDVC20-U': '50A', 'SDVC22-S': '5A', 'SDVC22-L': '10A', 'SDVC21-LP': '10A', 'SDVC21-XLP': '25A'}
            },
            'SDVC31': {
                'name': 'SDVC311系列:智能数字调频振动送料控制器',
                'models': ['SDVC311-S', 'SDVC311-M', 'SDVC33-M', 'SDVC301-S', 'SDVC301-M', 'SDVC302-S', 'SDVC302-M', 'SDVC31-S', 'SDVC31-M', 'SDVC31-L', 'SDVC31-XL', 'SDVC31-U'],
                'specs': {'SDVC311-S': '1.5A', 'SDVC311-M': '3.0A', 'SDVC33-M': '3.5A', 'SDVC31-S': '1.5A', 'SDVC31-M': '3.0A', 'SDVC31-L': '4.5A', 'SDVC31-XL': '6A', 'SDVC31-U': '10A'}
            },
            'SDVC40': {
                'name': 'SDVC40系列:智能压电数字调频振动送料控制器',
                'models': ['SDVC40-S', 'SDVC41-M', 'SDVC42-SD', 'SDVC42-S', 'SDVC40-XS2', 'SDVC40-XS3', 'SDVC40-XS4'],
                'specs': {'SDVC40-S': '150mA', 'SDVC41-M': '300mA', 'SDVC42-SD': '600mA', 'SDVC42-S': '150mA', 'SDVC40-XS2': '50mA*2', 'SDVC40-XS3': '50mA*3', 'SDVC40-XS4': '50mA*4'}
            },
            'SDVC34': {
                'name': 'SDVC34系列:智能数字自动调频振动送料控制器',
                'models': ['SDVC341-M', 'SDVC34-MR', 'SDVC34-MRJ', 'SDVC34-XLR', 'SDVC34-UR', 'SDVC35-MRJ'],
                'specs': {}
            },
            'SDVS30': {
                'name': 'SDVS30系列:智能数字防护振动送料控制器',
                'models': ['SDVS31', 'SDVS30', 'SDVS301'],
                'specs': {}
            },
            'SDVC60': {
                'name': 'SDVC60系列:柔性送料控制器（驱动电磁铁）',
                'models': ['SDVC6014-M', 'SDVC6014-XL', 'SDVC6024-M', 'SDVC6024-XL', 'SDVC621-M', 'SDVC61-M'],
                'specs': {}
            },
            'SDUC20': {
                'name': 'SDUC20系列:柔性送料控制器（驱动音圈电机）',
                'models': ['SDUC20-US', 'SDUC20-UM'],
                'specs': {'SDUC20-US': '1.5A', 'SDUC20-UM': '3.0A'}
            },
            'SDMC': {
                'name': 'SDMC系列:智能光纤物料分选控制器',
                'models': ['SDMC10-S', 'SDMC20-S', 'SDMC30-S', 'SDMC201-S'],
                'specs': {}
            }
        }
        
        # 产品名称到系列的映射
        self.product_name_to_series = {
            '自动稳压振动送料控制器': 'SDVC10',
            '智能数字调压振动送料控制器': 'SDVC20', 
            '智能数字调频振动送料控制器': 'SDVC31',
            '智能压电数字调频振动送料控制器': 'SDVC40',
            '智能数字自动调频振动送料控制器': 'SDVC34',
            '智能数字防护振动送料控制器': 'SDVS30',
            '柔性送料控制器（驱动电磁铁）': 'SDVC60',
            '柔性送料控制器（驱动音圈电机）': 'SDUC20',
            '智能光纤物料分选控制器': 'SDMC',
            '数字单相交流异步电机控制器': 'SDAC',  # 如果有的话
            '智能数字超声波焊接控制器': 'SDUW'     # 如果有的话
        }

    def fetch_latest_product_info(self):
        """从官网获取最新的产品信息"""
        try:
            print("正在从创优科技官网获取最新产品信息...")
            
            # 获取产品中心页面
            response = requests.get(f"{self.base_url}/href/html/prodXlPic", timeout=10)
            response.encoding = 'utf-8'
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # 解析产品分类（这里需要根据实际网页结构调整）
                # 由于网页结构可能复杂，我们先使用已知的产品分类
                print("✓ 成功获取产品信息")
                return True
            else:
                print(f"⚠️ 无法访问官网，使用内置产品分类 (状态码: {response.status_code})")
                return False
                
        except Exception as e:
            print(f"⚠️ 获取官网信息失败，使用内置产品分类: {e}")
            return False

    def scan_existing_folders(self):
        """扫描现有文件夹"""
        if not os.path.exists(self.download_dir):
            print(f"目录不存在: {self.download_dir}")
            return []
        
        folders = []
        for item in os.listdir(self.download_dir):
            item_path = os.path.join(self.download_dir, item)
            if os.path.isdir(item_path):
                folders.append(item)
        
        return sorted(folders)

    def classify_folder_by_content(self, folder_name):
        """根据文件夹内容和官网分类标准分类文件夹"""
        folder_path = os.path.join(self.download_dir, folder_name)
        
        if not os.path.exists(folder_path):
            return folder_name
        
        try:
            # 获取文件夹中的所有文件
            items = os.listdir(folder_path)
            
            # 统计各系列产品数量
            series_matches = {}
            
            for series_code, series_info in self.official_product_series.items():
                count = 0
                for item in items:
                    for model in series_info['models']:
                        # 检查文件名是否包含该型号
                        if model.replace('-', '').upper() in item.replace('-', '').upper():
                            count += 1
                            break
                
                if count > 0:
                    series_matches[series_code] = count
            
            # 如果没有匹配到任何型号，尝试根据现有文件夹名称推断
            if not series_matches:
                # 提取现有文件夹名中的产品名称
                for product_name, series_code in self.product_name_to_series.items():
                    if product_name in folder_name:
                        return self.official_product_series[series_code]['name']
                
                return folder_name
            
            # 找到匹配最多的系列
            best_series = max(series_matches, key=series_matches.get)
            return self.official_product_series[best_series]['name']
            
        except Exception as e:
            print(f"分析文件夹失败 {folder_name}: {e}")
            return folder_name

    def extract_product_name_from_folder(self, folder_name):
        """从文件夹名称中提取产品名称"""
        if ':' in folder_name:
            return folder_name.split(':', 1)[1]
        
        # 检查是否包含已知产品名称
        for product_name in self.product_name_to_series.keys():
            if product_name in folder_name:
                return product_name
        
        return "智能数字调频振动送料控制器"  # 默认产品名

    def generate_rename_plan(self):
        """生成重命名计划"""
        existing_folders = self.scan_existing_folders()
        if not existing_folders:
            return []
        
        rename_plan = []
        
        for folder_name in existing_folders:
            # 根据官网分类标准重新分类
            new_name = self.classify_folder_by_content(folder_name)
            
            if new_name != folder_name:
                rename_plan.append({
                    'old_name': folder_name,
                    'new_name': new_name,
                    'reason': '基于官网产品分类标准'
                })
        
        return rename_plan

    def rename_folders(self, dry_run=True):
        """执行文件夹重命名"""
        # 获取最新产品信息
        self.fetch_latest_product_info()
        
        existing_folders = self.scan_existing_folders()
        if not existing_folders:
            print("未找到任何文件夹")
            return
        
        print(f"发现现有文件夹 {len(existing_folders)} 个:")
        for folder in existing_folders:
            print(f"  - {folder}")
        print()
        
        # 生成重命名计划
        rename_plan = self.generate_rename_plan()
        
        if not rename_plan:
            print("所有文件夹名称都已符合官网分类标准，无需重命名")
            return
        
        print(f"重命名计划 ({len(rename_plan)} 个文件夹):")
        print("-" * 80)
        
        for plan in rename_plan:
            print(f"原名: {plan['old_name']}")
            print(f"新名: {plan['new_name']}")
            print(f"理由: {plan['reason']}")
            print()
        
        if dry_run:
            print("⚠️ 这是预览模式，实际重命名请运行: python 创优产品分类脚本.py --execute")
            return
        
        # 执行重命名
        success_count = 0
        for plan in rename_plan:
            old_path = os.path.join(self.download_dir, plan['old_name'])
            new_path = os.path.join(self.download_dir, plan['new_name'])
            
            try:
                if os.path.exists(new_path):
                    print(f"❌ 目标文件夹已存在，跳过: {plan['new_name']}")
                    continue
                
                os.rename(old_path, new_path)
                print(f"✓ 重命名成功: {plan['old_name']} -> {plan['new_name']}")
                success_count += 1
                
            except Exception as e:
                print(f"❌ 重命名失败: {plan['old_name']} -> {e}")
        
        print(f"\n重命名完成，成功 {success_count}/{len(rename_plan)} 个文件夹")

    def show_official_classification(self):
        """显示官网产品分类"""
        print("创优科技官网产品分类标准:")
        print("=" * 80)
        
        for series_code, series_info in self.official_product_series.items():
            print(f"\n{series_info['name']}")
            print(f"型号: {', '.join(series_info['models'])}")
            if series_info['specs']:
                print("规格:")
                for model, spec in series_info['specs'].items():
                    print(f"  - {model}: {spec}")

def main():
    parser = argparse.ArgumentParser(description='创优科技产品分类和文件夹重命名脚本')
    parser.add_argument('--base-dir', default='/Volumes/KIOXIA/产品资料下载',
                        help='基础目录路径')
    parser.add_argument('--execute', action='store_true',
                        help='执行重命名操作（默认为预览模式）')
    parser.add_argument('--show-classification', action='store_true',
                        help='显示官网产品分类标准')
    
    args = parser.parse_args()
    
    classifier = ChuangyouProductClassifier(args.base_dir)
    
    if args.show_classification:
        classifier.show_official_classification()
        return
    
    classifier.rename_folders(dry_run=not args.execute)

if __name__ == "__main__":
    main()
