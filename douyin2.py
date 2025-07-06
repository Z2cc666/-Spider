#!/home/inspur/anaconda3/bin/python
# -*- coding: UTF-8 -*-
import requests
import json
import os
import re
from typing import List, Dict

class DouyinVideoDownloader:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Referer': 'https://www.douyin.com/',
        #    'Cookie': '__ac_nonce=06851770400d77b3576aa; __ac_signature=_02B4Z6wo00f01Aim1iwAAIDB1.0lYqVr2ugIhtKAAGp.a5; enter_pc_once=1; UIFID_TEMP=e71d819f1cb72e7166823ce125547a3e5a83b631a52f7c0b3c34cd9714dd602d8f6822b0ee9a4181a0294fccbfa8b20de9ecec40e3ce0e0edea019d09aec85047846f01ea768acf6654ff5441e8e848e; x-web-secsdk-uid=41634112-f6b4-4d73-b87e-d81ddd7cae00; douyin.com; s_v_web_id=verify_mc0ln6uj_gsKU2lU3_NeF4_4XUI_9uKR_BnfH9P94ES3H; device_web_cpu_core=8; device_web_memory_size=8; architecture=amd64; hevc_supported=true; dy_swidth=1536; dy_sheight=864; strategyABtestKey=%221750169353.924%22; volume_info=%7B%22volume%22%3A0.6%2C%22isMute%22%3Afalse%7D; xgplayer_user_id=347213054805; passport_csrf_token=2654d4b0f00de9f622e818a13ee3c5a8; passport_csrf_token_default=2654d4b0f00de9f622e818a13ee3c5a8; xg_device_score=7.658235294117647; __security_mc_1_s_sdk_crypt_sdk=4452a320-4d22-afea; bd_ticket_guard_client_web_domain=2; fpk1=U2FsdGVkX1/EJ9Y+sMPnDspHKc2y0O9MttWrGQ6tcikG4YfB+V8AMtgzCl+rbqYC1pW7EZOb06j642kkSrmxIQ==; fpk2=0fe6feb54289f4c67027ec06cc2131f8; passport_mfa_token=CjcFmhScwoaO2386vsZ9dF4NJ%2By5TzMYwUd2zBAUMIKn99ALwmNaMvm%2BwiFoOd1rGbB6TwtYxGiIGkoKPAAAAAAAAAAAAABPIArbC8qVRNkV4aOCVtZg1ElcnEywMW%2BxhHC0K8nQ%2B%2FR1kcwWCYg9QTkBDObqF5G0%2BRDZrfQNGPax0WwgAiIBAwFzYtA%3D; d_ticket=17bc8c09d2dd9fb904981fb43996b38dca513; passport_assist_user=CkGDnu-20I479RzU4-8n6wjTsg_Mzewjal3lag_eWn9LCfPfnMG-A_lCGfPR6WQkD4jyr-YJ4OvNFg8JlJzNo2rd6RpKCjwAAAAAAAAAAAAATyCEVxeO1AZ63s0RQfRhUQ4lgFDLgm-tI31BvP_TGJxjZD6jTPo18PsJhBSDIPrKAzMQy630DRiJr9ZUIAEiAQN_IHX0; n_mh=iKJwgWOXsyG1BeO2jEaQPInak02p3QQm9lN7bQKyNz0; sid_guard=f1d9d51f5c95663d56ce0d5a601316cc%7C1750169448%7C5184000%7CSat%2C+16-Aug-2025+14%3A10%3A48+GMT; uid_tt=46139f88352e3bb3aeeb22d36246b3d9; uid_tt_ss=46139f88352e3bb3aeeb22d36246b3d9; sid_tt=f1d9d51f5c95663d56ce0d5a601316cc; sessionid=f1d9d51f5c95663d56ce0d5a601316cc; sessionid_ss=f1d9d51f5c95663d56ce0d5a601316cc; is_staff_user=false; sid_ucp_v1=1.0.0-KDI2YTJkNDRkZDExMzgzYmVkYWI4MTg3NjlmOGFhYTEwYjFlNjNmMTgKIQj3pYCz9PXbBRDo7sXCBhjvMSAMMNzN0f8FOAdA9AdIBBoCaGwiIGYxZDlkNTFmNWM5NTY2M2Q1NmNlMGQ1YTYwMTMxNmNj; ssid_ucp_v1=1.0.0-KDI2YTJkNDRkZDExMzgzYmVkYWI4MTg3NjlmOGFhYTEwYjFlNjNmMTgKIQj3pYCz9PXbBRDo7sXCBhjvMSAMMNzN0f8FOAdA9AdIBBoCaGwiIGYxZDlkNTFmNWM5NTY2M2Q1NmNlMGQ1YTYwMTMxNmNj; login_time=1750169448966; UIFID=e71d819f1cb72e7166823ce125547a3e5a83b631a52f7c0b3c34cd9714dd602d8f6822b0ee9a4181a0294fccbfa8b20d62ef43c8e7bab226a1b5df4ed034375035c5d3c2e228e1296a9aa2782f26b4897fe5653ad794302eff18036dba0e95eb14d1e2d30695dbbd45a960234db22130d8c1d43276f5390cc69e17633cb99fac7611b8a44aeae59498f2c385ec82c5d759568f5161d5cccc169def716978e3a4; SelfTabRedDotControl=%5B%5D; is_dash_user=1; _bd_ticket_crypt_cookie=4742189a39c3d981e1a4e43fe5401370; __security_mc_1_s_sdk_sign_data_key_web_protect=5e366c22-4cf8-8d8b; __security_mc_1_s_sdk_cert_key=7a4c6090-4d2e-8eff; __security_server_data_status=1; ttwid=1%7CmbMuRBuRX5FM-IxMWAU3eHLUIoLwf0nwn9AHqsHsUBM%7C1750169455%7Cad3bba17441ccff4b929e082854a82279d5c4115f8636dcf2c508f371c4780dc; biz_trace_id=2cc54153; publish_badge_show_info=%220%2C0%2C0%2C1750169455700%22; stream_player_status_params=%22%7B%5C%22is_auto_play%5C%22%3A0%2C%5C%22is_full_screen%5C%22%3A0%2C%5C%22is_full_webscreen%5C%22%3A0%2C%5C%22is_mute%5C%22%3A0%2C%5C%22is_speed%5C%22%3A1%2C%5C%22is_visible%5C%22%3A0%7D%22; stream_recommend_feed_params=%22%7B%5C%22cookie_enabled%5C%22%3Atrue%2C%5C%22screen_width%5C%22%3A1536%2C%5C%22screen_height%5C%22%3A864%2C%5C%22browser_online%5C%22%3Atrue%2C%5C%22cpu_core_num%5C%22%3A8%2C%5C%22device_memory%5C%22%3A8%2C%5C%22downlink%5C%22%3A10%2C%5C%22effective_type%5C%22%3A%5C%224g%5C%22%2C%5C%22round_trip_time%5C%22%3A50%7D%22; bd_ticket_guard_client_data=eyJiZC10aWNrZXQtZ3VhcmQtdmVyc2lvbiI6MiwiYmQtdGlja2V0LWd1YXJkLWl0ZXJhdGlvbi12ZXJzaW9uIjoxLCJiZC10aWNrZXQtZ3VhcmQtcmVlLXB1YmxpYy1rZXkiOiJCQ3IybTJRMUNsT29CbmE2eUloRjVRVHAra01JOVd3TWtaRFh2bHJoUDROOTc5RHRGdHp1eFBPV0xZMW5kNXhEZkczQnJjSWgwUFlKY3d2d2Z3OGpmSkk9IiwiYmQtdGlja2V0LWd1YXJkLXdlYi12ZXJzaW9uIjoyfQ%3D%3D; home_can_add_dy_2_desktop=%221%22; odin_tt=a674135af84d648c8e8a258494662eb2f6273b23db7c5f15ce90f4218a7ad89b971353bdef31be85a956b70881cc07717cbff8cf6b4f633587ab94fb98af4d33; download_guide=%223%2F20250617%2F0%22; SEARCH_RESULT_LIST_TYPE=%22single%22; IsDouyinActive=false; passport_fe_beating_status=false'
            # 'Cookie': 'douyin.com; UIFID_TEMP=4be83ecefa579a300714166db9e569bafd8689fc248d1e190e384db8df203b81e402f41a8b8b3e502cf76133b6ef49bab751a206b9b67b019b2f498368c5a223edc01ec7ec43226e2cea611c264fac4d3563088be0f1da62167fdd2befb61dd16913a58c664b3553829765bc8600771f; s_v_web_id=verify_m9zgexsi_8kk4O8qo_zx2e_4sgX_B0Zc_8REfOtEfKkgL; hevc_supported=true; WebUgChannelId=%2230004%22; xgplayer_user_id=765112718722; enter_pc_once=1; douyin.com; device_web_cpu_core=8; device_web_memory_size=8; architecture=amd64; dy_swidth=1440; dy_sheight=960; strategyABtestKey=%221750124908.666%22; volume_info=%7B%22isUserMute%22%3Afalse%2C%22isMute%22%3Afalse%2C%22volume%22%3A0.5%7D; passport_csrf_token=6a4e3ad1b9ed722b646363ae1710ad8c; passport_csrf_token_default=6a4e3ad1b9ed722b646363ae1710ad8c; fpk1=U2FsdGVkX19oAZysuIjrNvDH9VeoNrrcCNpbbHJOBUOHlNGQvILnudXckmIEmUYqHEY+zgBf4e5fXTK42hHcnA==; fpk2=0e0369e2813db7deb26e5937c353aab4; xg_device_score=6.8747058823529406; is_dash_user=1; UIFID=28ea90c1b0cf804752225259882c701fb12323f08ef828fc1032b615e29efbbea9d4f174b4c5d4676e3128e66084d9fd05c73a8ea31050ad79b84643cbd66b915f6ae6107b6d9937af2901d4bbad161928e13856980ce98ad1022a6ddc277942f5cba6885a9ed12c4d488c29882d853067838729129d07cc850b68f0c2893c7b4164f28dfea6f620f37346f1cbd740433420dbdf165c02a24242caa3943f45761112f0061a27b66d768ca47e65045659d06b437c909b54a48d61d74c55cea163; __security_mc_1_s_sdk_crypt_sdk=b1cd1e3f-4fe5-93d2; bd_ticket_guard_client_web_domain=2; stream_player_status_params=%22%7B%5C%22is_auto_play%5C%22%3A0%2C%5C%22is_full_screen%5C%22%3A0%2C%5C%22is_full_webscreen%5C%22%3A0%2C%5C%22is_mute%5C%22%3A0%2C%5C%22is_speed%5C%22%3A1%2C%5C%22is_visible%5C%22%3A0%7D%22; download_guide=%223%2F20250617%2F0%22; WallpaperGuide=%7B%22showTime%22%3A0%2C%22closeTime%22%3A0%2C%22showCount%22%3A0%2C%22cursor1%22%3A10%2C%22cursor2%22%3A2%7D; d_ticket=6629989ce59a70125c15f79f58474b6b97c83; n_mh=iKJwgWOXsyG1BeO2jEaQPInak02p3QQm9lN7bQKyNz0; is_staff_user=false; SelfTabRedDotControl=%5B%5D; FOLLOW_NUMBER_YELLOW_POINT_INFO=%22MS4wLjABAAAAKJ2U1CsbukObFz4uqcOKrYyL9REJnfPiR-EC-h-DJMLaz0gSX_baJCGVzLvHq5Fp%2F1750176000000%2F0%2F1750126635246%2F0%22; __security_mc_1_s_sdk_cert_key=ae101f24-4580-8340; __security_server_data_status=1; publish_badge_show_info=%220%2C0%2C0%2C1750126639930%22; ttwid=1%7CS5Jxo7Qx-F1BuxXIGOMOkNoK5JVnDDxvsfG2FvOq-gM%7C1750126641%7Ca288ca25387c7a4eb550e2bbd6c709236ea4d9e61d06400b071d164d14749217; biz_trace_id=d85b672d; passport_mfa_token=CjfhyfxpMJt10A1hQagLlU49D2Qy87EmEzTx%2By2FyVwEi%2Fya0UGXenYedBo721Ywbemjrp%2BmPL10GkoKPAAAAAAAAAAAAABPH%2F8Ga4BWILxW9faqKC%2BJhVBS1PmUwFLrZIFhm%2BTwNoIUj%2F2E3acE7aLNPe55WVyvNBDhpvQNGPax0WwgAiIBA7DE75Q%3D; passport_assist_user=CkEcnQQHwbiANjKIaBwK5lUH3Lja_zOQGrt-YdIqG8txPvajKD3eW0-xq5I17y_OCpT0phek1R6Vd0qLVBxZVn90-xpKCjwAAAAAAAAAAAAATx-Dqlfqf4-1quKkYRD6DfmCCI_AXZbfouP8loXh4sd34d6HpF6OMaaGpP07Lc3cCPAQ4ab0DRiJr9ZUIAEiAQP7oY9N; passport_auth_status=3beb450c588cb608973364e19bba6038%2C48d75387228c76f99e88a658c50bf2b4; passport_auth_status_ss=3beb450c588cb608973364e19bba6038%2C48d75387228c76f99e88a658c50bf2b4; sid_guard=e35133a6c1bddd4092ba32993a8434f3%7C1750126931%7C5183999%7CSat%2C+16-Aug-2025+02%3A22%3A10+GMT; uid_tt=5b9596fd1e2f807609bef46535a1d50d; uid_tt_ss=5b9596fd1e2f807609bef46535a1d50d; sid_tt=e35133a6c1bddd4092ba32993a8434f3; sessionid=e35133a6c1bddd4092ba32993a8434f3; sessionid_ss=e35133a6c1bddd4092ba32993a8434f3; sid_ucp_v1=1.0.0-KDdhYjY5ZjA2NmM1YWZjMjdjOWU1ZGI1ZTFhZGQwNWE3YjQ2NjBiNmEKIQj3pYCz9PXbBRDTosPCBhjvMSAMMNzN0f8FOAJA8QdIBBoCaGwiIGUzNTEzM2E2YzFiZGRkNDA5MmJhMzI5OTNhODQzNGYz; ssid_ucp_v1=1.0.0-KDdhYjY5ZjA2NmM1YWZjMjdjOWU1ZGI1ZTFhZGQwNWE3YjQ2NjBiNmEKIQj3pYCz9PXbBRDTosPCBhjvMSAMMNzN0f8FOAJA8QdIBBoCaGwiIGUzNTEzM2E2YzFiZGRkNDA5MmJhMzI5OTNhODQzNGYz; login_time=1750126931678; __ac_nonce=06850d153008e6ae52b0f; __ac_signature=_02B4Z6wo00f01YbK64AAAIDAWZEYzQ3se22G6u8AAAnj99; stream_recommend_feed_params=%22%7B%5C%22cookie_enabled%5C%22%3Atrue%2C%5C%22screen_width%5C%22%3A1440%2C%5C%22screen_height%5C%22%3A960%2C%5C%22browser_online%5C%22%3Atrue%2C%5C%22cpu_core_num%5C%22%3A8%2C%5C%22device_memory%5C%22%3A8%2C%5C%22downlink%5C%22%3A7.3%2C%5C%22effective_type%5C%22%3A%5C%224g%5C%22%2C%5C%22round_trip_time%5C%22%3A150%7D%22; _bd_ticket_crypt_cookie=a7501f60a7bcb25a8486321d4350087d; __security_mc_1_s_sdk_sign_data_key_web_protect=5008d1eb-4c20-9047; bd_ticket_guard_client_data=eyJiZC10aWNrZXQtZ3VhcmQtdmVyc2lvbiI6MiwiYmQtdGlja2V0LWd1YXJkLWl0ZXJhdGlvbi12ZXJzaW9uIjoxLCJiZC10aWNrZXQtZ3VhcmQtcmVlLXB1YmxpYy1rZXkiOiJCUHN4OTU5RXd0OFBLOFZ0Z1BudVBpQ0dUSi96ek13ZU9Kd2tBQmNRSlZXbmpoeUMwZTZ1RkdPdDRlQnIwejgrNVpKcDdCVXJnMFdVck44bFdDUkpJSGs9IiwiYmQtdGlja2V0LWd1YXJkLXdlYi12ZXJzaW9uIjoyfQ%3D%3D; home_can_add_dy_2_desktop=%221%22; odin_tt=9788040638e0e7d4d68b663a98172486626741fb9133da18ca432dba9f1723a30dc648133b43db180cb794cf25ef3cb344fe012dcaaa8777731dc03228056218; IsDouyinActive=false; passport_fe_beating_status=false',  # 必须添加有效的Cookie
        
            'Cookie': 'douyin.com; xg_device_score=6.8747058823529406; device_web_cpu_core=8; device_web_memory_size=8; architecture=amd64; UIFID_TEMP=4be83ecefa579a300714166db9e569bafd8689fc248d1e190e384db8df203b81e402f41a8b8b3e502cf76133b6ef49bab751a206b9b67b019b2f498368c5a223edc01ec7ec43226e2cea611c264fac4d3563088be0f1da62167fdd2befb61dd16913a58c664b3553829765bc8600771f; s_v_web_id=verify_m9zgexsi_8kk4O8qo_zx2e_4sgX_B0Zc_8REfOtEfKkgL; hevc_supported=true; WebUgChannelId=%2230004%22; xgplayer_user_id=765112718722; enter_pc_once=1; passport_csrf_token=6a4e3ad1b9ed722b646363ae1710ad8c; passport_csrf_token_default=6a4e3ad1b9ed722b646363ae1710ad8c; fpk1=U2FsdGVkX19oAZysuIjrNvDH9VeoNrrcCNpbbHJOBUOHlNGQvILnudXckmIEmUYqHEY+zgBf4e5fXTK42hHcnA==; fpk2=0e0369e2813db7deb26e5937c353aab4; UIFID=28ea90c1b0cf804752225259882c701fb12323f08ef828fc1032b615e29efbbea9d4f174b4c5d4676e3128e66084d9fd05c73a8ea31050ad79b84643cbd66b915f6ae6107b6d9937af2901d4bbad161928e13856980ce98ad1022a6ddc277942f5cba6885a9ed12c4d488c29882d853067838729129d07cc850b68f0c2893c7b4164f28dfea6f620f37346f1cbd740433420dbdf165c02a24242caa3943f45761112f0061a27b66d768ca47e65045659d06b437c909b54a48d61d74c55cea163; __security_mc_1_s_sdk_crypt_sdk=b1cd1e3f-4fe5-93d2; bd_ticket_guard_client_web_domain=2; d_ticket=6629989ce59a70125c15f79f58474b6b97c83; n_mh=iKJwgWOXsyG1BeO2jEaQPInak02p3QQm9lN7bQKyNz0; SelfTabRedDotControl=%5B%5D; __security_mc_1_s_sdk_cert_key=ae101f24-4580-8340; __security_server_data_status=1; passport_mfa_token=CjfhyfxpMJt10A1hQagLlU49D2Qy87EmEzTx%2By2FyVwEi%2Fya0UGXenYedBo721Ywbemjrp%2BmPL10GkoKPAAAAAAAAAAAAABPH%2F8Ga4BWILxW9faqKC%2BJhVBS1PmUwFLrZIFhm%2BTwNoIUj%2F2E3acE7aLNPe55WVyvNBDhpvQNGPax0WwgAiIBA7DE75Q%3D; passport_auth_status=3beb450c588cb608973364e19bba6038%2C48d75387228c76f99e88a658c50bf2b4; passport_auth_status_ss=3beb450c588cb608973364e19bba6038%2C48d75387228c76f99e88a658c50bf2b4; __ac_nonce=0685bd70e0086d6b05201; __ac_signature=_02B4Z6wo00f01L0sCywAAIDBYnf4YGDef1i9DA-AAEcV2a; dy_swidth=1440; dy_sheight=960; stream_recommend_feed_params=%22%7B%5C%22cookie_enabled%5C%22%3Atrue%2C%5C%22screen_width%5C%22%3A1440%2C%5C%22screen_height%5C%22%3A960%2C%5C%22browser_online%5C%22%3Atrue%2C%5C%22cpu_core_num%5C%22%3A8%2C%5C%22device_memory%5C%22%3A8%2C%5C%22downlink%5C%22%3A10%2C%5C%22effective_type%5C%22%3A%5C%224g%5C%22%2C%5C%22round_trip_time%5C%22%3A200%7D%22; strategyABtestKey=%221750849302.108%22; sdk_source_info=7e276470716a68645a606960273f276364697660272927676c715a6d6069756077273f276364697660272927666d776a68605a607d71606b766c6a6b5a7666776c7571273f275e58272927666a6b766a69605a696c6061273f27636469766027292762696a6764695a7364776c6467696076273f275e582729277672715a646971273f2763646976602729277f6b5a666475273f2763646976602729276d6a6e5a6b6a716c273f2763646976602729276c6b6f5a7f6367273f27636469766027292771273f273234343335363c313d35303234272927676c715a75776a716a666a69273f2763646976602778; bit_env=WqIKCU8T7YLbsQ3LVDTaMLtKEXNIe9EMj7dz5uJ42zScyPnL-BKCOJz9Gf3hVtVWEBn_Rri7msIUXZBBP92O55DbPW7aMgmLKQjtVVMafOMFo_7adlZYRqceGWvc8tRiYxD9LSFDQfIk16_dEdowAUhEW-U__2jyWE2N85Nj48Qnt2BkGvd0pi9XQlqhlL8eik9-9uo-eXmvbFMYfquaht8T9CgMjXBU1YFM8-Xdy5nLyvTpKkxuBGAtcQStGIsRNQRMAg-Gab9WuBab-NOmmzLaqbXDppzqeVDNqweSk8yG6FKA3D007Y_qF_c-2rV-JBD8ZaEU_gBxsdYXWPbs7-PlwSV-pFgHM5vaS4J-iOkdk8WqcP1vbF-WqW_iGCzFeteTTEsrkgTrocrRdjqo4FssU-Vt4fNpx0SuYcXeqBXEu1ZYhiZF2D8CsXh-Sm1YIa0CDQOf22SeSP-atc1L4E_mjCXhRWii8P9rCzZ3t4Eei_5iCNlXP1Lc_yTTabmx; gulu_source_res=eyJwX2luIjoiODE4ZmQ4N2E4MDc2ZjQwYjA4ZjJiNjYwZTg5MGFhM2JmYTMyNTg1MzhhODVhZTBlYjZkM2M5YTZkNWU0ZDEzZCJ9; passport_auth_mix_state=drurtt7xwbpxcwee1korgn7qmf7oug99; ttwid=1%7CS5Jxo7Qx-F1BuxXIGOMOkNoK5JVnDDxvsfG2FvOq-gM%7C1750849312%7C7094e093ca75def00f5d13c60154c5eac8f1d552afad378afbd7def0500fad60; biz_trace_id=54ceb25f; passport_assist_user=CkHLsULRGNDrdViaQUm9clPurYOS6nodOesP7uuAluDkr3nF0Q-OArYbelv7sictWuIlA3J6eOYZl0rfTTJBZaad9hpKCjwAAAAAAAAAAAAATygSUm19z7U2go5_H9egEYnVhB6bIVMmDR9YZJwpCBY-IUT6vMXGEL5g6EOG5rcU5jQQ8oT1DRiJr9ZUIAEiAQOV_8X3; sid_guard=1088656f29b742d2489e48df17362cb7%7C1750849326%7C5184000%7CSun%2C+24-Aug-2025+11%3A02%3A06+GMT; uid_tt=deecb81129532a5c6edaa918c82bb013; uid_tt_ss=deecb81129532a5c6edaa918c82bb013; sid_tt=1088656f29b742d2489e48df17362cb7; sessionid=1088656f29b742d2489e48df17362cb7; sessionid_ss=1088656f29b742d2489e48df17362cb7; session_tlb_tag=sttt%7C8%7CEIhlbym3QtJInkjfFzYst_________-oo4GMVfTYyObdcxbxbmQ8bmaeTBPQAACoZ0D3lK9D7G8%3D; is_staff_user=false; sid_ucp_v1=1.0.0-KGNkMjA0YzRkMGQ0ZTVlZDE5YTgxZTI4MzZmZDU0ZDFiNTEwNmE3ZDAKIQj3pYCz9PXbBRCuru_CBhjvMSAMMNzN0f8FOAdA9AdIBBoCbGYiIDEwODg2NTZmMjliNzQyZDI0ODllNDhkZjE3MzYyY2I3; ssid_ucp_v1=1.0.0-KGNkMjA0YzRkMGQ0ZTVlZDE5YTgxZTI4MzZmZDU0ZDFiNTEwNmE3ZDAKIQj3pYCz9PXbBRCuru_CBhjvMSAMMNzN0f8FOAdA9AdIBBoCbGYiIDEwODg2NTZmMjliNzQyZDI0ODllNDhkZjE3MzYyY2I3; login_time=1750849326941; publish_badge_show_info=%220%2C0%2C0%2C1750849327411%22; _bd_ticket_crypt_cookie=a53cd019c76187cbf34634e34bf9cfde; __security_mc_1_s_sdk_sign_data_key_web_protect=77f96ce8-4938-b5e7; bd_ticket_guard_client_data=eyJiZC10aWNrZXQtZ3VhcmQtdmVyc2lvbiI6MiwiYmQtdGlja2V0LWd1YXJkLWl0ZXJhdGlvbi12ZXJzaW9uIjoxLCJiZC10aWNrZXQtZ3VhcmQtcmVlLXB1YmxpYy1rZXkiOiJCUHN4OTU5RXd0OFBLOFZ0Z1BudVBpQ0dUSi96ek13ZU9Kd2tBQmNRSlZXbmpoeUMwZTZ1RkdPdDRlQnIwejgrNVpKcDdCVXJnMFdVck44bFdDUkpJSGs9IiwiYmQtdGlja2V0LWd1YXJkLXdlYi12ZXJzaW9uIjoyfQ%3D%3D; home_can_add_dy_2_desktop=%221%22; odin_tt=bb8bf2bc47abe602408f05cc00e00918430124301f6826d4fba0af50e8e1f6611ed416fb965ad11fff698e2573cd947bd0a5d0c6d98d1ab2d42322bc323b07d7; is_dash_user=1; IsDouyinActive=false; passport_fe_beating_status=false' 
       }
        self.session = requests.Session()
        self.session.headers.update(self.headers)

    def clean_filename(self, title):
        """移除文件名中的非法字符"""
        title = re.sub(r'[\/:*?"<>|]', '', title)
        return title[:60]  # 限制长度

    def get_real_video_url(self, video_url):
        """获取真实的视频播放地址"""
        try:
            # 先访问一次获取重定向地址
            response = self.session.get(video_url, allow_redirects=False)
            if response.status_code in [301, 302]:
                return response.headers['Location']
            return video_url
        except Exception as e:
            print(f"获取真实视频地址失败: {e}")
            return None

 

    def get_user_videos(self, sec_uid: str, max_count: int = 1700) -> List[Dict]:
        """获取用户视频列表 - 带分页功能"""
        api_url = "https://www.douyin.com/aweme/v1/web/aweme/post/" 
        videos = []
        max_cursor = 0
        has_more = True
        
        while has_more and len(videos) < max_count:
            params = {
                "sec_user_id": sec_uid,
                "count": "20",  # 每次请求20个
                "max_cursor": str(max_cursor),
                "aid": "6383",
                "cookie_enabled": "true",
                "platform": "PC",
                "downlink": "10",
            }

            try:
                response = self.session.get(api_url, params=params)
                
                if response.url.startswith('https://www.douyin.com/security'):
                    print("⚠️ 触发抖音安全验证，请更新Cookie或使用浏览器手动验证")
                    break
                    
                data = response.json()
                new_videos = data.get('aweme_list', [])
                videos.extend(new_videos)
                
                has_more = data.get('has_more', False)
                max_cursor = data.get('max_cursor', max_cursor + len(new_videos))
                
                print(f"已获取 {len(videos)}/{max_count} 个视频")
                
            except Exception as e:
                print(f"获取视频列表失败: {e}")
                break
        
        return videos[:max_count]  # 确保不超过请求的最大数量


    def download_video(self, video_info: Dict, save_dir: str = "抖音") -> bool:
        """下载单个视频"""
        if not os.path.exists(save_dir):
            os.makedirs(save_dir)

        try:
            # 获取无水印视频URL
            # 初始播放地址
            video_url = video_info.get("video", {}).get("play_addr", {}).get("url_list", [None])[0]
            if not video_url:
                print("无法获取视频URL")
                return False
            
            # 获取真实视频地址
            real_url = self.get_real_video_url(video_url.replace("playwm", "play"))
            if not real_url:
                return False
            
            # 处理文件名
            desc = self.clean_filename(video_info.get("desc", "无标题"))
            desc = desc[:50]
            video_id = video_info.get("aweme_id", "未知ID")
            filename = f"{desc}_{video_id}.mp4"
            filepath = os.path.join(save_dir, filename)
            
            # 下载视频
            print(f"正在下载: {filename}")
            response = self.session.get(real_url, stream=True)
            response.raise_for_status()
            
            with open(filepath, "wb") as f:
                for chunk in response.iter_content(chunk_size=1024*1024):
                    if chunk:
                        f.write(chunk)
            
            print(f"✅ 下载成功: {filename}")
            return True
        except Exception as e:
            print(f"❌ 下载失败: {e}")
            return False

    def download_user_videos(self, sec_uid: str, max_count: int = 1700):
        """下载用户所有视频"""
        videos = self.get_user_videos(sec_uid, max_count)
        if not videos:
            print("未获取到任何视频信息")
            return
        
        print(f"获取到 {len(videos)} 个视频")
        success_count = 0
        for idx, video in enumerate(videos, 1):
            print(f"\n正在处理第 {idx}/{len(videos)} 个视频...")
            if self.download_video(video):
                success_count += 1
        
        print(f"\n下载完成! 成功下载 {success_count}/{len(videos)} 个视频")


if __name__ == "__main__":
    downloader = DouyinVideoDownloader()
    
    # 示例用户sec_uid
    sec_uid = "MS4wLjABAAAA4YleGW2OxYpSy7s5YfvuV8wO0FPz183Mu2c8xANgbMM"
    
    # 下载前5个视频
    downloader.download_user_videos(sec_uid, max_count=1700)