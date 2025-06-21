import json
import pandas as pd
from datetime import datetime
import os
import zipfile
import fnmatch
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.text import Text
import glob

PROFILE_BASE_PATH = ["Profile", "Profile Info", "ProfileMap"]
USER_NAME_PATH = PROFILE_BASE_PATH + ["userName"]
EMAIL_PATH = PROFILE_BASE_PATH + ["emailAddress"]
PHONE_PATH = PROFILE_BASE_PATH + ["telephoneNumber"]
BIO_DESC_PATH = PROFILE_BASE_PATH + ["bioDescription"]
BIRTH_DATE_PATH = PROFILE_BASE_PATH + ["birthDate"]
LIKES_RECEIVED_PATH = PROFILE_BASE_PATH + ["likesReceived"]

APP_SETTINGS_BASE_PATH = ["App Settings", "Settings", "SettingsMap"]
APP_LANG_PATH = APP_SETTINGS_BASE_PATH + ["App Language"]
PRIVATE_ACCOUNT_PATH = APP_SETTINGS_BASE_PATH + ["Private Account"]
PERSONALIZED_ADS_PATH = APP_SETTINGS_BASE_PATH + ["PersonalizedAds"]
FYP_KEYWORD_FILTERS_PATH = APP_SETTINGS_BASE_PATH + ["Content Preferences", "Keyword filters for videos in For You feed"]

WATCH_HISTORY_PATH = ["Your Activity", "Watch History", "VideoList"]
LIKE_LIST_PATH = ["Your Activity", "Like List", "ItemFavoriteList"]
COMMENT_LIST_PATH = ["Comment", "Comments", "CommentsList"]
SEARCH_LIST_PATH = ["Your Activity", "Searches", "SearchList"]
SHARE_HISTORY_PATH = ["Your Activity", "Share History", "ShareHistoryList"]
LOGIN_HISTORY_PATH = ["Your Activity", "Login History", "LoginHistoryList"]
FOLLOWER_LIST_PATH = ["Your Activity", "Follower", "FansList"]
FOLLOWING_LIST_PATH = ["Your Activity", "Following", "Following"]
FAVORITE_VIDEOS_PATH = ["Your Activity", "Favorite Videos", "FavoriteVideoList"]
FAVORITE_SOUNDS_PATH = ["Your Activity", "Favorite Sounds", "FavoriteSoundList"]
BLOCKED_USERS_PATH = ["App Settings", "Block List", "BlockList"]

ORDER_HISTORY_PATH = ["TikTok Shop", "Order History", "OrderHistories"]
PRODUCT_BROWSING_PATH = ["TikTok Shop", "Product Browsing History", "ProductBrowsingHistories"]
SHOPPING_CART_PATH = ["TikTok Shop", "Shopping Cart List", "ShoppingCart"]
SAVED_ADDRESSES_PATH = ["TikTok Shop", "Saved Address Information", "SavedAddress"]
SAVED_PAYMENT_CARDS_PATH = ["TikTok Shop", "Current Payment Information", "PayCard"]

WATCH_LIVE_HISTORY_PATH = ["Tiktok Live", "Watch Live History", "WatchLiveMap"]

OFF_TIKTOK_ACTIVITY_PATH = ["Ads and data", "Off TikTok Activity", "OffTikTokActivityDataList"]

DM_CHAT_HISTORY_PATH = ["Direct Message", "Direct Messages", "ChatHistory"]

ZIP_FILE_PATTERN = "./data/TikTok_Data_*.zip"

console = Console()

def safe_get_data(data_dict, path_list, default=None):
    """
    Truy cập và lấy dữ liệu từ một từ điển (dictionary) lồng nhau một cách an toàn.

    Hàm này duyệt qua một từ điển theo một danh sách các khóa (keys) được cung cấp.
    Nếu bất kỳ khóa nào trong đường dẫn không tồn tại, hàm sẽ trả về giá trị mặc định
    thay vì gây ra lỗi `KeyError`.

    Args:
        data_dict (dict): Từ điển nguồn chứa dữ liệu.
        path_list (list): Danh sách các khóa (string) để chỉ định đường dẫn đến
                          dữ liệu cần lấy.
        default (any, optional): Giá trị sẽ được trả về nếu đường dẫn không hợp lệ
                                 hoặc giá trị cuối cùng là None. Mặc định là None.

    Returns:
        any: Giá trị được tìm thấy tại đường dẫn chỉ định, hoặc giá trị `default`
             nếu không tìm thấy.
    """
    temp_dict = data_dict
    for key in path_list:
        if isinstance(temp_dict, dict) and key in temp_dict:
            temp_dict = temp_dict[key]
        else:
            return default
    return temp_dict if temp_dict is not None else default

def load_tiktok_data_from_zip(zip_file_path, internal_file_pattern="user_data*.json"):
    """
    Tải và phân tích cú pháp (parse) tệp JSON dữ liệu người dùng từ tệp ZIP của TikTok.

    Hàm này mở một tệp ZIP, tìm kiếm tệp JSON dữ liệu người dùng (ví dụ: user_data.json)
    bên trong, đọc nội dung và chuyển đổi nó thành một đối tượng từ điển Python.
    Nó cũng xử lý các lỗi phổ biến như không tìm thấy tệp, tệp ZIP hỏng,
    hoặc lỗi giải mã JSON.

    Args:
        zip_file_path (str): Đường dẫn đến tệp ZIP chứa dữ liệu TikTok.
        internal_file_pattern (str, optional): Mẫu tên tệp để tìm kiếm tệp JSON
            bên trong tệp ZIP. Mặc định là "user_data*.json".

    Returns:
        dict | None: Một từ điển chứa toàn bộ dữ liệu người dùng nếu tải thành công.
                      Trả về None nếu có lỗi xảy ra.
    """
    all_data = None
    try:
        with zipfile.ZipFile(zip_file_path, 'r') as zf:
            json_files_in_zip = [
                name for name in zf.namelist()
                if fnmatch.fnmatch(os.path.basename(name).lower(), internal_file_pattern.lower()) and name.endswith(".json")
            ]

            if not json_files_in_zip:
                console.print(f"| 🚫 Không tìm thấy tệp JSON nào khớp với mẫu '[yellow]{internal_file_pattern}[/yellow]' trong '[cyan]{zip_file_path}[/cyan]'.")
                console.print(f"| ℹ️ Các tệp có trong ZIP: {zf.namelist()}")
                return None
            
            preferred_files = [f for f in json_files_in_zip if os.path.basename(f).lower() in ('user_data_tiktok.json', 'user_data.json')]
            if preferred_files:
                 file_to_load = preferred_files[0]
            else:
                 file_to_load = json_files_in_zip[0] 

            console.print(f"| 📄 Tìm thấy và đang tải tệp: [cyan]{file_to_load}[/cyan]")
            try:
                with zf.open(file_to_load) as f:
                    file_content = f.read().decode('utf-8')
                    all_data = json.loads(file_content)
            except json.JSONDecodeError as e_json:
                console.print(f"| ❌ Lỗi giải mã JSON từ tệp '[yellow]{file_to_load}[/yellow]' trong ZIP: {e_json}", style="red")
            except Exception as e_file:
                console.print(f"| ❌ Lỗi khi đọc tệp '[yellow]{file_to_load}[/yellow]' từ ZIP: {e_file}", style="red")

    except FileNotFoundError:
        console.print(f"| 🚫 Không tìm thấy tệp ZIP: [cyan]{zip_file_path}[/cyan]", style="red")
        return None
    except zipfile.BadZipFile:
        console.print(f"| ❌ Tệp ZIP không hợp lệ hoặc bị hỏng: [cyan]{zip_file_path}[/cyan]", style="red")
        return None
    except Exception as e_zip:
        console.print(f"| ❌ Lỗi không xác định khi xử lý tệp ZIP: {e_zip}", style="red")
        return None

    if not all_data:
        console.print(f"| ⚠️ Không có dữ liệu nào được tải.", style="yellow")
    return all_data

def extract_tiktok_insights(data):
    """
    Trích xuất và tổng hợp các thông tin chính (insights) từ dữ liệu thô của TikTok.

    Hàm này lấy từ điển dữ liệu lớn và rút ra các chỉ số quan trọng như thông tin
    cá nhân, số liệu hoạt động, dữ liệu mua sắm, v.v. Nó trả về một từ điển
    mới, gọn gàng hơn, chứa các thông tin đã được tóm tắt.

    Args:
        data (dict): Từ điển chứa toàn bộ dữ liệu TikTok đã được tải từ tệp JSON.

    Returns:
        dict: Một từ điển chứa các thông tin và chỉ số chính đã được trích xuất.
              Trả về từ điển rỗng nếu dữ liệu đầu vào không hợp lệ.
    """
    if not data:
        return {}

    insights = {}

    insights["Username"] = safe_get_data(data, USER_NAME_PATH, "N/A")
    insights["Email"] = safe_get_data(data, EMAIL_PATH, "N/A")
    insights["Phone Number"] = safe_get_data(data, PHONE_PATH, "N/A")
    insights["Bio Description"] = safe_get_data(data, BIO_DESC_PATH, "N/A")
    insights["Birth Date"] = safe_get_data(data, BIRTH_DATE_PATH, "N/A")
    insights["Likes Received"] = safe_get_data(data, LIKES_RECEIVED_PATH, 0)

    insights["App Language"] = safe_get_data(data, APP_LANG_PATH, "N/A")
    insights["Private Account"] = safe_get_data(data, PRIVATE_ACCOUNT_PATH, "N/A")
    insights["Personalized Ads"] = safe_get_data(data, PERSONALIZED_ADS_PATH, "N/A")
    fyp_filters = safe_get_data(data, FYP_KEYWORD_FILTERS_PATH, [])
    insights["FYP Keyword Filters Count"] = len(fyp_filters) if isinstance(fyp_filters, list) else 0

    insights["Videos Watched Count"] = len(safe_get_data(data, WATCH_HISTORY_PATH, []))
    insights["Liked Videos Count"] = len(safe_get_data(data, LIKE_LIST_PATH, []))
    insights["Comments Made Count"] = len(safe_get_data(data, COMMENT_LIST_PATH, []))
    insights["Searches Made Count"] = len(safe_get_data(data, SEARCH_LIST_PATH, []))
    insights["Shares Made Count"] = len(safe_get_data(data, SHARE_HISTORY_PATH, []))
    insights["Login Sessions Count"] = len(safe_get_data(data, LOGIN_HISTORY_PATH, []))
    insights["Followers Count"] = len(safe_get_data(data, FOLLOWER_LIST_PATH, []))
    insights["Following Count"] = len(safe_get_data(data, FOLLOWING_LIST_PATH, []))
    insights["Favorite Videos Count"] = len(safe_get_data(data, FAVORITE_VIDEOS_PATH, []))
    insights["Favorite Sounds Count"] = len(safe_get_data(data, FAVORITE_SOUNDS_PATH, []))
    insights["Blocked Users Count"] = len(safe_get_data(data, BLOCKED_USERS_PATH, []))

    orders = safe_get_data(data, ORDER_HISTORY_PATH, {})
    insights["Shop Orders Count"] = len(orders) if isinstance(orders, dict) else 0
    insights["Shop Product Browsing Count"] = len(safe_get_data(data, PRODUCT_BROWSING_PATH, []))
    insights["Shop Shopping Cart Items Count"] = len(safe_get_data(data, SHOPPING_CART_PATH, []))
    insights["Shop Saved Addresses Count"] = len(safe_get_data(data, SAVED_ADDRESSES_PATH, []))
    insights["Shop Saved Payment Cards Count"] = len(safe_get_data(data, SAVED_PAYMENT_CARDS_PATH, []))

    live_history = safe_get_data(data, WATCH_LIVE_HISTORY_PATH, {})
    insights["Live Sessions Watched Count"] = len(live_history) if isinstance(live_history, dict) else 0
    
    total_live_comments = 0
    if isinstance(live_history, dict):
        for session_id, session_data in live_history.items():
            comments = safe_get_data(session_data, ["Comments"], [])
            valid_comments = [c for c in comments if c.get("CommentContent", "") != "" or c.get("RawTime", -1) != -1]
            total_live_comments += len(valid_comments)
    insights["Live Comments Made Count"] = total_live_comments

    insights["Off-TikTok Activity Events Count"] = len(safe_get_data(data, OFF_TIKTOK_ACTIVITY_PATH, []))
    
    chat_history = safe_get_data(data, DM_CHAT_HISTORY_PATH, {})
    insights["DM Chats Count"] = len(chat_history) if isinstance(chat_history, dict) else 0
    total_dm_messages = 0
    if isinstance(chat_history, dict):
        for chat_name, messages in chat_history.items():
            total_dm_messages += len(messages) if isinstance(messages, list) else 0
    insights["DM Total Messages Count"] = total_dm_messages

    return insights

def display_insights_rich(insights, username="User"):
    """
    Hiển thị các thông tin chi tiết đã tổng hợp ra console một cách đẹp mắt.

    Sử dụng thư viện `rich`, hàm này tạo ra các bảng (table) có định dạng rõ ràng
    để trình bày dữ liệu từ điển `insights` một cách trực quan và dễ đọc trên terminal.

    Args:
        insights (dict): Từ điển chứa các thông tin đã được trích xuất và tổng hợp
                         bởi hàm `extract_tiktok_insights`.
        username (str, optional): Tên người dùng để hiển thị. Mặc định là "User".
    """
    if not insights:
        console.print(f"| ⚠️ Không có thông tin chi tiết để hiển thị.[/red]")
        return

    table_width = 89
    col1_width = 35
    col2_width = 50 

    profile_table = Table(
        title="| 👤 Thông Tin Cá Nhân",
        show_header=False,
        show_lines=False,
        title_justify="left",
        title_style="none",
        width=table_width
    )
    profile_table.add_column("Mục", style="dim", width=col1_width, justify="left")
    profile_table.add_column("Giá trị", width=col2_width, justify="right")
    profile_table.add_row("Tên người dùng", insights.get("Username", "N/A"))
    profile_table.add_row("Email", insights.get("Email", "N/A"))
    profile_table.add_row("Số điện thoại", insights.get("Phone Number", "N/A"))
    bio = insights.get("Bio Description", "N/A")
    profile_table.add_row("Tiểu sử", bio if bio else "N/A") 
    profile_table.add_row("Ngày sinh", insights.get("Birth Date", "N/A"))
    profile_table.add_row("Lượt thích đã nhận", str(insights.get("Likes Received", 0)))
    console.print(profile_table)

    activity_table = Table(
        title="| 🚀 Hoạt Động Chính",
        show_header=False,
        show_lines=False,
        title_justify="left",
        title_style="none",
        width=table_width
    )
    activity_table.add_column("Mục", style="dim", width=col1_width, justify="left")
    activity_table.add_column("Số lượng", width=col2_width, justify="right")
    activity_table.add_row("Video đã xem", str(insights.get("Videos Watched Count", 0)))
    activity_table.add_row("Video đã thích", str(insights.get("Liked Videos Count", 0)))
    activity_table.add_row("Bình luận đã đăng", str(insights.get("Comments Made Count", 0)))
    activity_table.add_row("Lượt tìm kiếm", str(insights.get("Searches Made Count", 0)))
    activity_table.add_row("Lượt chia sẻ", str(insights.get("Shares Made Count", 0)))
    activity_table.add_row("Phiên đăng nhập", str(insights.get("Login Sessions Count", 0)))
    activity_table.add_row("Người theo dõi", str(insights.get("Followers Count", 0)))
    activity_table.add_row("Đang theo dõi", str(insights.get("Following Count", 0)))
    activity_table.add_row("Video yêu thích", str(insights.get("Favorite Videos Count", 0)))
    activity_table.add_row("Âm thanh yêu thích", str(insights.get("Favorite Sounds Count", 0)))
    activity_table.add_row("Người dùng bị chặn", str(insights.get("Blocked Users Count", 0)))
    console.print(activity_table)
    
    dm_table = Table(
        title="| 💬 Tin Nhắn Trực Tiếp (DM)",
        show_header=False,
        show_lines=False,
        title_justify="left",
        title_style="none",
        width=table_width
    )
    dm_table.add_column("Mục", style="dim", width=col1_width, justify="left")
    dm_table.add_column("Số lượng", width=col2_width, justify="right")
    dm_table.add_row("Số cuộc trò chuyện", str(insights.get("DM Chats Count",0)))
    dm_table.add_row("Tổng số tin nhắn", str(insights.get("DM Total Messages Count",0)))
    console.print(dm_table)

    shop_table = Table(
        title="| 🛍️  TikTok Shop",
        show_header=False,
        show_lines=False,
        title_justify="left",
        title_style="none",
        width=table_width
    )
    shop_table.add_column("Mục", style="dim", width=col1_width, justify="left")
    shop_table.add_column("Số lượng", width=col2_width, justify="right")
    shop_table.add_row("Đơn hàng đã đặt", str(insights.get("Shop Orders Count", 0)))
    shop_table.add_row("Lượt xem sản phẩm", str(insights.get("Shop Product Browsing Count", 0)))
    shop_table.add_row("Sản phẩm trong giỏ hàng", str(insights.get("Shop Shopping Cart Items Count", 0)))
    shop_table.add_row("Địa chỉ đã lưu", str(insights.get("Shop Saved Addresses Count", 0)))
    shop_table.add_row("Thẻ thanh toán đã lưu", str(insights.get("Shop Saved Payment Cards Count", 0)))
    console.print(shop_table)

    live_table = Table(
        title="| 🔴 TikTok Live",
        show_header=False,
        show_lines=False,
        title_justify="left",
        title_style="none",
        width=table_width
    )
    live_table.add_column("Mục", style="dim", width=col1_width, justify="left")
    live_table.add_column("Số lượng", width=col2_width, justify="right")
    live_table.add_row("Live đã xem", str(insights.get("Live Sessions Watched Count", 0)))
    live_table.add_row("Bình luận trong Live", str(insights.get("Live Comments Made Count", 0)))
    console.print(live_table)

    other_table = Table(
        title="| ⚙️  Cài Đặt & Dữ Liệu Khác",
        show_header=False,
        show_lines=False,
        title_style="none",
        title_justify="left",
        width=table_width
    )
    other_table.add_column("Mục", style="dim", width=col1_width, justify="left")
    other_table.add_column("Giá trị / Số lượng", width=col2_width, justify="right") 
    other_table.add_row("Ngôn ngữ ứng dụng", insights.get("App Language", "N/A"))
    other_table.add_row("Tài khoản riêng tư", str(insights.get("Private Account", "N/A"))) 
    other_table.add_row("Quảng cáo cá nhân hoá", str(insights.get("Personalized Ads", "N/A"))) 
    other_table.add_row("Bộ lọc từ khoá FYP", str(insights.get("FYP Keyword Filters Count",0)))
    other_table.add_row("Sự kiện hoạt động Off-TikTok", str(insights.get("Off-TikTok Activity Events Count", 0)))
    console.print(other_table)


def export_detailed_data_to_excel(data, excel_writer):
    """
    Xuất các danh sách dữ liệu chi tiết vào các sheet riêng biệt của một tệp Excel.

    Hàm này duyệt qua dữ liệu thô, chuyển đổi các phần dữ liệu như lịch sử xem,
    danh sách thích, bình luận, v.v., thành các DataFrame của Pandas và ghi chúng
    vào đối tượng `ExcelWriter` được cung cấp.

    Args:
        data (dict): Từ điển chứa toàn bộ dữ liệu thô của TikTok.
        excel_writer (pd.ExcelWriter): Đối tượng `ExcelWriter` của Pandas để
                                       ghi dữ liệu vào tệp Excel.

    Returns:
        Ghi dữ liệu vào `excel_writer`.
    """
    if not data:
        return

    watch_history = safe_get_data(data, WATCH_HISTORY_PATH, [])
    if watch_history:
        pd.DataFrame(watch_history).to_excel(excel_writer, sheet_name="Watch History", index=False)

    like_list = safe_get_data(data, LIKE_LIST_PATH, [])
    if like_list:
        pd.DataFrame(like_list).to_excel(excel_writer, sheet_name="Liked Videos", index=False)

    comments = safe_get_data(data, COMMENT_LIST_PATH, [])
    if comments:
        pd.DataFrame(comments).to_excel(excel_writer, sheet_name="Comments Made", index=False)

    searches = safe_get_data(data, SEARCH_LIST_PATH, [])
    if searches:
        pd.DataFrame(searches).to_excel(excel_writer, sheet_name="Search History", index=False)

    logins = safe_get_data(data, LOGIN_HISTORY_PATH, [])
    if logins:
        pd.DataFrame(logins).to_excel(excel_writer, sheet_name="Login History", index=False)
        
    blocked = safe_get_data(data, BLOCKED_USERS_PATH, [])
    if blocked:
        pd.DataFrame(blocked).to_excel(excel_writer, sheet_name="Blocked Users", index=False)

    followers = safe_get_data(data, FOLLOWER_LIST_PATH, [])
    if followers:
        pd.DataFrame(followers).to_excel(excel_writer, sheet_name="Followers", index=False)

    following = safe_get_data(data, FOLLOWING_LIST_PATH, [])
    if following:
        pd.DataFrame(following).to_excel(excel_writer, sheet_name="Following", index=False)
        
    fav_videos = safe_get_data(data, FAVORITE_VIDEOS_PATH, [])
    if fav_videos:
        pd.DataFrame(fav_videos).to_excel(excel_writer, sheet_name="Favorite Videos", index=False)

    fav_sounds = safe_get_data(data, FAVORITE_SOUNDS_PATH, [])
    if fav_sounds:
        pd.DataFrame(fav_sounds).to_excel(excel_writer, sheet_name="Favorite Sounds", index=False)
        
    orders_dict = safe_get_data(data, ORDER_HISTORY_PATH, {})
    if orders_dict:
        orders_list = []
        for order_id, order_details in orders_dict.items():
            order_details['order_id_from_key'] = order_id 
            orders_list.append(order_details)
        if orders_list:
            pd.DataFrame(orders_list).to_excel(excel_writer, sheet_name="Shop Order History", index=False)
            
    browsing = safe_get_data(data, PRODUCT_BROWSING_PATH, [])
    if browsing:
        pd.DataFrame(browsing).to_excel(excel_writer, sheet_name="Shop Product Browsing", index=False)

    cart = safe_get_data(data, SHOPPING_CART_PATH, [])
    if cart:
        pd.DataFrame(cart).to_excel(excel_writer, sheet_name="Shop Shopping Cart", index=False)

    live_map = safe_get_data(data, WATCH_LIVE_HISTORY_PATH, {})
    live_list_for_excel = []
    if live_map:
        for live_id, details in live_map.items():
            entry = {"LiveSessionID": live_id, "WatchTime": details.get("WatchTime")}
            comments = details.get("Comments", [])
            valid_comments = [c.get("CommentContent") for c in comments if c.get("CommentContent", "") != "" or c.get("RawTime", -1) != -1]
            entry["CommentsInLive"] = "; ".join(valid_comments) if valid_comments else ""
            live_list_for_excel.append(entry)
        if live_list_for_excel:
            pd.DataFrame(live_list_for_excel).to_excel(excel_writer, sheet_name="Watch Live History", index=False)

    dm_history = safe_get_data(data, DM_CHAT_HISTORY_PATH, {})
    all_dms_flat = []
    if dm_history:
        for chat_with, messages in dm_history.items():
            chat_name = chat_with.replace("Chat History with ", "").replace(":", "")
            if isinstance(messages, list):
                for msg in messages:
                    msg_copy = msg.copy()
                    msg_copy["ChatWith"] = chat_name
                    all_dms_flat.append(msg_copy)
        if all_dms_flat:
             pd.DataFrame(all_dms_flat).to_excel(excel_writer, sheet_name="Direct Messages", index=False)


def export_insights_to_excel(insights, full_data, excel_path="tiktok_analysis_summary.xlsx"):
    """
    Điều phối việc xuất cả dữ liệu tổng quan và chi tiết ra một tệp Excel duy nhất.

    Hàm này tạo một tệp Excel, ghi dữ liệu tổng hợp `insights` vào một sheet
    tên là "Tổng Quan", sau đó gọi hàm `export_detailed_data_to_excel` để ghi
    tất cả các dữ liệu chi tiết khác vào các sheet còn lại.

    Args:
        insights (dict): Từ điển chứa dữ liệu tổng quan.
        full_data (dict): Từ điển chứa toàn bộ dữ liệu thô của TikTok.
        excel_path (str, optional): Tên và đường dẫn của tệp Excel đầu ra.
                                    Mặc định là "tiktok_analysis_summary.xlsx".

    Returns:
        Tạo ra một tệp Excel.
    """
    console.print(f"\n📚 [bold green]Xuất ra EXCEL[/bold green]")
    if not insights:
        console.print(f"| ⚠️ Không có thông tin chi tiết để xuất ra Excel.[/red]")
        return

    try:
        with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
            summary_df = pd.DataFrame(list(insights.items()), columns=['Mục Phân Tích', 'Giá trị'])
            summary_df.to_excel(writer, sheet_name="Tổng Quan", index=False)
            
            export_detailed_data_to_excel(full_data, writer)

        console.print(f"| 💾 Đã xuất kết quả phân tích chi tiết ra: [cyan]{excel_path}[/cyan]")
    except Exception as e:
        console.print(f"| ❌ Lỗi khi xuất ra Excel: {e}[/red]")

def main():
    """
    Hàm chính điều khiển toàn bộ quy trình của chương trình.

    Đây là điểm bắt đầu thực thi. Hàm này sẽ:
    1. Tìm kiếm tệp dữ liệu ZIP của TikTok trong thư mục chỉ định.
    2. Gọi hàm để tải và phân tích dữ liệu từ tệp ZIP.
    3. Nếu tải thành công, gọi hàm để trích xuất các thông tin chi tiết.
    4. Hiển thị kết quả tổng quan ra console.
    5. Xuất báo cáo đầy đủ (tổng quan và chi tiết) ra tệp Excel.
    """
    ZIP_FILE_PATH = None
    zip_file_pattern = ZIP_FILE_PATTERN

    zip_files_found = glob.glob(zip_file_pattern)
    console.print(f"🚀[bold green] Bắt đầu phân tích dữ liệu TIKTOK[bold green]")         
    if not zip_files_found:
        console.print(f"| 🚫 Không tìm thấy tệp ZIP nào khớp với mẫu '[yellow]{zip_file_pattern}[/yellow]' trong thư mục hiện tại.")
        console.print(f"| ℹ️  Hãy đảm bảo bạn đã đặt tên tệp ZIP chính xác (ví dụ: TikTok_Data_12345.zip) và đặt nó vào folder data.")
    else:
        if len(zip_files_found) > 1:
            console.print(f"| ℹ️ Tìm thấy nhiều tệp ZIP khớp mẫu. Sử dụng tệp đầu tiên: [cyan]{zip_files_found[0]}[/cyan]")
        ZIP_FILE_PATH = zip_files_found[0]
        console.print(f"| 📄 Sử dụng tệp dữ liệu: [cyan]{ZIP_FILE_PATH}[/cyan]")

    if ZIP_FILE_PATH and os.path.exists(ZIP_FILE_PATH):
        tiktok_data = load_tiktok_data_from_zip(ZIP_FILE_PATH, internal_file_pattern="user_data*.json")

        if tiktok_data:
            insights = extract_tiktok_insights(tiktok_data)
            username = insights.get("Username", "Người dùng")
            
            console.print(f"\n📊[bold green] Kết quả phân tích[/bold green]")
            display_insights_rich(insights, username=username)
            
            export_insights_to_excel(insights, tiktok_data, excel_path="tiktok_analysis_report.xlsx")
        else:
            console.print(f"| ⚠️ Không thể tải hoặc xử lý dữ liệu TikTok từ tệp đã chọn.")
    elif ZIP_FILE_PATH and not os.path.exists(ZIP_FILE_PATH):
        console.print(f"| ❌ Tệp ZIP '{ZIP_FILE_PATH}' được tìm thấy bởi glob nhưng không thể truy cập. Kiểm tra lại đường dẫn và quyền.")
    else:
        pass

if __name__ == "__main__":
    main()