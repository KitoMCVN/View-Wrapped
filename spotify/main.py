import json
import pandas as pd
from collections import Counter
from datetime import datetime, timedelta
import os
import zipfile
import fnmatch
from rich.console import Console
from rich.table import Table

TIMESTAMP_COL = 'ts'
TRACK_NAME_COL = 'master_metadata_track_name'
ARTIST_NAME_COL = 'master_metadata_album_artist_name'
MS_PLAYED_COL = 'ms_played'
EPISODE_NAME_COL = 'episode_name'
MIN_MS_PLAYED_FOR_COUNT = 15000

ZIP_FILE_PATH = "./data/my_spotify_data.zip"
INTERNAL_JSON_PATTERN = "Streaming_History_*.json" # Handles both StreamingHistoryX.json and Streaming_History_Audio_*.json

console = Console()

def load_streaming_data_from_zip(zip_file_path, internal_file_pattern="Streaming_History_*.json"):
    """Tải dữ liệu lịch sử nghe nhạc từ tệp ZIP của Spotify.

    Hàm này đọc tệp ZIP được cung cấp, tìm các tệp JSON khớp với `internal_file_pattern`
    (mặc định là "Streaming_History_*.json"), và gộp nội dung của chúng vào một DataFrame.
    Xử lý các trường hợp lỗi như không tìm thấy tệp ZIP, tệp ZIP không hợp lệ,
    hoặc lỗi giải mã JSON.

    Args:
        zip_file_path (str): Đường dẫn đến tệp ZIP chứa dữ liệu Spotify.
        internal_file_pattern (str, optional): Mẫu để khớp tên tệp JSON bên trong ZIP.
                                            Mặc định là "Streaming_History_*.json".

    Returns:
        pd.DataFrame: DataFrame chứa tất cả các mục từ các tệp JSON,
                      hoặc DataFrame rỗng nếu không có dữ liệu hoặc xảy ra lỗi.
    """
    all_entries = []
    console.print(f"| 🔄 Đang cố gắng đọc từ file ZIP: [cyan]{zip_file_path}[/cyan]")
    try:
        with zipfile.ZipFile(zip_file_path, 'r') as zf:
            json_files_in_zip = [
                name for name in zf.namelist()
                if fnmatch.fnmatch(os.path.basename(name), internal_file_pattern) and name.endswith(".json")
            ]

            if not json_files_in_zip:
                console.print(f"| 🚫 Không tìm thấy tệp JSON nào khớp với mẫu '[yellow]{internal_file_pattern}[/yellow]' trong '[cyan]{zip_file_path}[/cyan]'.")
                console.print(f"|    Các tệp có trong ZIP: {zf.namelist()}")
                return pd.DataFrame()

            for file_name_in_zip in json_files_in_zip:
                try:
                    with zf.open(file_name_in_zip) as f:
                        file_content = f.read().decode('utf-8')
                        data = json.loads(file_content)
                        all_entries.extend(data)
                except json.JSONDecodeError as e_json:
                    console.print(f"| ❌ Lỗi giải mã JSON từ tệp '[yellow]{file_name_in_zip}[/yellow]' trong ZIP: {e_json}", style="red")
                except Exception as e_file:
                    console.print(f"| ❌ Lỗi khi đọc tệp '[yellow]{file_name_in_zip}[/yellow]' từ ZIP: {e_file}", style="red")
    except FileNotFoundError:
        console.print(f"| 🚫 Không tìm thấy tệp ZIP: [cyan]{zip_file_path}[/cyan]", style="red")
        return pd.DataFrame()
    except zipfile.BadZipFile:
        console.print(f"| 🚫 Tệp ZIP không hợp lệ hoặc bị hỏng: [cyan]{zip_file_path}[/cyan]", style="red")
        return pd.DataFrame()
    except Exception as e_zip:
        console.print(f"| ❌ Lỗi không xác định khi xử lý tệp ZIP: {e_zip}", style="red")
        return pd.DataFrame()

    if not all_entries:
        console.print("| 🤷 Không có dữ liệu nào được tải từ các tệp JSON trong ZIP.", style="yellow")
        return pd.DataFrame()

    df = pd.DataFrame(all_entries)
    return df

def preprocess_data(df):
    """Tiền xử lý DataFrame dữ liệu thô từ Spotify.

    Hàm này thực hiện các bước sau:
    1. Chuẩn hóa tên cột (ví dụ: 'endTime' -> 'ts', 'trackName' -> 'master_metadata_track_name').
    2. Kiểm tra sự tồn tại của các cột cần thiết cho phân tích bài hát.
    3. Xử lý trường hợp thiếu tên nghệ sĩ (`ARTIST_NAME_COL`) bằng cách điền 'Không xác định'.
    4. Chuyển đổi kiểu dữ liệu cho cột thời gian (`TIMESTAMP_COL`) sang datetime và
       thời gian nghe (`MS_PLAYED_COL`) sang numeric.
    5. Loại bỏ khoảng trắng thừa ở đầu và cuối tên bài hát và tên nghệ sĩ.
    6. Loại bỏ các dòng có giá trị rỗng (NaN) ở các cột quan trọng (`TIMESTAMP_COL`, `TRACK_NAME_COL`, `MS_PLAYED_COL`).
    7. Lọc các bản nhạc được nghe dưới `MIN_MS_PLAYED_FOR_COUNT` (mặc định 15 giây).
    8. Tạo cột 'year' từ timestamp và 'fullSongName' (Tên bài hát - Tên nghệ sĩ).

    Args:
        df (pd.DataFrame): DataFrame chứa dữ liệu thô được tải từ các tệp JSON.

    Returns:
        pd.DataFrame: DataFrame đã được tiền xử lý, chỉ chứa dữ liệu bài hát hợp lệ,
                      hoặc DataFrame rỗng nếu không có dữ liệu hợp lệ sau khi xử lý.
    """
    if df.empty: return pd.DataFrame()
    initial_count = len(df)

    column_mapping = {
        'ts': TIMESTAMP_COL, 'endTime': TIMESTAMP_COL,
        'master_metadata_track_name': TRACK_NAME_COL, 'trackName': TRACK_NAME_COL, 'track_name': TRACK_NAME_COL,
        'master_metadata_album_artist_name': ARTIST_NAME_COL, 'artistName': ARTIST_NAME_COL, 'artist_name': ARTIST_NAME_COL,
        'ms_played': MS_PLAYED_COL, 'msPlayed': MS_PLAYED_COL,
        'episode_name': EPISODE_NAME_COL
    }

    df.rename(columns=lambda c: column_mapping.get(c, c), inplace=True)

    required_cols_for_songs = [TIMESTAMP_COL, TRACK_NAME_COL, ARTIST_NAME_COL, MS_PLAYED_COL]

    if not all(col in df.columns for col in required_cols_for_songs):
        missing = [col for col in required_cols_for_songs if col not in df.columns]
        console.print(f"| 🚫 LỖI: Thiếu cột bài hát bắt buộc: {', '.join(missing)}.", style="red")
        console.print(f"|    Các cột có trong dữ liệu: {list(df.columns)}")
        if TRACK_NAME_COL not in df.columns and EPISODE_NAME_COL in df.columns:
            console.print("| ⚠️ Chỉ có dữ liệu podcast (episode_name). Sẽ bỏ qua phân tích bài hát.", style="yellow")
        elif ARTIST_NAME_COL not in missing or len(missing) == 1 and ARTIST_NAME_COL in missing:
             pass
        else:
            return pd.DataFrame()

    df_songs = df[df[TRACK_NAME_COL].notna()].copy()

    if df_songs.empty:
        console.print("| 🤷 Không có bài hát nào (dữ liệu track_name bị thiếu hoặc rỗng) để phân tích.", style="yellow")
        return pd.DataFrame()

    if ARTIST_NAME_COL not in df_songs.columns:
        console.print(f"| ⚠️ Cột '{ARTIST_NAME_COL}' không tìm thấy. Sẽ sử dụng 'Không xác định' cho tên nghệ sĩ.", style="yellow")
        df_songs[ARTIST_NAME_COL] = "Không xác định"


    if not all(col in df_songs.columns for col in required_cols_for_songs):
        missing_after_fill = [col for col in required_cols_for_songs if col not in df_songs.columns]
        console.print(f"| 🚫 LỖI: Thiếu cột bài hát thiết yếu sau khi xử lý: {', '.join(missing_after_fill)}.", style="red")
        return pd.DataFrame()


    df_songs[MS_PLAYED_COL] = pd.to_numeric(df_songs[MS_PLAYED_COL], errors='coerce')
    df_songs[TIMESTAMP_COL] = pd.to_datetime(df_songs[TIMESTAMP_COL], errors='coerce')

    if TRACK_NAME_COL in df_songs.columns and df_songs[TRACK_NAME_COL].dtype == 'object':
        df_songs[TRACK_NAME_COL] = df_songs[TRACK_NAME_COL].str.strip()
    if ARTIST_NAME_COL in df_songs.columns and df_songs[ARTIST_NAME_COL].dtype == 'object':
        df_songs[ARTIST_NAME_COL] = df_songs[ARTIST_NAME_COL].str.strip()

    df_songs.dropna(subset=[TIMESTAMP_COL, TRACK_NAME_COL, MS_PLAYED_COL], inplace=True)
    if df_songs.empty:
        console.print("| 🗑️ Không có dữ liệu hợp lệ sau khi loại bỏ NaN từ các cột bài hát thiết yếu.", style="yellow")
        return pd.DataFrame()

    df_songs = df_songs[df_songs[MS_PLAYED_COL] >= MIN_MS_PLAYED_FOR_COUNT].copy()
    if df_songs.empty:
        console.print(f"| 🎧 Không có bài hát nào được nghe quá {MIN_MS_PLAYED_FOR_COUNT/1000} giây.", style="yellow")
        return pd.DataFrame()

    df_songs['year'] = df_songs[TIMESTAMP_COL].dt.year
    df_songs['fullSongName'] = df_songs[TRACK_NAME_COL] + " - " + df_songs[ARTIST_NAME_COL].fillna("Không xác định")

    processed_count = len(df_songs)
    if initial_count > 0 and processed_count > 0:
         console.print(f"| ✨ Tiền xử lý: {initial_count} mục ban đầu -> {processed_count} mục bài hát hợp lệ sau khi lọc.")
    elif initial_count > 0 and processed_count == 0:
         console.print("| ✨ Tiền xử lý: không có mục bài hát hợp lệ nào sau khi lọc.", style="yellow")
    return df_songs

def format_ms_to_detailed_play_time_string(ms):
    """Chuyển đổi thời gian từ miligiây sang chuỗi định dạng chi tiết (phút và giờ).

    Ví dụ: 120000 ms sẽ được chuyển thành "120 phút (2.0 giờ)".
    Trả về "0 phút (0.0 giờ)" nếu đầu vào là NaN hoặc âm.

    Args:
        ms (int | float): Thời gian nghe tính bằng miligiây.

    Returns:
        str: Chuỗi biểu diễn thời gian đã định dạng (ví dụ: "1,234 phút (20.6 giờ)").
    """
    if pd.isna(ms) or ms < 0: return "0 phút (0.0 giờ)"
    total_seconds = int(ms / 1000)
    total_minutes = total_seconds // 60
    total_hours = total_minutes / 60
    return f"{total_minutes:,} phút ({total_hours:.1f} giờ)"

def calculate_top_songs_data(df, top_n=10):
    """Tính toán top N bài hát dựa trên số lần nghe và tổng thời gian nghe.

    Hàm này nhóm dữ liệu theo 'fullSongName' (Tên bài hát - Tên nghệ sĩ),
    đếm số lần nghe và tính tổng thời gian nghe (ms) cho mỗi bài.
    Nếu 'fullSongName' không tồn tại, nó sẽ được tạo từ `TRACK_NAME_COL` và `ARTIST_NAME_COL`.
    Sau đó, sắp xếp để lấy ra `top_n` bài hát hàng đầu.

    Args:
        df (pd.DataFrame): DataFrame chứa dữ liệu bài hát đã được tiền xử lý.
                           Cần có cột `MS_PLAYED_COL` và các cột để tạo 'fullSongName'
                           (`TRACK_NAME_COL`, `ARTIST_NAME_COL`).
        top_n (int, optional): Số lượng bài hát hàng đầu muốn lấy. Mặc định là 10.

    Returns:
        pd.DataFrame: DataFrame chứa top N bài hát, với index là 'fullSongName'
                      và các cột 'Số lần nghe', 'Tổng thời gian nghe (ms)'.
                      Trả về DataFrame rỗng nếu không có dữ liệu hoặc thiếu cột cần thiết.
    """
    if df.empty: return pd.DataFrame()
    df_copy = df.copy()

    if 'fullSongName' not in df_copy.columns:
        if TRACK_NAME_COL in df_copy.columns and ARTIST_NAME_COL in df_copy.columns:
            df_copy['fullSongName'] = df_copy[TRACK_NAME_COL] + " - " + df_copy[ARTIST_NAME_COL].fillna("Không xác định")
        elif TRACK_NAME_COL in df_copy.columns:
            console.print("| ⚠️ Thiếu 'ARTIST_NAME_COL', 'fullSongName' sẽ chỉ dựa trên 'TRACK_NAME_COL'.", style="yellow")
            df_copy['fullSongName'] = df_copy[TRACK_NAME_COL]
        else:
            console.print("| 🚫 Không thể tạo 'fullSongName' do thiếu 'TRACK_NAME_COL'.", style="red")
            return pd.DataFrame()

    counts = df_copy['fullSongName'].value_counts()
    times = df_copy.groupby('fullSongName')[MS_PLAYED_COL].sum()

    top_df = pd.DataFrame({'Số lần nghe': counts, 'Tổng thời gian nghe (ms)': times})
    return top_df.sort_values(by=['Số lần nghe', 'Tổng thời gian nghe (ms)'], ascending=[False, False]).head(top_n)

def calculate_top_artists_data(df, top_n=10):
    """Tính toán top N nghệ sĩ dựa trên số lần nghe và tổng thời gian nghe các bài hát của họ.

    Hàm này nhóm dữ liệu theo `ARTIST_NAME_COL`, đếm tổng số lần các bài hát
    của nghệ sĩ đó được nghe và tính tổng thời gian nghe (ms) cho mỗi nghệ sĩ.
    Sau đó, sắp xếp để lấy ra `top_n` nghệ sĩ hàng đầu.
    Yêu cầu cột `ARTIST_NAME_COL` phải tồn tại và không rỗng trong dữ liệu.

    Args:
        df (pd.DataFrame): DataFrame chứa dữ liệu bài hát đã được tiền xử lý.
                           Cần có cột `ARTIST_NAME_COL` và `MS_PLAYED_COL`.
        top_n (int, optional): Số lượng nghệ sĩ hàng đầu muốn lấy. Mặc định là 10.

    Returns:
        pd.DataFrame: DataFrame chứa top N nghệ sĩ, với index là `ARTIST_NAME_COL`
                      và các cột 'Số lần nghe', 'Tổng thời gian nghe (ms)'.
                      Trả về DataFrame rỗng nếu không có dữ liệu, thiếu cột `ARTIST_NAME_COL`,
                      hoặc tất cả tên nghệ sĩ đều rỗng.
    """
    if df.empty or ARTIST_NAME_COL not in df.columns:
        console.print("| 🤷 Không có dữ liệu nghệ sĩ hoặc thiếu cột 'ARTIST_NAME_COL'.", style="yellow")
        return pd.DataFrame()

    df_artists = df[df[ARTIST_NAME_COL].notna()].copy()
    if df_artists.empty:
        console.print("| 🤷 Không có dữ liệu nghệ sĩ hợp lệ (tên nghệ sĩ bị thiếu).", style="yellow")
        return pd.DataFrame()

    artist_play_counts = df_artists[ARTIST_NAME_COL].value_counts()
    artist_total_time_ms = df_artists.groupby(ARTIST_NAME_COL)[MS_PLAYED_COL].sum()

    top_artists_df = pd.DataFrame({
        'Số lần nghe': artist_play_counts,
        'Tổng thời gian nghe (ms)': artist_total_time_ms
    })

    return top_artists_df.sort_values(
        by=['Số lần nghe', 'Tổng thời gian nghe (ms)'],
        ascending=[False, False]
    ).head(top_n)

def display_analysis_results(df_period_songs, period_name_display, top_n=10):
    """Hiển thị kết quả phân tích cho một giai đoạn nhất định lên console.

    Bao gồm thống kê chung (số bài hát/nghệ sĩ duy nhất, tổng thời gian nghe)
    và bảng top N bài hát, top N nghệ sĩ cho giai đoạn được cung cấp.
    Sử dụng thư viện `rich.Table` để định dạng bảng cho đẹp mắt.

    Args:
        df_period_songs (pd.DataFrame): DataFrame chứa dữ liệu bài hát của giai đoạn cần phân tích.
        period_name_display (str): Tên hiển thị cho giai đoạn (ví dụ: "Tất Cả Thời Gian", "NĂM 2023").
        top_n (int, optional): Số lượng mục (bài hát/nghệ sĩ) hàng đầu để hiển thị trong các bảng.
                               Mặc định là 10.
    """
    console.print(f"\n📊 [bold green]{period_name_display}[/bold green]")

    if df_period_songs.empty:
        console.print("|   🤷 Không có dữ liệu để phân tích cho giai đoạn này.", style="yellow")
        return

    num_unique_songs = df_period_songs['fullSongName'].nunique()

    num_unique_artists = 0
    if ARTIST_NAME_COL in df_period_songs.columns and not df_period_songs[df_period_songs[ARTIST_NAME_COL].notna()][ARTIST_NAME_COL].empty:
        num_unique_artists = df_period_songs[df_period_songs[ARTIST_NAME_COL].notna()][ARTIST_NAME_COL].nunique()

    total_ms_played = df_period_songs[MS_PLAYED_COL].sum()
    total_time_str = format_ms_to_detailed_play_time_string(total_ms_played)

    console.print(f"| 👀 [bold]Thống kê: {num_unique_songs} bài hát - {num_unique_artists} nghệ sĩ - {total_time_str}[/bold]")

    console.print(f"| 🔊 [bold]Bảng Top {top_n} Bài Hát và Nghệ Sĩ Nghe Nhiều Nhất:[/bold]")

    top_songs = calculate_top_songs_data(df_period_songs, top_n=top_n)
    if not top_songs.empty:
        songs_table = Table(show_lines=False, header_style="blue")
        songs_table.add_column("STT", style="dim", width=3, justify="right")
        songs_table.add_column("Tên bài hát", width=30)
        songs_table.add_column("Nghệ sĩ", width=20)
        songs_table.add_column("Số lần nghe", width=11, justify="right")
        songs_table.add_column("Thời gian nghe" , width=25, justify="right")

        for i, (full_song_name, row) in enumerate(top_songs.iterrows()):
            song_name_part = "Không rõ"
            artist_name_part = "Không rõ"

            if isinstance(full_song_name, str):
                parts = full_song_name.rsplit(" - ", 1)
                song_name_part = parts[0]
                if len(parts) > 1:
                    artist_name_part = parts[1]
                else:
                    artist_name_part = "Không xác định"

            if len(song_name_part) > 30:
                song_name_for_display = song_name_part[:27] + "..."
            else:
                song_name_for_display = song_name_part

            play_count = row['Số lần nghe']
            time_played_str = format_ms_to_detailed_play_time_string(row['Tổng thời gian nghe (ms)'])
            songs_table.add_row(str(i+1), song_name_for_display, artist_name_part, str(play_count), time_played_str)
        console.print(songs_table)
    else:
        console.print("  🤷 Không có dữ liệu top bài hát cho giai đoạn này.", style="yellow")

    top_artists = calculate_top_artists_data(df_period_songs, top_n=top_n)
    if not top_artists.empty:
        artists_table = Table(show_lines=False, header_style="blue")
        artists_table.add_column("STT", style="dim", width=3, justify="right")
        artists_table.add_column("Nghệ sĩ", width=53)
        artists_table.add_column("Số lần nghe",width=11, justify="right")
        artists_table.add_column("Thời gian nghe", width=25, justify="right")

        for i, (artist_name, row) in enumerate(top_artists.iterrows()):
            play_count = row['Số lần nghe']
            time_played_str = format_ms_to_detailed_play_time_string(row['Tổng thời gian nghe (ms)'])
            artists_table.add_row(str(i+1), str(artist_name), str(play_count), time_played_str)
        console.print(artists_table)
    else:
        console.print("| 🤷 Không có dữ liệu top nghệ sĩ cho giai đoạn này.", style="yellow")

def prepare_songs_df_for_excel(top_songs_df_calculated):
    """Chuẩn bị DataFrame top bài hát cho việc xuất ra tệp Excel.

    Hàm này nhận DataFrame kết quả từ `calculate_top_songs_data`,
    tách cột 'fullSongName' (index) thành hai cột mới 'Tên bài hát' và 'Nghệ sĩ'.
    Đồng thời, định dạng lại cột 'Tổng thời gian nghe (ms)' thành chuỗi thời gian dễ đọc.

    Args:
        top_songs_df_calculated (pd.DataFrame): DataFrame chứa top bài hát
                                                (thường là kết quả từ `calculate_top_songs_data`).

    Returns:
        pd.DataFrame: DataFrame đã được định dạng với các cột:
                      'Tên bài hát', 'Nghệ sĩ', 'Số lần nghe', 'Thời gian nghe'.
                      Trả về DataFrame rỗng với các cột này nếu đầu vào rỗng.
    """
    if top_songs_df_calculated.empty:
        return pd.DataFrame(columns=['Tên bài hát', 'Nghệ sĩ', 'Số lần nghe', 'Thời gian nghe'])

    df_excel = top_songs_df_calculated.reset_index().rename(columns={'index': 'fullSongName'})

    split_names = df_excel['fullSongName'].astype(str).str.rsplit(' - ', n=1, expand=True)
    df_excel['Tên bài hát'] = split_names[0]
    df_excel['Nghệ sĩ'] = split_names[1].fillna("Không xác định") if 1 in split_names else "Không xác định"

    df_excel['Thời gian nghe'] = df_excel['Tổng thời gian nghe (ms)'].apply(format_ms_to_detailed_play_time_string)

    return df_excel[['Tên bài hát', 'Nghệ sĩ', 'Số lần nghe', 'Thời gian nghe']]

def prepare_artists_df_for_excel(top_artists_df_calculated):
    """Chuẩn bị DataFrame top nghệ sĩ cho việc xuất ra tệp Excel.

    Hàm này nhận DataFrame kết quả từ `calculate_top_artists_data`,
    đổi tên cột `ARTIST_NAME_COL` (index) thành 'Nghệ sĩ',
    đổi tên cột 'Số lần nghe' thành 'Số lần nghe bài hát',
    và định dạng lại cột 'Tổng thời gian nghe (ms)' thành chuỗi thời gian dễ đọc.

    Args:
        top_artists_df_calculated (pd.DataFrame): DataFrame chứa top nghệ sĩ
                                                  (thường là kết quả từ `calculate_top_artists_data`).

    Returns:
        pd.DataFrame: DataFrame đã được định dạng với các cột:
                      'Nghệ sĩ', 'Số lần nghe bài hát', 'Tổng thời gian nghe'.
                      Trả về DataFrame rỗng với các cột này nếu đầu vào rỗng.
    """
    if top_artists_df_calculated.empty:
        return pd.DataFrame(columns=['Nghệ sĩ', 'Số lần nghe bài hát', 'Tổng thời gian nghe'])

    df_excel = top_artists_df_calculated.reset_index().rename(columns={'index': ARTIST_NAME_COL})
    df_excel['Tổng thời gian nghe'] = df_excel['Tổng thời gian nghe (ms)'].apply(format_ms_to_detailed_play_time_string)

    return df_excel.rename(columns={
        ARTIST_NAME_COL: 'Nghệ sĩ',
        'Số lần nghe': 'Số lần nghe bài hát'
    })[['Nghệ sĩ', 'Số lần nghe bài hát', 'Tổng thời gian nghe']]

all_songs_df_base = pd.DataFrame()

def export_to_excel(yearly_data_dict, filename="spotify_analysis_report.xlsx", top_n_excel=20):
    """Xuất dữ liệu phân tích Spotify ra tệp Excel.

    Tạo một tệp Excel với nhiều sheet:
    - Một sheet "Tất cả": Chứa top `top_n_excel` bài hát và nghệ sĩ từ toàn bộ dữ liệu.
      Dữ liệu này được lấy từ biến global `all_songs_df_base`.
    - Mỗi năm có dữ liệu trong `yearly_data_dict`: Một sheet riêng (ví dụ: "2023")
      chứa top `top_n_excel` bài hát và nghệ sĩ của năm đó.

    Hàm sẽ tự động điều chỉnh độ rộng các cột trong Excel để dễ đọc hơn.
    Yêu cầu thư viện `openpyxl` phải được cài đặt.

    Args:
        yearly_data_dict (dict): Dictionary trong đó key là năm (int hoặc str) và value là
                                 DataFrame chứa dữ liệu bài hát của năm đó.
        filename (str, optional): Tên của tệp Excel đầu ra.
                                  Mặc định là "spotify_analysis_report.xlsx".
        top_n_excel (int, optional): Số lượng mục (bài hát/nghệ sĩ) hàng đầu
                                     để xuất ra mỗi sheet Excel. Mặc định là 20.
    """
    global all_songs_df_base
    console.print(f"\n📚 [bold green]Xuất ra EXCEL[/bold green]")
    try:
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            start_row_artists = 1
            if not all_songs_df_base.empty:
                all_songs_excel_data = calculate_top_songs_data(all_songs_df_base, top_n=top_n_excel)
                if not all_songs_excel_data.empty:
                    excel_all_songs = prepare_songs_df_for_excel(all_songs_excel_data)
                    excel_all_songs.to_excel(writer, sheet_name="Tất cả", index=False, startrow=1)
                    worksheet = writer.sheets["Tất cả"]
                    worksheet.cell(row=1, column=1, value=f"Top {top_n_excel} Bài Hát (Tất cả)")
                    start_row_artists = len(excel_all_songs) + 4
                    worksheet.cell(row=start_row_artists -1 , column=1, value=f"Top {top_n_excel} Nghệ Sĩ (Tất cả)")

                all_artists_excel_data = calculate_top_artists_data(all_songs_df_base, top_n=top_n_excel)
                if not all_artists_excel_data.empty:
                    excel_all_artists = prepare_artists_df_for_excel(all_artists_excel_data)
                    excel_all_artists.to_excel(writer, sheet_name="Tất cả", index=False, startrow=start_row_artists)
                    if start_row_artists == 1:
                        worksheet = writer.sheets["Tất cả"]
                        worksheet.cell(row=1, column=1, value=f"Top {top_n_excel} Nghệ Sĩ (Tất cả)")

            for year, df_year_songs in yearly_data_dict.items():
                if df_year_songs.empty:
                    continue
                sheet_name = str(year)
                start_row_artists_year = 1

                year_top_songs_data = calculate_top_songs_data(df_year_songs, top_n=top_n_excel)
                if not year_top_songs_data.empty:
                    excel_year_songs = prepare_songs_df_for_excel(year_top_songs_data)
                    excel_year_songs.to_excel(writer, sheet_name=sheet_name, index=False, startrow=1)
                    worksheet = writer.sheets[sheet_name]
                    worksheet.cell(row=1, column=1, value=f"Top {top_n_excel} Bài Hát (Năm {year})")
                    start_row_artists_year = len(excel_year_songs) + 4
                    worksheet.cell(row=start_row_artists_year -1 , column=1, value=f"Top {top_n_excel} Nghệ Sĩ (Năm {year})")

                year_top_artists_data = calculate_top_artists_data(df_year_songs, top_n=top_n_excel)
                if not year_top_artists_data.empty:
                    excel_year_artists = prepare_artists_df_for_excel(year_top_artists_data)
                    excel_year_artists.to_excel(writer, sheet_name=sheet_name, index=False, startrow=start_row_artists_year)
                    if start_row_artists_year == 1:
                        worksheet = writer.sheets[sheet_name]
                        worksheet.cell(row=1, column=1, value=f"Top {top_n_excel} Nghệ Sĩ (Năm {year})")

            for sheet_name_iter in writer.sheets:
                worksheet = writer.sheets[sheet_name_iter]
                for column_cells in worksheet.columns:
                    max_length = 0
                    column_letter = column_cells[0].column_letter
                    for cell in column_cells:
                        try:
                            if cell.value is not None:
                                max_length = max(max_length, len(str(cell.value)))
                        except:
                            pass
                    adjusted_width = min(max(max_length + 2, 10), 50)
                    worksheet.column_dimensions[column_letter].width = adjusted_width

        console.print(f"| ✅ Dữ liệu đã được xuất thành công vào '[cyan]{filename}[/cyan]'")
    except ImportError:
        console.print("| ❌ Lỗi: Thư viện 'openpyxl' chưa được cài đặt. Hãy chạy: pip install openpyxl", style="red")
    except Exception as e:
        console.print(f"| ❌ Lỗi khi xuất file Excel: {e}", style="red")


def main():
    """Hàm chính điều khiển luồng thực thi của chương trình phân tích lịch sử nghe nhạc Spotify.

    Các bước chính bao gồm:
    1. Tải dữ liệu từ tệp ZIP chứa lịch sử nghe nhạc Spotify.
    2. Tiền xử lý dữ liệu để chuẩn hóa và lọc ra các bản nhạc hợp lệ.
    3. Lưu trữ bản sao của DataFrame bài hát đã xử lý vào biến global `all_songs_df_base`.
    4. Tạo menu cho phép người dùng chọn xem phân tích cho "Tất cả thời gian" hoặc một năm cụ thể.
    5. Dựa trên lựa chọn của người dùng, thực hiện phân tích và hiển thị kết quả
       (thống kê chung, top bài hát, top nghệ sĩ) lên console.
    6. Chuẩn bị dữ liệu theo năm và xuất toàn bộ báo cáo phân tích ra tệp Excel.
    """
    global all_songs_df_base

    console.print("🚀 [bold green]Bắt đầu phân tích lịch sử SPOTIFY[/bold green]")

    df_raw = load_streaming_data_from_zip(ZIP_FILE_PATH, internal_file_pattern=INTERNAL_JSON_PATTERN)

    if df_raw.empty:
        console.print("| 🏁 Kết thúc: không có dữ liệu đầu vào từ file ZIP.", style="yellow")
        return

    df_songs = preprocess_data(df_raw)
    if df_songs.empty:
        console.print(f"\n| 🏁 Kết thúc: không có bài hát hợp lệ nào sau tiền xử lý.", style="yellow")
        return

    all_songs_df_base = df_songs.copy()

    available_years = sorted(df_songs['year'].unique(), reverse=True)

    menu_display_items = ["🌍 0. Tất cả"]
    numeric_choice_to_data_key = {0: "Tất cả"}

    for i, year in enumerate(available_years):
        menu_display_items.append(f"📅 {i+1}. Năm {year}")
        numeric_choice_to_data_key[i+1] = year

    console.print("\n💐 [bold green]MENU lựa chon xem[/bold green]")
    console.print("| " + " | ".join(menu_display_items))

    final_prompt_text = "| Lựa chọn của bạn: "
    choice_str = console.input(f"{final_prompt_text}").strip()

    df_to_analyze = pd.DataFrame()
    period_name_display = ""

    if choice_str.isdigit():
        choice_num = int(choice_str)
        if choice_num in numeric_choice_to_data_key:
            selected_option = numeric_choice_to_data_key[choice_num]
            if selected_option == "Tất cả":
                df_to_analyze = df_songs
                period_name_display = "Tất Cả Thời Gian"
            else:
                selected_year = selected_option
                df_to_analyze = df_songs[df_songs['year'] == selected_year]
                period_name_display = f"NĂM {selected_year}"

            display_analysis_results(df_to_analyze, period_name_display, top_n=10)
        else:
            console.print("| ❌ Lựa chọn không hợp lệ (số không có trong menu). Exit", style="red")
            return
    else:
        console.print("| ❌ Lựa chọn không hợp lệ (không phải là số).Exit", style="red")
        return

    yearly_data_dict_for_export = {}
    if not df_songs.empty:
        for year_val in available_years:
            yearly_data_dict_for_export[year_val] = df_songs[df_songs['year'] == year_val]

    if not df_songs.empty:
        export_to_excel(yearly_data_dict_for_export, top_n_excel=20)
    else:
        console.print("| 🤷 Không có dữ liệu bài hát để xuất ra Excel.", style="yellow")

if __name__ == "__main__":
    main()