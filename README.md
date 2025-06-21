# 📊 Personal Data Analysis: Spotify, TikTok & more

A quick guide to analyze your Spotify, TikTok & more data.

## 🛠️ Prerequisites

1.  Python 3.6+ installed.
2.  ⌨️ **Install Libraries** (in your terminal):
    ```bash
    pip install pandas rich openpyxl
    ```

---

## 🎧 Spotify

###  📥 Quick Start - Spotify

1.  🗂️ **Prepare Data**:
    *   ☁️ **Download**: Dowload [here](https://www.spotify.com/us/account/privacy/). This may take a few days. You'll get a ZIP.
    *   📁 **Organize**:
        *   ➡️ **Inside `view-wrapped/spotify/`, create a subfolder named `data`.**
        *   ➡️ **Place your downloaded Spotify data ZIP (e.g., `my_spotify_data.zip` containing `Streaming_History_*.json` files) or individual `Streaming_History_*.json` files *into* this `view-wrapped/spotify/data/` folder.**
    *   🌳 **Directory Structure**:
        ```
        view-wrapped/
        └── spotify/
            ├── main.py
            └── data/  <-- CREATE THIS FOLDER
                └── my_spotify_data.zip  <-- PUT DATA HERE
        ```

2.  ▶️ **Run Analysis**:
    ```bash
    cd path/to/view-wrapped/spotify
    python main.py
    ```

3.  👀 **View Results**:
    *   Check console output.
    *   Select year to view
    *   📄 Open the generated Excel file (usually in the `spotify/` directory).

---

## 📱 TikTok

### 📥 Quick Start - TikTok

1.  🗂️ **Prepare Data**:
    *   ☁️ **Download**: Request your data from the TikTok app (Profile > ☰ > Settings and privacy > Account > Download your data > JSON format). This may take a few days.
    *   📁 **Organize**:
        *   ➡️ **Inside `view-wrapped/tiktok/`, create a subfolder named `data`.**
        *   ➡️ **Place your downloaded TikTok data ZIP (e.g., `TikTok_Data_xxxxxxx.zip`) *into* this `view-wrapped/tiktok/data/` folder.** The script typically looks for files starting with `TikTok_Data_*.zip` in this `data` folder.
    *   🌳 **Directory Structure**:
        ```
        view-wrapped/
        └── tiktok/
            ├── main.py
            └── data/  <-- CREATE THIS FOLDER
                └── TikTok_Data_xxxx.zip  <-- PUT DATA HERE
        ```

2.  ▶️ **Run Analysis**:
    ```bash
    cd path/to/view-wrapped/tiktok
    python main.py
    ```

3.  👀 **View Results**:
    *   Check console output.
    *   📄 Open the generated Excel file (e.g., `tiktok_analysis_report.xlsx`) in the `tiktok/` directory.

## 📝 General Notes

*   🗺️ **Paths**: Ensure you're in the correct `spotify/` or `tiktok/` directory in your terminal before running `python main.py`. (e.g., `cd path/to/view-wrapped/spotify`)
*   🏷️ **Data Filenames**: Double-check that your scripts are configured to read the correct data filenames/patterns from within the `data` subfolder.
*   🔒 **Privacy**: Handle your personal data responsibly.
