# Twitch Recorder

English users: see [README_EN.md](./README_EN.md).

![Twitch Recorder 展示畫面](./show_demo.png)

這個專案可以幫你自動監看 Twitch 主播，只要對方一開播，就自動開始錄影，讓你不用一直自己盯著直播時間。

適合這些使用情境：

- 自動保存特定 Twitch 主播直播內容
- 同時追蹤多位主播
- 用瀏覽器就能管理監看名單
- 不想手動查開播、手動按錄影的人
- vod會過期，或是開訂閱會員才能看vod

## 可以做到什麼

- 新增你想監看的 Twitch 主播
- 自動檢查對方有沒有開播
- 開播時自動開始錄影
- 直播結束後自動停止錄影
- 查看目前誰正在直播、誰正在錄影
- 查看已經錄好的影片檔案
- 廣告緩解流程：可選「登入態錄影」最佳努力抓流 + 廣告段落偵測 + 可觀看檔案後處理

## 使用前要準備什麼

- 一台有安裝 Docker 的電腦
- 一組 Twitch 提供的應用程式金鑰
  - `TWITCH_CLIENT_ID`
  - `TWITCH_CLIENT_SECRET`

你可以把它理解成「讓這個工具有權限去查 Twitch 公開直播資訊」的通行證。沒有這兩個值，系統就不知道要用哪個 Twitch 應用程式去查資料。

如果你還沒有這組資料，可以照下面方式申請：

1. 登入你的 Twitch 帳號
2. 前往 Twitch Developer Console
3. 建立一個新的應用程式
4. 建立完成後，你會拿到 `Client ID`(用戶名端ID)
5. 接著再按新密碼按鈕，產生 `Client Secret`(用戶名端密碼)
6. 把這兩個值填進 `.env` 裡對應的位置

![Twitch API 設定畫面](./twitch_api.png)

如果申請頁面要求填 `OAuth Redirect URL`，你可以先填一個本機網址，例如 `http://localhost`。這個專案主要是拿來查直播資訊，不需要做複雜登入流程。

## 快速開始

1. 在專案根目錄建立 `.env` 檔案

把下面內容填進去：

```env
TWITCH_CLIENT_ID=你的_client_id
TWITCH_CLIENT_SECRET=你的_client_secret
TWITCH_USER_OAUTH_TOKEN=
TWITCH_USER_LOGIN=
MAX_CONCURRENT_STREAMERS=3
POLL_INTERVAL_SECONDS=30
OFFLINE_GRACE_PERIOD_SECONDS=20
RECORDING_START_DELAY_SECONDS=15
RECORDINGS_PATH=/recordings
CONFIG_PATH=/config
ALLOWED_ORIGINS=http://localhost:3000,http://127.0.0.1:3000
```

可選參數（不填也可運作）：

- `TWITCH_USER_OAUTH_TOKEN`：使用者 OAuth Token，用於「登入態錄影」以最佳努力降低廣告與開場等待畫面
- `TWITCH_USER_LOGIN`：可選，通常填 Twitch 帳號 login；未填時系統會以 token 情境盡力處理
- `RECORDING_START_DELAY_SECONDS`：主播開播後延遲幾秒才啟動錄影（預設 15 秒），用來避開開場 `Preparing your stream` 畫面

2. 啟動專案

```bash
docker compose up -d --build
```

3. 打開瀏覽器

- 管理頁面：`http://localhost:3000`

## 平常怎麼使用

1. 打開管理頁面
2. 輸入你想監看的 Twitch 主播名稱
3. 按下新增
4. 系統會自動定期檢查對方是否開播
5. 如果主播開播，就會自動開始錄影
6. 錄好的檔案會存到 `recordings/` 資料夾

## 管理畫面可以看到什麼

- 主播目前是否開播
- 直播標題
- 遊戲或分類
- 觀看人數
- 是否正在錄影
- 錄影開始時間
- 錄影輸出檔案位置

## 錄好的影片會放在哪裡

所有錄影檔都會存在專案裡的 `recordings/` 資料夾。

## 廣告緩解（Hybrid 模式）

- 未設定使用者 token：走一般模式錄影
- 有設定 `TWITCH_USER_OAUTH_TOKEN`：系統會先嘗試登入態抓流（best-effort），失敗時自動回退
- 錄影後會做廣告段落偵測，並產生可正常拖曳/播放的 watchable 後處理檔案
- 錄影列表（API 與前端清單）會顯示 watchable 狀態與廣告段落數（ad break count）
  - ad break count 優先使用錄製期間事件；若事件為 0，會回退使用檔案內 `timed_id3` 的 `content.ad` 標記估算

## 常用指令

啟動：

```bash
docker compose up -d --build
```

查看執行狀態：

```bash
docker compose logs -f
```

停止：

```bash
docker compose down
```

## 目前問題
- 輸出影片有時會因為長時間關係，用影片播放器會顯示錯誤時間和無法正常滑動時間軸，要用特殊指令轉出復製影片才正常，因為錄製影片格式是MPEG-TS，只是副檔名是.mp4，內部資料才會顯示錯誤
- 開始錄影時會有prepare your stream，會影響錄影時間，考慮優化
