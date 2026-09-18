# AdPulse AI Monitor V2.1

Dashboard theo dõi quảng cáo TikTok Ads / Meta Ads bằng dữ liệu API thật. Không cần upload CSV trong giao diện. Sau khi cấu hình credentials, hệ thống tự đồng bộ nền theo `POLL_SECONDS` (mặc định 300 giây) và cho phép xem cửa sổ phân tích 1h / 3h / 5h.

## Nguồn dữ liệu
- TikTok Ads API for Business v1.3: delivery metrics theo `stat_time_hour`; TikTok Shop purchase/value/ROAS giữ đúng độ phân giải mà API trả về.
- Meta Marketing API Insights: campaign-level + hourly advertiser time-zone breakdown; purchase counts/value/ROAS chỉ hiển thị từ fields/actions/action_values/purchase_roas mà API trả về.
- Không suy ra revenue từ ROAS. Thiếu dữ liệu nguồn thì hiển thị `—`/DATA_LIMITED.

## Render
Build: `pip install -r requirements.txt`
Start: `python app.py`

Environment variables:
- `TIKTOK_ACCESS_TOKEN`
- `TIKTOK_ADVERTISER_ID`
- `META_ACCESS_TOKEN`
- `META_AD_ACCOUNT_ID`
- `META_GRAPH_VERSION` (mặc định trong render.yaml)
- `OPENAI_API_KEY` (AI explanation + Sora 2/Sora 2 Pro)
- `OPENAI_MODEL` (mặc định `gpt-5`)

## Video AI
Module tạo video dùng OpenAI Videos API, nhận ảnh tham chiếu + prompt và chạy bất đồng bộ: submit job → theo dõi tiến độ → tải MP4 khi hoàn tất. Không giữ HTTP request mở trong lúc chờ render.

## CSV
CSV import vẫn tồn tại như endpoint kỹ thuật dự phòng, nhưng không còn xuất hiện trên giao diện và không cần dùng cho vận hành bình thường.
