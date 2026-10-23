# Nguồn dữ liệu & Provenance

| Dataset | Nguồn | Truy cập | Version / snapshot | Terms |
|---|---|---|---|---|
| `google_trends_rss` | Google Trends – Trending Now RSS, geo=VN: `https://trends.google.com/trending/rss?geo=VN` | HTTP GET, public, không key | Snapshot theo thời điểm ingest (`ingest_started_utc` + sha256 trong manifest) | Google ToS, dùng học tập |
| `google_trends` | Google Trends Explore (interest over time + related queries) qua `pytrends==4.9.2` | Unofficial API, rate-limit (HTTP 429) | Tham số `timeframe` (mặc định `today 3-m`), `geo=VN`; chỉ số tương đối 0–100, chuẩn hoá theo từng nhóm ≤5 từ khoá | Google ToS, dùng học tập |
| `tiki_products` | Tiki.vn listing API: `https://tiki.vn/api/personalish/v1/blocks/listings` | HTTP GET, public JSON | Snapshot theo thời điểm ingest; tham số `category`, `page`, `limit` lưu trong manifest | Dữ liệu công khai; crawl tần suất thấp (sleep 1.5s), không dùng thương mại |

## Cách xác định version/snapshot
Nguồn là dữ liệu "live" (không có version chính thức) ⇒ **mỗi run = một snapshot** được định danh bởi
`run_id` (UTC timestamp), `params` và `sha256` của từng file trong `_manifest.json`.

## Vai trò trong bài toán gắn nhãn affiliate
- **Trends RSS**: phát hiện từ khoá nổi lên trong ngày (tín hiệu ngắn hạn).
- **Trends interest-over-time**: độ tăng trưởng nhu cầu theo ngành hàng (tín hiệu trung hạn).
- **Tiki products**: giá, rating, `quantity_sold`, review count → ghép với từ khoá để chọn sản phẩm
  gắn nhãn `HOT / RISING / STABLE / COLD` cho affiliate.
