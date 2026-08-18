# Reflection: Lakehouse Anti-Patterns

Trong 5 Anti-Patterns phổ biến của Data Lakehouse, hệ thống dữ liệu của team dễ mắc phải nhất là **"Small-Files Problem" (Vấn đề tệp nhỏ)**. 

**Phân tích nguyên nhân:**
Với việc ingest dữ liệu liên tục (đặc biệt là log từ hệ thống LLM như bảng `bronze/llm_calls_raw`), quá trình xử lý dữ liệu stream hoặc batch nhỏ theo thời gian thực sẽ sinh ra hàng nghìn tệp Parquet có dung lượng rất nhỏ (chỉ vài KB). Nếu bỏ qua việc bảo trì và gộp file định kỳ, điều này dẫn đến:
- Tốc độ truy vấn (I/O) bị thắt cổ chai do engine phải thực hiện thao tác mở và đóng tệp quá nhiều lần.
- Quá tải metadata cho thao tác liệt kê thư mục, tăng chi phí API khi đọc dữ liệu từ object storage.
- Làm chậm quá trình join và aggregate của các notebook ở tầng Silver/Gold.

**Giải pháp khắc phục:**
Cần thiết lập quy trình bảo trì (Maintenance) tự động để tối ưu hoá Lakehouse:
- Thực thi lệnh `OPTIMIZE` định kỳ để gộp các tệp nhỏ thành các tệp lớn tối ưu hơn (ví dụ: ~1GB).
- Kết hợp `ZORDER BY (ts)` để gom cụm dữ liệu gần nhau theo dòng thời gian, giúp Data Skipping hoạt động hiệu quả khi truy vấn time-series.
- Chạy `VACUUM` thường xuyên để dọn dẹp các tệp mồ côi (orphaned files) sau khi thực hiện OPTIMIZE, giúp tối ưu dung lượng lưu trữ.
