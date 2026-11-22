FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# 内置只读备份镜像:构建期确定性生成,并置为只读
RUN python backup_util.py /app/backup.img 256 4096 && chmod 0444 /app/backup.img

ENV BLOCK_SIZE=4096 \
    BLOCK_COUNT=256 \
    BACKFILL_DELAY=0.05 \
    BACKUP_IMAGE=/app/backup.img \
    DATA_DIR=/data \
    PORT=5000

VOLUME /data
EXPOSE 5000

CMD ["python", "app.py"]
