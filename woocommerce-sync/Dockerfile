FROM python:3.12-slim
WORKDIR /app
COPY woocommerce-sync/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY shipping /app/shipping
COPY woocommerce-sync/woo_sync /app/woocommerce-sync/woo_sync
COPY woocommerce-sync/woocommerce_sync.py /app/woocommerce-sync/woocommerce_sync.py
CMD ["python", "/app/woocommerce-sync/woocommerce_sync.py", "run"]
