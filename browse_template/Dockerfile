FROM python:3.11-slim
ENV PLAYWRIGHT_BROWSERS_PATH=/opt/ms-playwright
RUN pip install --no-cache-dir playwright && \
    playwright install --with-deps chromium && \
    chmod -R a+rX /opt/ms-playwright
