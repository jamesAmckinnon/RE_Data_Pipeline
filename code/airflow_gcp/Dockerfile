FROM apache/airflow:2.10.5
WORKDIR /opt/airflow

USER root

# Install Chrome
# RUN apt-get update && apt-get install -y wget unzip && \
#     # Install Chrome
#     wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb && \
#     apt install -y ./google-chrome-stable_current_amd64.deb && \
#     rm google-chrome-stable_current_amd64.deb && \
#     # Install ChromeDriver using Chrome for Testing
#     CHROME_VERSION=$(google-chrome --version | awk '{print $3}') && \
#     wget -O /tmp/chromedriver-linux64.zip "https://edgedl.me.goog/edgedl/chrome/chrome-for-testing/${CHROME_VERSION}/linux64/chromedriver-linux64.zip" && \
#     unzip /tmp/chromedriver-linux64.zip -d /tmp/ && \
#     mv /tmp/chromedriver-linux64/chromedriver /usr/bin/chromedriver && \
#     chmod +x /usr/bin/chromedriver && \
#     rm -rf /tmp/chromedriver-linux64.zip /tmp/chromedriver-linux64 && \
#     apt-get clean

RUN apt-get update && apt-get install -y \
    chromium \
    chromium-driver \
    && apt-get clean

USER airflow

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt