FROM python:3.9-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y fonts-nanum* apt-utils fontconfig chromium-driver && \
    fc-cache -fv

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN python -c "import matplotlib; print(matplotlib.__file__)" && \
    cp /usr/share/fonts/truetype/nanum/Nanum* /usr/local/lib/python3.9/site-packages/matplotlib/mpl-data/fonts/ttf/ && \
    rm -rf ~/.cache/matplotlib/*

EXPOSE 11100

# Start the FastAPI app using uvicorn
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "11100"]
