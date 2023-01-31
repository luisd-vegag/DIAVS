FROM python:3.8

# set the working directory
WORKDIR /app

# copy requirements file
COPY requirements.txt .

# install dependencies
RUN pip install -r requirements.txt

# copy the entire DIAVS directory to /app
COPY . /app

